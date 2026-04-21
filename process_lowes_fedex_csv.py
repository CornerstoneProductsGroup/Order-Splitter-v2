"""
Build FedEx upload spreadsheets from Lowe's raw CSV exports.

Configuration lives in ``Lowe's FedEx mapping.xlsx``:
  - **Settings**: row 1 = sheet name in the weights workbook; rows 3–4 = raw CSV
    column names for SKU and units; rows 5–6 = FedEx output column names for
    shipment total weight and number of packages.
  - **ColumnMap**: each row maps a FedEx column to a raw CSV column.
  - **Constants**: each row sets a fixed value for a FedEx column on every output row.

SKU weights and label splits use the same rules model as Depot CSV
(``load_sku_rules`` / ``split_row_for_labels``) against ``Lowe's Weights for Labels.xlsx``.
"""

from __future__ import annotations

import argparse
import contextlib
import datetime as dt
import hashlib
import json
import logging
import os
import re
import shutil
import time
import uuid
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

import process_depot_csv_orders as depot

_LOG = logging.getLogger(__name__)

# Same folder as watcher.py — empty file pauses all automation (see watcher module docstring).
_AUTOMATION_STOP_FILE = Path(__file__).resolve().parent / "AUTOMATION_STOP.txt"


class LowesFedexHalt(Exception):
    """Skip Lowe's processing without treating it as a bug (watcher catches by ``code``)."""

    __slots__ = ("code", "detail")

    def __init__(self, code: str, detail: str = "") -> None:
        self.code = code
        self.detail = detail
        super().__init__(f"{code}: {detail}" if detail else code)


def _lowes_automation_blocked_reason() -> str | None:
    """Return a reason string if Lowe's FedEx must not run (mirrors watcher.py master + CSV flags)."""
    if os.environ.get("ORDER_SPLITTER_DISABLE_ALL_AUTOMATION", "0").strip().lower() in {"1", "true", "yes", "y"}:
        return "ORDER_SPLITTER_DISABLE_ALL_AUTOMATION"
    if os.environ.get("ORDER_SPLITTER_DISABLE_CSV_WATCH", "0").strip().lower() in {"1", "true", "yes", "y"}:
        return "ORDER_SPLITTER_DISABLE_CSV_WATCH"
    try:
        if _AUTOMATION_STOP_FILE.is_file():
            return f"file {_AUTOMATION_STOP_FILE}"
    except OSError:
        pass
    return None


def _sha256_file(path: Path) -> str | None:
    h = hashlib.sha256()
    try:
        with path.open("rb") as f:
            for chunk in iter(lambda: f.read(1048576), b""):
                if not chunk:
                    break
                h.update(chunk)
        return h.hexdigest()
    except OSError:
        return None


def _persist_state_path(mapping_xlsx: Path) -> Path:
    return mapping_xlsx.resolve().parent / ".lowes_fedex_last_run.json"


def _load_persist_state(path: Path) -> dict:
    if not path.is_file():
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        return raw if isinstance(raw, dict) else {}
    except Exception:
        return {}


def _persist_is_recent_duplicate(mapping_xlsx: Path, stem: str, content_hash: str, window_sec: float = 86400.0) -> bool:
    by = _load_persist_state(_persist_state_path(mapping_xlsx)).get("by_stem")
    if not isinstance(by, dict):
        return False
    entry = by.get(stem)
    if not isinstance(entry, dict):
        return False
    if entry.get("sha256") != content_hash:
        return False
    try:
        return time.time() - float(entry.get("ts", 0)) < window_sec
    except (TypeError, ValueError):
        return False


def _persist_record_success(mapping_xlsx: Path, stem: str, content_hash: str) -> None:
    path = _persist_state_path(mapping_xlsx)
    state = _load_persist_state(path)
    by = state.get("by_stem")
    if not isinstance(by, dict):
        by = {}
    by[stem] = {"sha256": content_hash, "ts": time.time()}
    state["by_stem"] = by
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")
    os.replace(str(tmp), str(path))


@contextlib.contextmanager
def _stem_process_lock(mapping_xlsx: Path, stem: str):
    """Exclusive lock for one Lowe's stem (any Python process using this mapping folder)."""
    lock_dir = mapping_xlsx.resolve().parent
    lock_dir.mkdir(parents=True, exist_ok=True)
    tag = hashlib.sha256(stem.encode("utf-8", errors="surrogateescape")).hexdigest()[:28]
    lock_path = lock_dir / f".__lowes_fedex_lock_{tag}"
    stale_sec = 900.0
    for attempt in range(2):
        try:
            if lock_path.exists():
                try:
                    if time.time() - lock_path.stat().st_mtime > stale_sec:
                        lock_path.unlink(missing_ok=True)
                except OSError:
                    pass
            fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            try:
                os.write(fd, f"{os.getpid()}\n".encode("ascii", errors="replace"))
            finally:
                os.close(fd)
            break
        except FileExistsError:
            if attempt == 0:
                time.sleep(0.1)
                continue
            raise LowesFedexHalt("busy", str(lock_path)) from None
    try:
        yield lock_path
    finally:
        try:
            lock_path.unlink(missing_ok=True)
        except OSError:
            pass


def _cleanup_lowes_timestamp_collision_artifacts(directories: tuple[Path, ...], raw_stem: str) -> int:
    """Delete legacy ``{stem} Output_YYYYMMDD_HHMMSS.xlsx`` / ``{stem} Input_YYYYMMDD_HHMMSS.csv`` files.

    Older builds (or overlapping runs) could leave these next to the canonical ``{stem} Output.xlsx``
    and ``{stem} Input.csv`` names; remove them from output and archive folders after a good run.
    """
    out_pat = re.compile(rf"^{re.escape(raw_stem)} Output_\d{{8}}_\d{{6}}\.xlsx$", re.IGNORECASE)
    in_pat = re.compile(rf"^{re.escape(raw_stem)} Input_\d{{8}}_\d{{6}}\.csv$", re.IGNORECASE)
    removed = 0
    for directory in directories:
        try:
            for p in directory.iterdir():
                if not p.is_file():
                    continue
                n = p.name
                if out_pat.fullmatch(n) or in_pat.fullmatch(n):
                    try:
                        p.unlink()
                        removed += 1
                    except OSError:
                        pass
        except OSError:
            pass
    return removed


def _norm_cell(value: object) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    s = str(value).strip()
    if s.lower() == "nan":
        return ""
    return s


def _pick_sheet(xl: pd.ExcelFile, wanted: set[str]) -> str:
    for name in xl.sheet_names:
        if name.strip().lower() in wanted:
            return name
    raise ValueError(f"No sheet matching {wanted!r} in {xl.sheet_names!r}")


def _settings_value(df: pd.DataFrame, excel_row: int) -> str:
    """Read one Settings row value from first populated value column (B..end), else A."""
    r = excel_row - 1
    if r < 0 or r >= len(df):
        return ""
    if df.shape[1] > 1:
        for c in range(1, df.shape[1]):
            v = _norm_cell(df.iat[r, c])
            if v:
                return v
    return _norm_cell(df.iat[r, 0]) if df.shape[1] > 0 else ""


def _settings_value_by_keywords(df: pd.DataFrame, keywords: tuple[str, ...]) -> str:
    """Fallback lookup: find a row whose first two cells mention all keywords."""
    key_parts = tuple(k.strip().lower() for k in keywords if k.strip())
    if not key_parts:
        return ""
    for i in range(len(df)):
        a = _norm_cell(df.iat[i, 0]) if df.shape[1] > 0 else ""
        b = _norm_cell(df.iat[i, 1]) if df.shape[1] > 1 else ""
        hay = f"{a} {b}".strip().lower()
        if hay and all(k in hay for k in key_parts):
            # Prefer explicit value in col B; if not there and there are >2 cols, use next non-empty cell.
            if b:
                return b
            if df.shape[1] > 2:
                for c in range(2, df.shape[1]):
                    v = _norm_cell(df.iat[i, c])
                    if v:
                        return v
            return a
    return ""


def _two_col_table(df: pd.DataFrame) -> list[tuple[str, str]]:
    if df.shape[1] < 2:
        raise ValueError("Expected at least two columns")
    start = 0
    h0 = _norm_cell(df.iat[0, 0]).lower()
    h1 = _norm_cell(df.iat[0, 1]).lower()
    if h0 and h1:
        headerish = {
            "fedex",
            "output",
            "destination",
            "field",
            "column",
            "raw",
            "source",
            "csv",
            "value",
            "constant",
            "templatecolumn",
            "rawcolumn",
        }
        if h0 in headerish and h1 in headerish:
            start = 1
    out: list[tuple[str, str]] = []
    for i in range(start, len(df)):
        left = _norm_cell(df.iat[i, 0])
        right = _norm_cell(df.iat[i, 1])
        if not left:
            continue
        out.append((left, right))
    return out


def _sheet_weights_name(settings_value: str) -> str | int | None:
    s = settings_value.strip()
    if not s:
        return None
    if re.fullmatch(r"[0-9]+", s):
        return int(s)
    return s


@dataclass(frozen=True)
class LoweFedexMapping:
    weights_selector: str
    raw_sku_col: str
    raw_units_col: str
    raw_sku_candidates: tuple[str, ...]
    raw_units_candidates: tuple[str, ...]
    fedex_weight_col: str
    fedex_packages_col: str
    column_map: tuple[tuple[str, str], ...]
    constants: tuple[tuple[str, str], ...]
    output_columns: tuple[str, ...]


def load_lowes_fedex_mapping(mapping_xlsx: Path) -> LoweFedexMapping:
    if not mapping_xlsx.exists():
        raise FileNotFoundError(f"Mapping workbook not found: {mapping_xlsx}")

    xl = pd.ExcelFile(mapping_xlsx)
    sheet_names = list(xl.sheet_names)
    settings_name = sheet_names[0]
    cmap_name = sheet_names[1] if len(sheet_names) > 1 else settings_name
    const_name = sheet_names[2] if len(sheet_names) > 2 else settings_name
    try:
        cmap_name = _pick_sheet(xl, {"columnmap", "column map"})
    except ValueError:
        pass
    try:
        const_name = _pick_sheet(xl, {"constants", "constant"})
    except ValueError:
        pass

    sdf = pd.read_excel(mapping_xlsx, sheet_name=settings_name, header=None)
    weights_selector = _settings_value(sdf, 1)
    ws_lower = weights_selector.strip().lower()
    if ws_lower in {"main", "raw csv file", "fedex template", "value"} or "raw csv" in ws_lower:
        weights_selector = ""
    if not weights_selector:
        weights_selector = _settings_value(sdf, 2)
    if not weights_selector:
        weights_selector = (
            _settings_value_by_keywords(sdf, ("rules_workbook",))
            or _settings_value_by_keywords(sdf, ("rules", "workbook"))
            or _settings_value_by_keywords(sdf, ("weights",))
        )
    raw_sku_candidates = _settings_row_candidates(sdf, 3)
    raw_units_candidates = _settings_row_candidates(sdf, 4)
    raw_sku_col = (raw_sku_candidates[0] if raw_sku_candidates else "") or _settings_value_by_keywords(sdf, ("sku",))
    raw_units_col = (raw_units_candidates[0] if raw_units_candidates else "") or _settings_value_by_keywords(sdf, ("unit",))
    fedex_weight_col = (
        _settings_value(sdf, 5)
        or _settings_value_by_keywords(sdf, ("fedex", "weight"))
        or _settings_value_by_keywords(sdf, ("weight",))
    )
    fedex_packages_col = (
        _settings_value(sdf, 6)
        or _settings_value_by_keywords(sdf, ("fedex", "package"))
        or _settings_value_by_keywords(sdf, ("package",))
    )
    for label, val in (
        ("Settings row 3 (raw SKU column)", raw_sku_col),
        ("Settings row 4 (raw units column)", raw_units_col),
        ("Settings row 5 (FedEx total weight column)", fedex_weight_col),
        ("Settings row 6 (FedEx packages column)", fedex_packages_col),
    ):
        if not val:
            raise ValueError(f"{label} is empty in {mapping_xlsx.name} / {settings_name!r}")

    cdf = pd.read_excel(mapping_xlsx, sheet_name=cmap_name, header=None)
    kdf = pd.read_excel(mapping_xlsx, sheet_name=const_name, header=None)
    column_map = tuple(_two_col_table(cdf))
    constants = tuple(_two_col_table(kdf))
    if not column_map and not constants:
        raise ValueError(f"No column map or constants rows in {mapping_xlsx}")

    seen: set[str] = set()
    order: list[str] = []
    for fed, _raw in column_map:
        if fed not in seen:
            seen.add(fed)
            order.append(fed)
    for fed, _val in constants:
        if fed not in seen:
            seen.add(fed)
            order.append(fed)
    for fed in (fedex_weight_col, fedex_packages_col):
        if fed not in seen:
            seen.add(fed)
            order.append(fed)

    return LoweFedexMapping(
        weights_selector=weights_selector,
        raw_sku_col=raw_sku_col,
        raw_units_col=raw_units_col,
        raw_sku_candidates=raw_sku_candidates or ((raw_sku_col,) if raw_sku_col else tuple()),
        raw_units_candidates=raw_units_candidates or ((raw_units_col,) if raw_units_col else tuple()),
        fedex_weight_col=fedex_weight_col,
        fedex_packages_col=fedex_packages_col,
        column_map=column_map,
        constants=constants,
        output_columns=tuple(order),
    )


def _raw_column_key_map(columns: list[object]) -> dict[str, str]:
    return {str(c).strip().lower(): str(c) for c in columns}


def _raw_series_for_name(df: pd.DataFrame, logical_name: str) -> str:
    """Return actual DataFrame column name for a raw CSV header from mapping."""
    want = str(logical_name).strip().lower()
    m = _raw_column_key_map(list(df.columns))
    hit = m.get(want)
    if hit is not None:
        return hit
    raise ValueError(f"Raw CSV missing column {logical_name!r} (have: {list(df.columns)})")


def _settings_row_candidates(df: pd.DataFrame, excel_row: int) -> tuple[str, ...]:
    r = excel_row - 1
    if r < 0 or r >= len(df):
        return tuple()
    vals: list[str] = []
    seen: set[str] = set()
    for c in range(df.shape[1]):
        v = _norm_cell(df.iat[r, c])
        if not v:
            continue
        k = v.strip().lower()
        if k in seen:
            continue
        seen.add(k)
        vals.append(v)
    return tuple(vals)


def _raw_series_for_candidates(df: pd.DataFrame, candidates: tuple[str, ...]) -> str:
    last_err: ValueError | None = None
    for name in candidates:
        try:
            return _raw_series_for_name(df, name)
        except ValueError as e:
            last_err = e
    if last_err is not None:
        raise last_err
    raise ValueError(f"No raw CSV candidates provided (have: {list(df.columns)})")


def _sku_rules_list(rules: dict[str, list[depot.SkuRule]], sku_raw: object) -> list[depot.SkuRule] | None:
    k = depot._norm_sku(sku_raw)
    if k in rules:
        return rules[k]
    kl = depot._norm_sku_loose(sku_raw)
    if kl in rules:
        return rules[kl]
    return None


def _build_output_row(
    mapping: LoweFedexMapping,
    raw_headers: dict[str, str],
    raw_values: dict[str, object],
    weight_str: str,
    packages_str: str,
) -> dict[str, str]:
    const_by_fed: dict[str, str] = {fed: val for fed, val in mapping.constants}
    map_by_fed: dict[str, str] = {fed: raw for fed, raw in mapping.column_map}
    row: dict[str, str] = {}
    for col in mapping.output_columns:
        if col == mapping.fedex_weight_col:
            row[col] = weight_str
        elif col == mapping.fedex_packages_col:
            row[col] = packages_str
        elif col in const_by_fed:
            row[col] = const_by_fed[col]
        elif col in map_by_fed:
            logical = map_by_fed[col]
            actual = raw_headers.get(str(logical).strip().lower())
            if actual is None:
                raise ValueError(f"ColumnMap references unknown raw column {logical!r} for FedEx {col!r}")
            v = raw_values.get(actual, "")
            if v is None or (isinstance(v, float) and pd.isna(v)):
                row[col] = ""
            else:
                row[col] = str(v).strip()
        else:
            row[col] = ""
    _normalize_us_postal_in_row(row)
    return row


def _find_key_case_insensitive(row: dict[str, str], candidates: tuple[str, ...]) -> str | None:
    lookup = {k.strip().lower(): k for k in row.keys()}
    for c in candidates:
        hit = lookup.get(c.strip().lower())
        if hit is not None:
            return hit
    return None


def _normalize_us_postal_in_row(row: dict[str, str]) -> None:
    postal_key = _find_key_case_insensitive(
        row,
        ("SHPTO_POSTAL_CODE", "Postal Code", "PostalCode", "Zip", "ZIP"),
    )
    country_key = _find_key_case_insensitive(
        row,
        ("SHPTO_COUNTRY_ID", "Country", "CountryCode", "Country Code"),
    )
    if postal_key is None:
        return

    postal = str(row.get(postal_key, "") or "").strip()
    country = str(row.get(country_key, "") or "").strip().upper() if country_key else ""
    normalized = depot._normalize_postal_code(postal, country)
    row[postal_key] = normalized


def build_preview(
    raw_csv: Path,
    rules: dict[str, list[depot.SkuRule]],
    mapping: LoweFedexMapping,
    uproot_skip: frozenset[str],
) -> tuple[int, int]:
    raw_df = pd.read_csv(raw_csv, encoding="utf-8-sig", dtype=str, keep_default_na=False)
    if raw_df.empty:
        raise ValueError(f"No data rows in {raw_csv}")
    sku_actual = _raw_series_for_candidates(raw_df, mapping.raw_sku_candidates)
    units_actual = _raw_series_for_candidates(raw_df, mapping.raw_units_candidates)
    raw_key_map = _raw_column_key_map(list(raw_df.columns))
    for _fed, logical in mapping.column_map:
        k = str(logical).strip().lower()
        if k not in raw_key_map:
            raise ValueError(f"ColumnMap references raw column {logical!r} not present in CSV")

    out_rows = 0
    unknown = 0
    for _, r in raw_df.iterrows():
        sku_raw = r.get(sku_actual, "")
        loose = depot._norm_sku_loose(sku_raw)
        if loose and loose in uproot_skip:
            continue
        sku_rules = _sku_rules_list(rules, sku_raw)
        if not sku_rules:
            unknown += 1
            continue
        units = depot._parse_int(r.get(units_actual, ""), 0)
        base = [""] * 31
        base[depot.IDX_AB] = depot._norm_sku(sku_raw)
        base[depot.IDX_AC] = str(units)
        out_rows += len(depot.split_row_for_labels(base, sku_rules))
    return out_rows, unknown


def process_file(
    raw_csv: Path,
    rules: dict[str, list[depot.SkuRule]],
    mapping: LoweFedexMapping,
    output_dir: Path,
    uproot_skip: frozenset[str],
    output_basename: str | None = None,
) -> tuple[Path, int, int]:
    raw_df = pd.read_csv(raw_csv, encoding="utf-8-sig", dtype=str, keep_default_na=False)
    if raw_df.empty:
        raise ValueError(f"No data rows in {raw_csv}")

    sku_actual = _raw_series_for_candidates(raw_df, mapping.raw_sku_candidates)
    units_actual = _raw_series_for_candidates(raw_df, mapping.raw_units_candidates)
    raw_key_map = _raw_column_key_map(list(raw_df.columns))
    raw_headers: dict[str, str] = {}
    for _fed, logical in mapping.column_map:
        k = str(logical).strip().lower()
        if k not in raw_key_map:
            raise ValueError(f"ColumnMap references raw column {logical!r} not present in CSV")
        raw_headers[k] = raw_key_map[k]

    out_records: list[dict[str, str]] = []
    unknown = 0

    for _, r in raw_df.iterrows():
        raw_values = {str(c): r.get(c, "") for c in raw_df.columns}
        sku_raw = r.get(sku_actual, "")
        loose = depot._norm_sku_loose(sku_raw)
        if loose and loose in uproot_skip:
            continue
        sku_rules = _sku_rules_list(rules, sku_raw)
        if not sku_rules:
            unknown += 1
            continue
        units = depot._parse_int(r.get(units_actual, ""), 0)
        base = [""] * 31
        base[depot.IDX_AB] = depot._norm_sku(sku_raw)
        base[depot.IDX_AC] = str(units)
        for split_row, _split_idx in depot.split_row_for_labels(base, sku_rules):
            w = split_row[depot.IDX_AD]
            n = split_row[depot.IDX_AE]
            out_records.append(
                _build_output_row(
                    mapping,
                    raw_headers,
                    raw_values,
                    depot._fmt_number(depot._parse_float(w, 0.0)) if w else "",
                    str(depot._parse_int(n, 0)) if n else "",
                )
            )

    output_dir.mkdir(parents=True, exist_ok=True)
    base = output_basename.strip() if output_basename else f"{raw_csv.stem} Output"
    out_path = output_dir / f"{base}.xlsx"
    out_df = pd.DataFrame(out_records, columns=list(mapping.output_columns))
    tmp_path = output_dir / f".__lowes_fedex_{os.getpid()}_{uuid.uuid4().hex}.tmp.xlsx"
    try:
        with pd.ExcelWriter(tmp_path, engine="openpyxl") as writer:
            out_df.to_excel(writer, sheet_name="FedEx", index=False)
        if out_path.exists():
            try:
                out_path.unlink()
            except OSError:
                pass
        os.replace(str(tmp_path), str(out_path))
    except Exception:
        try:
            if tmp_path.exists():
                tmp_path.unlink()
        except OSError:
            pass
        raise

    return out_path, len(out_records), unknown


def _archive_copy(source_path: Path, archive_dir: Path, archive_name: str) -> Path:
    archive_dir.mkdir(parents=True, exist_ok=True)
    target = archive_dir / archive_name
    shutil.copy2(str(source_path), str(target))
    return target


def _archive_move_replace(source_path: Path, archive_dir: Path, archive_name: str) -> Path:
    """Move ``source_path`` into the archive folder under ``archive_name`` (overwrites target)."""
    archive_dir.mkdir(parents=True, exist_ok=True)
    target = archive_dir / archive_name
    try:
        if target.exists():
            target.unlink()
    except OSError:
        pass
    try:
        shutil.move(str(source_path), str(target))
    except OSError:
        shutil.copy2(str(source_path), str(target))
        source_path.unlink()
    return target


def _resolve_weights_source(mapping_xlsx: Path, default_weights_xlsx: Path, selector: str) -> tuple[Path, str | int | None]:
    s = selector.strip()
    if not s:
        return default_weights_xlsx, None
    lower = s.lower()
    if lower.endswith(".xlsx") or lower.endswith(".xlsm") or lower.endswith(".xls"):
        p = Path(s)
        if not p.is_absolute():
            p = mapping_xlsx.parent / p
        return p, None
    # If selector matches an existing sheet in the default workbook, treat it as sheet name.
    try:
        if default_weights_xlsx.exists():
            xl = pd.ExcelFile(default_weights_xlsx)
            want = s.strip().lower()
            if any(str(name).strip().lower() == want for name in xl.sheet_names):
                return default_weights_xlsx, _sheet_weights_name(s)
    except Exception:
        pass

    # If selector appears to be a workbook base name, resolve it next to the mapping workbook.
    for ext in (".xlsx", ".xlsm", ".xls"):
        candidate = mapping_xlsx.parent / f"{s}{ext}"
        if candidate.exists():
            return candidate, None

    # Fallback: treat selector as informational label and use first sheet of default workbook.
    return default_weights_xlsx, None


def process_one_csv(
    raw_csv: Path,
    mapping_xlsx: Path,
    weights_xlsx: Path,
    output_dir: Path,
    archive_dir: Path,
    dry_run: bool = False,
    *,
    force: bool = False,
) -> tuple[Path | None, int, int, Path | None]:
    """Process one raw Lowe's CSV into a FedEx upload workbook.

    Returns ``(output_path_or_none, output_row_count, unknown_sku_count, archive_path_or_none)``.

    Unless ``force=True``, processing is refused when CSV automation is disabled or
    ``AUTOMATION_STOP.txt`` is present (same rules as ``watcher.py``), so stray schedulers
    cannot bypass the watcher.
    """
    if not force:
        blocked = _lowes_automation_blocked_reason()
        if blocked:
            raise LowesFedexHalt("paused", blocked)

    content_hash = _sha256_file(raw_csv)
    if content_hash is None:
        raise ValueError(f"Could not read file for hashing: {raw_csv}")

    if not dry_run and not force:
        if _persist_is_recent_duplicate(mapping_xlsx, raw_csv.stem, content_hash):
            raise LowesFedexHalt("duplicate", raw_csv.stem)

    mapping = load_lowes_fedex_mapping(mapping_xlsx)
    resolved_weights_xlsx, weights_sheet = _resolve_weights_source(mapping_xlsx, weights_xlsx, mapping.weights_selector)
    rules = depot.load_sku_rules(resolved_weights_xlsx, weights_sheet)
    uproot_skip = depot.uproot_placeholder_skus_from_rules_workbook(resolved_weights_xlsx, weights_sheet)

    if dry_run:
        out_rows, unknown = build_preview(raw_csv, rules, mapping, uproot_skip)
        return None, out_rows, unknown, None

    stem = raw_csv.stem
    out_path: Path | None = None
    with _stem_process_lock(mapping_xlsx, stem):
        try:
            out_path, out_rows, unknown = process_file(
                raw_csv,
                rules,
                mapping,
                output_dir,
                uproot_skip,
                output_basename=f"{stem} Output",
            )
            archived_input = _archive_move_replace(
                raw_csv, archive_dir, f"{stem} Input{raw_csv.suffix}",
            )
            _archive_copy(out_path, archive_dir, f"{stem} Output{out_path.suffix}")
        except Exception:
            if out_path is not None:
                try:
                    if out_path.exists():
                        out_path.unlink()
                except OSError:
                    pass
            raise
        n_legacy = _cleanup_lowes_timestamp_collision_artifacts((output_dir, archive_dir), stem)
        if n_legacy:
            _LOG.info(
                "Removed %d legacy timestamp collision file(s) for %r (Output_*.xlsx / Input_*.csv)",
                n_legacy,
                stem,
            )
        _persist_record_success(mapping_xlsx, stem, content_hash)
    return out_path, out_rows, unknown, archived_input


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Lowe's raw CSV → FedEx upload xlsx")
    p.add_argument("--input", type=Path, required=True, help="Raw Lowe's CSV file")
    p.add_argument("--mapping", type=Path, required=True, help="Lowe's FedEx mapping.xlsx")
    p.add_argument("--weights", type=Path, required=True, help="Lowe's Weights for Labels.xlsx")
    p.add_argument("--output", type=Path, required=True, help="Output folder for FedEx xlsx")
    p.add_argument("--archive", type=Path, required=True, help="Archive folder for source CSV copies")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument(
        "--force",
        action="store_true",
        help="Run even when CSV automation is disabled or AUTOMATION_STOP.txt exists (operator override)",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    out, rows, unk, arch = process_one_csv(
        args.input,
        args.mapping,
        args.weights,
        args.output,
        args.archive,
        dry_run=args.dry_run,
        force=args.force,
    )
    if args.dry_run:
        print(f"DRY RUN: would write {rows} row(s), unknown SKU rows skipped: {unk}")
    else:
        print(f"Wrote {out} ({rows} rows), unknown SKU rows skipped: {unk}")
        print(f"Archived: {arch}")


if __name__ == "__main__":
    main()
