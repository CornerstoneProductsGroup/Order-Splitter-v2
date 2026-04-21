"""
Build WorldShip-ready Depot CSV files from raw Rithum exports.

Workflow:
1) Read each raw CSV from INPUT_DIR.
2) Copy source row data B:V into output columns A:U.
3) Fill constants and SKU-driven routing fields for columns W:AE.
4) Split rows for mixed-box orders so each output row has accurate per-label weights.
5) Sort rows by Save/Print, Vendor order, then SKU sheet order.
6) Write output CSV and move the raw source file into ARCHIVE_DIR.

Run:
  python process_depot_csv_orders.py

Optional overrides:
  python process_depot_csv_orders.py --input "..." --output "..." --archive "..." --rules "..."
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import math
import re
import shutil
from dataclasses import dataclass
from pathlib import Path

import pandas as pd


INPUT_DIR = Path(
    r"\\rygarcorp.com\shares\Cornerstone\Dot Com Packing Slips\1-Orders Before Extraction\6-CSV Order Files\Depot"
)
OUTPUT_DIR = Path(
    r"\\rygarcorp.com\shares\Cornerstone\Dot Com Packing Slips\1-Orders Before Extraction\Order Splitter Output\CSV File Output\Depot"
)
WORLD_SHIP_DROP_DIR = Path(
    r"\\rygarcorp.com\shares\Cornerstone\Dot Com Packing Slips\zzz - Worldship Shipment Files\Cornerstone"
)
ARCHIVE_DIR = Path(
    r"\\rygarcorp.com\shares\Cornerstone\Dot Com Packing Slips\1-Orders Before Extraction\6-CSV Order Files\z- Archive Depot"
)


# Use the rules file from the local project directory by default
RULES_FILENAME = "Weights, Max Units and Printer for CSV routing.xlsx"
RULES_XLSX = Path(__file__).parent / RULES_FILENAME


# Keep this header exactly as WorldShip expects.
WORLD_SHIP_HEADER = [
    "SHPTO_NAME",
    "SHPTO_ADDRESS_1",
    "SHPTO_ADDRESS_2",
    "SHPTO_ADDRESS_3",
    "SHPTO_CITY",
    "SHPTO_STATE_PROV",
    "SHPTO_POSTAL_CODE",
    "SHPTO_COUNTRY_ID",
    "SHPTO_TELEPHONE",
    "PACKAGE_SERVICE",
    "SHIPMENT_TOTAL_WEIGHT",
    "PKG_CUSTOM1",
    "NUMBER_OF_PACKAGES",
    "PACKAGE_TYPE",
    "PURCHASE_ORDER",
    "UNITS",
    "SHPTO_RESIDENTIAL",
    "UOL_SOURCE",
    "SHPTO_ATTN_LINE",
    "SHPTO_COMPANY",
    "MERCHANT_ID",
    "",
    "STORE_NUMBER",
    "LABEL_PRINTER_ID",
    "PROFILE",
    "PACKAGE_SERVICE",
    "PACKAGE_TYPE",
    "SKU",
    "Units",
    "SHIPMENT_TOTAL_WEIGHT",
    "NUMBER_OF_PACKAGES",
]

# 0-based output indices for W..AE.
IDX_W = 22
IDX_X = 23
IDX_Y = 24
IDX_Z = 25
IDX_AA = 26
IDX_AB = 27
IDX_AC = 28
IDX_AD = 29
IDX_AE = 30

IDX_OUTPUT_L = 11
IDX_OUTPUT_P = 15
IDX_PURCHASE_ORDER = 14


@dataclass(frozen=True)
class SkuRule:
    sku: str
    unit_weight: float
    fixed_box_weight: float | None
    min_units_per_box: int
    max_units_per_box: int
    weight_mode: str
    printer: str
    sku_order: int
    vendor_name: str
    vendor_sort_order: int
    label_action: str
    label_action_order: int
    po_description: str


def _norm_text(value: object) -> str:
    return str(value).strip() if value is not None else ""


def _norm_sku(value: object) -> str:
    s = _norm_text(value).upper()
    s = re.sub(r"\s+", "", s)
    return s


def _parse_int(value: object, default: int = 0) -> int:
    if pd.isna(value):
        return default
    txt = _norm_text(value)
    if not txt:
        return default
    txt = txt.replace(",", "")
    try:
        return int(float(txt))
    except ValueError:
        return default


def _parse_float(value: object, default: float = 0.0) -> float:
    if pd.isna(value):
        return default
    txt = _norm_text(value)
    if not txt:
        return default
    txt = txt.replace(",", "")
    try:
        parsed = float(txt)
        if math.isnan(parsed):
            return default
        return parsed
    except ValueError:
        return default


def _fmt_number(value: float) -> str:
    if float(value).is_integer():
        return str(int(value))
    return f"{value:.3f}".rstrip("0").rstrip(".")


def _normalize_postal_code(postal_code: str, country_code: str) -> str:
    postal = _norm_text(postal_code)
    country = _norm_text(country_code).upper()

    # Preserve leading-zero ZIP codes for US addresses.
    if country == "US":
        digits_only = re.sub(r"\D", "", postal)
        if digits_only and len(digits_only) < 5:
            return digits_only.zfill(5)

    return postal


def _find_col(df: pd.DataFrame, candidates: list[str], required: bool = False) -> str | None:
    normalized = {str(c).strip().lower(): c for c in df.columns}
    for c in candidates:
        hit = normalized.get(c.strip().lower())
        if hit is not None:
            return hit
    if required:
        raise ValueError(f"Missing required column. Tried: {candidates}. Found: {list(df.columns)}")
    return None


def _weight_mode_from_row(weight_mode: str, fixed_box_weight: float | None) -> str:
    mode = weight_mode.strip().lower()
    if mode in {"fixed", "flat", "box", "box_weight", "boxweight", "label", "case"}:
        return "fixed"
    if fixed_box_weight is not None:
        return "fixed"
    return "per_unit"


def _box_weight_for_rule(rule: SkuRule, package_units: int, package_count: int) -> float:
    if rule.weight_mode == "fixed":
        fixed_weight = rule.fixed_box_weight or 0.0
        return package_count * fixed_weight
    return package_units * rule.unit_weight


def _select_pack_rule(remaining_units: int, rules: list[SkuRule]) -> tuple[SkuRule, int]:
    ordered = sorted(
        rules,
        key=lambda rule: (rule.max_units_per_box, rule.min_units_per_box, -rule.sku_order),
        reverse=True,
    )

    for rule in ordered:
        package_units = min(remaining_units, max(1, rule.max_units_per_box))
        if package_units >= max(1, rule.min_units_per_box):
            return rule, package_units

    fallback = min(ordered, key=lambda rule: (rule.max_units_per_box, rule.sku_order))
    return fallback, min(remaining_units, max(1, fallback.max_units_per_box))


def _build_package_plan(total_units: int, rules: list[SkuRule]) -> list[tuple[SkuRule, int, int]]:
    if total_units <= 0:
        return []

    boxes: list[tuple[SkuRule, int]] = []
    remaining_units = total_units
    while remaining_units > 0:
        rule, package_units = _select_pack_rule(remaining_units, rules)
        package_units = max(1, min(package_units, remaining_units))
        boxes.append((rule, package_units))
        remaining_units -= package_units

    grouped: list[tuple[SkuRule, int, int]] = []
    for rule, package_units in boxes:
        if grouped and grouped[-1][0] == rule and grouped[-1][1] == package_units:
            prev_rule, prev_units, prev_count = grouped[-1]
            grouped[-1] = (prev_rule, prev_units, prev_count + 1)
        else:
            grouped.append((rule, package_units, 1))

    return grouped


def load_sku_rules(path: Path) -> dict[str, list[SkuRule]]:
    if not path.exists():
        raise FileNotFoundError(f"Rules file not found: {path}")

    df = pd.read_excel(path)

    col_sku = _find_col(df, ["SKU"], required=True)
    col_weight = _find_col(
        df,
        ["UnitWeight", "Unit Weight", "Weight", "EachWeight", "ItemWeight", "Label Weight"],
        required=True,
    )
    col_max = _find_col(
        df,
        ["MaxUnitsPerBox", "Max Unit per box", "MaxUnits", "Max Per Box", "UnitsPerBox"],
        required=True,
    )
    col_min = _find_col(
        df,
        ["MinUnitsPerBox", "Min Unit per box", "MinUnits", "Min Qty", "MinQty", "Min Order Qty"],
    )
    col_printer = _find_col(df, ["Printer", "LABEL_PRINTER_ID"], required=True)
    col_box_weight = _find_col(
        df,
        ["BoxWeight", "Box Weight", "FixedBoxWeight", "Fixed Box Weight", "FlatWeight", "LabelWeight", "ShipmentWeight"],
    )
    col_weight_mode = _find_col(df, ["WeightMode", "Weight Type", "Weight Basis", "WeightMethod"])

    col_vendor = _find_col(df, ["VendorName", "Vendor", "Vendor Name"])
    col_po_desc = _find_col(
        df,
        [
            "Added Description with PO#",
            "PO Description",
            "PO Descriptor",
            "Reference Description",
            "PKG_CUSTOM4 Description",
        ],
    )
    col_vendor_order = _find_col(df, ["VendorSortOrder", "VendorOrder", "Vendor Sort", "Vendor Priority"])
    col_action = _find_col(df, ["LabelAction", "Action", "SaveOrPrint", "Label Mode"])
    col_action_order = _find_col(df, ["LabelActionOrder", "ActionOrder", "Action Priority"])

    rules: dict[str, list[SkuRule]] = {}
    for idx, row in df.iterrows():
        sku_raw = row.get(col_sku)  # type: ignore[arg-type]
        sku = _norm_sku(sku_raw)
        if not sku:
            continue

        unit_weight = _parse_float(row.get(col_weight), 0.0)  # type: ignore[arg-type]
        fixed_box_weight = _parse_float(row.get(col_box_weight), 0.0) if col_box_weight else 0.0
        max_units = _parse_int(row.get(col_max), 1)  # type: ignore[arg-type]
        min_units = _parse_int(row.get(col_min), 1) if col_min else 1
        if max_units <= 0:
            max_units = 1
        if min_units <= 0:
            min_units = 1
        if min_units > max_units:
            min_units = max_units

        printer = _norm_text(row.get(col_printer))  # type: ignore[arg-type]
        raw_weight_mode = _norm_text(row.get(col_weight_mode)) if col_weight_mode else ""
        parsed_fixed_box_weight = fixed_box_weight if fixed_box_weight > 0 else None
        weight_mode = _weight_mode_from_row(raw_weight_mode, parsed_fixed_box_weight)
        if weight_mode == "fixed" and parsed_fixed_box_weight is None:
            parsed_fixed_box_weight = unit_weight

        vendor_name = _norm_text(row.get(col_vendor)) if col_vendor else ""
        if col_po_desc:
            po_desc_raw = row.get(col_po_desc)  # type: ignore[arg-type]
            po_description = "" if pd.isna(po_desc_raw) else _norm_text(po_desc_raw)
        else:
            po_description = ""
        vendor_sort_order = _parse_int(row.get(col_vendor_order), 9999) if col_vendor_order else 9999

        label_action = _norm_text(row.get(col_action)).lower() if col_action else ""
        if col_action_order:
            label_action_order = _parse_int(row.get(col_action_order), 9)
        else:
            if label_action.startswith("save"):
                label_action_order = 1
            elif label_action.startswith("print"):
                label_action_order = 2
            else:
                label_action_order = 9

        rules.setdefault(sku, []).append(SkuRule(
            sku=sku,
            unit_weight=unit_weight,
            fixed_box_weight=parsed_fixed_box_weight,
            min_units_per_box=min_units,
            max_units_per_box=max_units,
            weight_mode=weight_mode,
            printer=printer,
            sku_order=idx,
            vendor_name=vendor_name,
            vendor_sort_order=vendor_sort_order,
            label_action=label_action,
            label_action_order=label_action_order,
            po_description=po_description,
        ))

    if not rules:
        raise ValueError(f"No SKU rules loaded from {path}")

    # Debug: Print all loaded SKUs
    print(f"Loaded {len(rules)} SKUs from rules file: {path}")
    for sku in sorted(rules.keys()):
        print(f"  SKU: {sku} ({len(rules[sku])} rule(s))")

    return rules


def build_base_output_row(raw_row: list[str]) -> list[str]:
    # Raw row B:V -> output A:U.
    b_to_v = raw_row[1:22] + [""] * max(0, 21 - len(raw_row[1:22]))
    out = [""] * 31
    out[0:21] = b_to_v[:21]

    out[6] = _normalize_postal_code(out[6], out[7])

    # Constants
    out[IDX_W] = "8119"
    out[IDX_Y] = ".com"
    out[IDX_Z] = "GND"
    out[IDX_AA] = "CP"

    # AB from output L, AC from output P.
    out[IDX_AB] = out[IDX_OUTPUT_L]
    out[IDX_AC] = out[IDX_OUTPUT_P]

    return out


def split_row_for_labels(base_row: list[str], rules: list[SkuRule] | None) -> list[tuple[list[str], int]]:
    units = _parse_int(base_row[IDX_AC], 0)

    if units <= 0:
        row = base_row.copy()
        row[IDX_AE] = "1"
        return [(row, 0)]

    if not rules:
        row = base_row.copy()
        row[IDX_AE] = "1"
        return [(row, 0)]

    out: list[tuple[list[str], int]] = []
    for split_idx, (rule, box_units, box_count) in enumerate(_build_package_plan(units, rules)):
        row = base_row.copy()
        piece_units = box_units * box_count
        row[IDX_AC] = str(piece_units)
        row[IDX_AD] = _fmt_number(_box_weight_for_rule(rule, piece_units, box_count))
        row[IDX_AE] = str(box_count)
        out.append((row, split_idx))

    return out


def process_file(raw_csv: Path, rules: dict[str, list[SkuRule]], output_dir: Path) -> tuple[Path, int, int]:
    with raw_csv.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        rows = list(reader)

    if len(rows) <= 1:
        raise ValueError(f"No data rows found in {raw_csv.name}")

    records: list[tuple[tuple, list[str]]] = []
    unknown_skus = 0

    for source_idx, raw_row in enumerate(rows[1:], start=1):
        # Need at least through source column V.
        if len(raw_row) < 22:
            continue

        base = build_base_output_row(raw_row)
        sku = _norm_sku(base[IDX_AB])
        sku_rules = rules.get(sku)
        primary_rule = None if not sku_rules else _select_pack_rule(_parse_int(base[IDX_AC], 0), sku_rules)[0]

        if primary_rule is not None:
            base[IDX_X] = primary_rule.printer
            if primary_rule.po_description:
                po_value = _norm_text(base[IDX_PURCHASE_ORDER])
                base[IDX_PURCHASE_ORDER] = f"{po_value} {primary_rule.po_description}".strip()
        else:
            unknown_skus += 1
            continue

        expanded = split_row_for_labels(base, sku_rules)

        for split_idx, (row_out, row_split_index) in enumerate(expanded):
            sort_key = (
                primary_rule.label_action_order,
                primary_rule.vendor_sort_order,
                primary_rule.sku_order,
                source_idx,
                row_split_index,
            )
            records.append((sort_key, row_out))

    records.sort(key=lambda x: x[0])

    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    out_name = f"{raw_csv.stem}_WorldShip_{stamp}.csv"
    out_path = output_dir / out_name

    with out_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(WORLD_SHIP_HEADER)
        for _key, row in records:
            writer.writerow(row)

    # Also write a fixed-name CSV for WorldShip import location.
    WORLD_SHIP_DROP_DIR.mkdir(parents=True, exist_ok=True)
    worldship_path = WORLD_SHIP_DROP_DIR / "CornerstoneMaster.csv"
    with worldship_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(WORLD_SHIP_HEADER)
        for _key, row in records:
            writer.writerow(row)

    return out_path, len(records), unknown_skus


def archive_source(raw_csv: Path, archive_dir: Path) -> Path:
    archive_dir.mkdir(parents=True, exist_ok=True)
    target = archive_dir / raw_csv.name
    if target.exists():
        stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
        target = archive_dir / f"{raw_csv.stem}_{stamp}{raw_csv.suffix}"
    shutil.copy2(str(raw_csv), str(target))
    return target


def build_preview(raw_csv: Path, rules: dict[str, list[SkuRule]]) -> tuple[int, int]:
    """Return output row count and unknown SKU count without writing files."""
    with raw_csv.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        rows = list(reader)

    if len(rows) <= 1:
        raise ValueError(f"No data rows found in {raw_csv.name}")

    out_rows = 0
    unknown_skus = 0

    for raw_row in rows[1:]:
        if len(raw_row) < 22:
            continue
        base = build_base_output_row(raw_row)
        sku = _norm_sku(base[IDX_AB])
        sku_rules = rules.get(sku)
        if sku_rules is None:
            unknown_skus += 1

        out_rows += len(split_row_for_labels(base, sku_rules))

    return out_rows, unknown_skus


def process_one_csv(
    raw_csv: Path,
    rules: dict[str, list[SkuRule]],
    output_dir: Path,
    archive_dir: Path,
    dry_run: bool = False,
) -> tuple[Path | None, int, int, Path | None]:
    """Process one raw CSV.

    Returns: (output_path_or_none, output_row_count, unknown_sku_count, archive_path_or_none)
    """
    if dry_run:
        out_rows, unknown_skus = build_preview(raw_csv, rules)
        return None, out_rows, unknown_skus, None

    out_path, out_rows, unknown_skus = process_file(raw_csv, rules, output_dir)
    archived_to = archive_source(raw_csv, archive_dir)
    return out_path, out_rows, unknown_skus, archived_to


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Process Depot raw CSVs into WorldShip-ready CSV output.")
    parser.add_argument("--input", type=Path, default=INPUT_DIR, help="Input folder for raw Rithum CSV files")
    parser.add_argument("--output", type=Path, default=OUTPUT_DIR, help="Output folder for WorldShip CSV files")
    parser.add_argument("--archive", type=Path, default=ARCHIVE_DIR, help="Archive folder for processed raw files")
    parser.add_argument("--rules", type=Path, default=RULES_XLSX, help="SKU rules workbook path")
    parser.add_argument("--dry-run", action="store_true", help="Preview processing without writing output or archiving files")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    rules = load_sku_rules(args.rules)

    input_dir: Path = args.input
    if not input_dir.exists():
        raise FileNotFoundError(f"Input folder not found: {input_dir}")

    raw_files = sorted(input_dir.glob("*.csv"), key=lambda p: p.name.lower())
    if not raw_files:
        print(f"No CSV files found in {input_dir}")
        return

    for raw_csv in raw_files:
        try:
            out_path, out_rows, unknown_skus, archived_to = process_one_csv(
                raw_csv,
                rules,
                args.output,
                args.archive,
                dry_run=bool(args.dry_run),
            )
            if args.dry_run:
                print(f"DRY RUN: {raw_csv.name} -> would create {out_rows} output row(s)")
                print(f"DRY RUN: would overwrite {WORLD_SHIP_DROP_DIR / 'CornerstoneMaster.csv'}")
            else:
                assert out_path is not None
                assert archived_to is not None
                print(f"Processed: {raw_csv.name} -> {out_path.name} ({out_rows} rows)")
                print(f"WorldShip: {(WORLD_SHIP_DROP_DIR / 'CornerstoneMaster.csv')}")
            if unknown_skus:
                print(f"  Warning: {unknown_skus} row(s) had SKU not found in rules and were skipped.")
            if args.dry_run:
                print("Archived:  (skipped in dry-run)")
            else:
                print(f"Archived copy:  {archived_to}")
        except Exception as e:
            print(f"Failed: {raw_csv.name} -> {e}")


if __name__ == "__main__":
    main()
