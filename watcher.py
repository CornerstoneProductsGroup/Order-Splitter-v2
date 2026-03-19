"""
File Watcher — Auto-processes PDFs dropped into retailer watch folders.

Watch folders (created automatically on first run):
  ./watch/home_depot/      →  processed as Home Depot orders
  ./watch/lowes/           →  processed as Lowe's orders
  ./watch/tractor_supply/  →  processed as Tractor Supply orders

Output is written to:
  ./watch/output/

Run:
  python watcher.py

Stop:
  Ctrl+C
"""

import io
import json
import logging
import os
import re
import time
import zipfile as zf
from collections import defaultdict
from io import BytesIO
from pathlib import Path

import fitz  # PyMuPDF
import pandas as pd
from pypdf import PdfReader, PdfWriter
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

# ─────────────────────────────────────────────────────────────────────────────
# Directories
# ─────────────────────────────────────────────────────────────────────────────

WATCH_DIRS: dict[str, Path] = {
    "Home Depot":     Path(r"\\rygarcorp.com\shares\Cornerstone\Dot Com Packing Slips\1-Orders Before Extraction\1-Depot"),
    "Lowe's":         Path(r"\\rygarcorp.com\shares\Cornerstone\Dot Com Packing Slips\1-Orders Before Extraction\2-Lowe's"),
    "Tractor Supply": Path(r"\\rygarcorp.com\shares\Cornerstone\Dot Com Packing Slips\1-Orders Before Extraction\3-Tractor Supply"),
}
OUTPUT_ROOT = Path(r"\\rygarcorp.com\shares\Cornerstone\Dot Com Packing Slips\1-Orders Before Extraction\Order Splitter Output")
OUTPUT_DIRS: dict[str, Path] = {
    "Home Depot":     OUTPUT_ROOT / "Depot",
    "Lowe's":         OUTPUT_ROOT / "Lowe's",
    "Tractor Supply": OUTPUT_ROOT / "Tractor Supply",
}

ROUTES_XLSX_PATH = Path("Vendor Output Routes.xlsx")
ROUTES_REQUIRED_COLS = ["Retailer", "Vendor"]
ROUTES_PATH_COL_CANDIDATES = ["DestinationPath", "Path"]

# ─────────────────────────────────────────────────────────────────────────────
# Config  (mirrors app.py)
# ─────────────────────────────────────────────────────────────────────────────

DEFAULT_MAPS: dict[str, str] = {
    "Home Depot":     "vendor_map_hd.xlsx",
    "Lowe's":         "vendor_map_lowes.xlsx",
    "Tractor Supply": "vendor_map_tsc.xlsx",
}
MAP_KEY_COL: dict[str, str] = {
    "Home Depot":     "Model Number",
    "Lowe's":         "SKU",
    "Tractor Supply": "SKU",
}
MAP_VENDOR_COL = "Vendor"

WAREHOUSE_VENDORS = [
    "Cord Mate",
    "Gate Latch",
    "Home Selects",
    "Nisus",
    "Post Protector-Here",
    "Soft Seal",
    "Weedshark",
    "Zaca",
    "Cornerstone",
]

CROP_CONFIG_PATH = "crop_config.json"
CROP_CONFIG_DEFAULTS: dict[str, dict] = {
    "Home Depot": {
        "extract_region": {"x0": 0.02, "x1": 0.14, "y0": 0.26, "y1": 0.54},
    },
    "Lowe's": {
        "extract_region": {"x0": 0.52, "x1": 0.79, "y0": 0.25, "y1": 0.67},
        "sos_output_crop": {"x0": 0.52, "x1": 0.79, "y0": 0.25, "y1": 0.67},
    },
    "Tractor Supply": {
        "extract_region": {"x0": 0.14, "x1": 0.30, "y0": 0.20, "y1": 0.55},
        "redact_regions": [],
    },
}

CONFIDENCE_THRESHOLD = 70  # pages below this are flagged REVIEW


def _normalize_region(region: dict | None, fallback: dict | None = None) -> dict:
    base = fallback or {"x0": 0.0, "x1": 1.0, "y0": 0.0, "y1": 1.0}
    src = region or {}
    x0f = float(src.get("x0", base.get("x0", 0.0)))
    x1f = float(src.get("x1", base.get("x1", 1.0)))
    y0f = float(src.get("y0", base.get("y0", 0.0)))
    y1f = float(src.get("y1", base.get("y1", 1.0)))

    x0f = max(0.0, min(1.0, x0f))
    x1f = max(0.0, min(1.0, x1f))
    y0f = max(0.0, min(1.0, y0f))
    y1f = max(0.0, min(1.0, y1f))

    if x1f < x0f:
        x0f, x1f = x1f, x0f
    if y1f < y0f:
        y0f, y1f = y1f, y0f

    return {"x0": x0f, "x1": x1f, "y0": y0f, "y1": y1f}


def _extract_region_from_cfg(retailer: str, crop_cfg: dict) -> dict:
    raw = crop_cfg.get(retailer, {})
    if not isinstance(raw, dict):
        return {"x0": 0.0, "x1": 1.0, "y0": 0.0, "y1": 1.0}

    # Backward compatible: legacy config had x0/x1/y0/y1 at the retailer root.
    if all(k in raw for k in ("x0", "x1", "y0", "y1")):
        return _normalize_region(raw)

    return _normalize_region(raw.get("extract_region"))


def _sos_crop_region_from_cfg(crop_cfg: dict) -> dict | None:
    raw = crop_cfg.get("Lowe's", {})
    if not isinstance(raw, dict):
        return None
    region = raw.get("sos_output_crop")
    if not isinstance(region, dict):
        return None
    return _normalize_region(region)


def _redact_regions_from_cfg(crop_cfg: dict) -> list[dict]:
    raw = crop_cfg.get("Tractor Supply", {})
    if not isinstance(raw, dict):
        return []
    regions = raw.get("redact_regions", [])
    if not isinstance(regions, list):
        return []

    cleaned: list[dict] = []
    for reg in regions:
        if isinstance(reg, dict):
            cleaned.append(_normalize_region(reg))
    return cleaned


def _region_to_rect(page: fitz.Page, region: dict) -> fitz.Rect:
    w = page.rect.width
    h = page.rect.height
    left = region["x0"] * w
    right = region["x1"] * w
    top = (1 - region["y1"]) * h
    bottom = (1 - region["y0"]) * h
    return fitz.Rect(left, top, right, bottom)

# ─────────────────────────────────────────────────────────────────────────────
# Core processing helpers  (exact copies of the logic in app.py)
# ─────────────────────────────────────────────────────────────────────────────

def normalize_key(x: str) -> str:
    if x is None:
        return ""
    s = str(x).strip().upper()
    s = re.sub(r"[\s\-_]", "", s)
    return s


def normalize_label(x: str) -> str:
    if x is None:
        return ""
    return re.sub(r"[^A-Z0-9]+", "", str(x).strip().upper())


def _is_enabled_cell(v) -> bool:
    if pd.isna(v):
        return True
    s = str(v).strip().lower()
    return s in {"1", "true", "yes", "y", "on", "enabled", ""}


def load_vendor_output_routes(xlsx_path: Path, logger: logging.Logger) -> dict[tuple[str, str], Path]:
    if not xlsx_path.exists():
        logger.warning("Routes file not found: %s (routing disabled)", xlsx_path)
        return {}

    try:
        df = pd.read_excel(xlsx_path)
    except Exception as e:
        logger.error("Could not read routes file %s: %s", xlsx_path, e)
        return {}

    missing = [c for c in ROUTES_REQUIRED_COLS if c not in df.columns]
    if missing:
        logger.error("Routes file missing columns %s. Expected: %s", missing, ROUTES_REQUIRED_COLS)
        return {}

    path_col = next((c for c in ROUTES_PATH_COL_CANDIDATES if c in df.columns), None)
    if path_col is None:
        logger.error("Routes file missing path column. Expected one of: %s", ROUTES_PATH_COL_CANDIDATES)
        return {}

    routes: dict[tuple[str, str], Path] = {}
    enabled_col = "Enabled" if "Enabled" in df.columns else None

    for _, row in df.iterrows():
        if enabled_col and not _is_enabled_cell(row.get(enabled_col)):
            continue

        retailer = normalize_label(row.get("Retailer"))
        vendor = normalize_label(row.get("Vendor"))
        dest_raw = row.get(path_col)

        if not retailer or not vendor or pd.isna(dest_raw):
            continue

        dest = Path(str(dest_raw).strip())
        if not str(dest):
            continue

        routes[(retailer, vendor)] = dest

    logger.info("Loaded %d vendor output route(s) from %s", len(routes), xlsx_path)
    return routes


def resolve_route_path(routes: dict[tuple[str, str], Path], retailer: str, vendor: str) -> Path | None:
    r = normalize_label(retailer)
    v = normalize_label(vendor)

    # Supports explicit matches and default fallbacks.
    candidates = [
        (r, v),
        (r, "DEFAULT"),
        ("DEFAULT", v),
        ("DEFAULT", "DEFAULT"),
    ]
    for key in candidates:
        if key in routes:
            return routes[key]
    return None


def load_crop_config() -> dict:
    try:
        if os.path.exists(CROP_CONFIG_PATH):
            with open(CROP_CONFIG_PATH, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict):
                for r, d in CROP_CONFIG_DEFAULTS.items():
                    data.setdefault(r, d)
                return data
    except Exception:
        pass
    return dict(CROP_CONFIG_DEFAULTS)


def load_vendor_map(retailer: str) -> pd.DataFrame:
    return pd.read_excel(DEFAULT_MAPS[retailer])


def build_lookup(df: pd.DataFrame, retailer: str) -> dict:
    key_col = MAP_KEY_COL[retailer]
    if key_col not in df.columns or MAP_VENDOR_COL not in df.columns:
        raise ValueError(
            f"Vendor map for {retailer} must include columns '{key_col}' and '{MAP_VENDOR_COL}'. "
            f"Found: {list(df.columns)}"
        )
    lookup: dict[str, str] = {}
    for _, row in df.iterrows():
        k = normalize_key(row.get(key_col))
        v_raw = row.get(MAP_VENDOR_COL)
        v = str(v_raw).strip() if pd.notna(v_raw) else ""
        if not k or not v:
            continue
        has_digit = any(ch.isdigit() for ch in k)
        if (not has_digit) and len(k) < 4:
            continue
        lookup[k] = v
    return lookup


def is_sos_tag_page(text: str) -> bool:
    t = (text or "").upper()
    return any(
        kw in t
        for kw in ["SOS", "SHIP TO STORE", "STORE PICKUP", "PICK UP IN STORE", "S2S", "SPECIAL ORDER"]
    )


def extract_text_by_page_with_regions(pdf_bytes: bytes, retailer: str, crop_cfg: dict) -> list[dict]:
    cfg = _extract_region_from_cfg(retailer, crop_cfg)
    x0f = cfg["x0"]
    x1f = cfg["x1"]
    y0f = cfg["y0"]
    y1f = cfg["y1"]

    reader = PdfReader(BytesIO(pdf_bytes))
    full_texts: list[str] = []
    for page in reader.pages:
        try:
            full_texts.append(page.extract_text() or "")
        except Exception:
            full_texts.append("")

    region_texts: list[str] = []
    try:
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        for i in range(doc.page_count):
            page = doc.load_page(i)
            w = page.rect.width
            h = page.rect.height
            left  = x0f * w
            right = x1f * w
            top   = (1 - y1f) * h
            bottom = (1 - y0f) * h
            rect_rot = fitz.Rect(left, top, right, bottom)
            rect = rect_rot * page.derotation_matrix
            words = page.get_text("words")
            tol = 1.0
            picked = [
                w for w in words
                if w[0] >= rect.x0 - tol and w[2] <= rect.x1 + tol
                and w[1] >= rect.y0 - tol and w[3] <= rect.y1 + tol
            ]
            picked.sort(key=lambda x: (round(x[1], 1), x[0]))
            txt = " ".join([w[4] for w in picked]).strip()
            region_texts.append(txt if txt else full_texts[i])
    except Exception:
        region_texts = full_texts[:]

    return [{"full": full_texts[i], "region": region_texts[i]} for i in range(len(full_texts))]


def match_vendor(text: str, lookup: dict) -> tuple[str, list[str], int]:
    raw = (text or "").upper()
    compact = normalize_key(text)
    matched: list[str] = []
    vendors: set[str] = set()

    for k, vendor in lookup.items():
        if not k:
            continue
        if k in compact:
            matched.append(k)
            vendors.add(vendor)
            continue
        if re.search(rf"\b{re.escape(k)}\b", raw):
            matched.append(k)
            vendors.add(vendor)

    if not vendors:
        return "UNKNOWN", [], 0
    if len(vendors) > 1:
        return "MIXED/REVIEW", matched[:15], 25

    hit = len(set(matched))
    conf = (
        98 if hit >= 5 else
        95 if hit == 4 else
        92 if hit == 3 else
        88 if hit == 2 else
        80 if hit == 1 else
        60
    )
    return next(iter(vendors)), matched[:15], conf


def build_vendor_pdfs(pdf_bytes: bytes, page_vendor_rows: list[dict], retailer: str, crop_cfg: dict) -> dict[str, bytes]:
    src_doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    pages_by_vendor: dict[str, list[int]] = defaultdict(list)
    row_by_page: dict[int, dict] = {}
    for r in page_vendor_rows:
        pages_by_vendor[r["Vendor"]].append(r["PageIndex"])
        row_by_page[r["PageIndex"]] = r

    sos_crop = _sos_crop_region_from_cfg(crop_cfg) if retailer == "Lowe's" else None
    redact_regions = _redact_regions_from_cfg(crop_cfg) if retailer == "Tractor Supply" else []

    vendor_pdfs: dict[str, bytes] = {}
    for vendor, idxs in pages_by_vendor.items():
        out_doc = fitz.open()
        for i in idxs:
            out_doc.insert_pdf(src_doc, from_page=i, to_page=i)
            page = out_doc[-1]
            row = row_by_page.get(i, {})

            if sos_crop is not None and bool(row.get("SOS Tag", False)):
                page.set_cropbox(_region_to_rect(page, sos_crop))

            if redact_regions:
                for reg in redact_regions:
                    page.add_redact_annot(_region_to_rect(page, reg), fill=(0, 0, 0))
                page.apply_redactions()

        buf = BytesIO()
        out_doc.save(buf)
        out_doc.close()
        vendor_pdfs[vendor] = buf.getvalue()

    src_doc.close()
    return vendor_pdfs


def build_warehouse_print_pdf(
    pdf_bytes: bytes,
    page_vendor_rows: list[dict],
    vendors: list[str],
) -> bytes | None:
    reader = PdfReader(BytesIO(pdf_bytes))
    pages_by_vendor: dict[str, list[int]] = defaultdict(list)
    for r in page_vendor_rows:
        pages_by_vendor[r["Vendor"]].append(r["PageIndex"])

    target = [v for v in vendors if v in pages_by_vendor]
    if not target:
        return None

    writer = PdfWriter()
    for vendor in sorted(target, key=lambda x: x.lower()):
        for i in sorted(pages_by_vendor[vendor]):
            writer.add_page(reader.pages[i])
    buf = BytesIO()
    writer.write(buf)
    return buf.getvalue()


def build_zip(
    vendor_pdfs: dict[str, bytes],
    base_name: str,
    warehouse_print_pdf: bytes | None,
    report_csv: bytes,
    review_files: dict[str, bytes],
) -> bytes:
    buf = BytesIO()
    base = re.sub(r"\.pdf$", "", base_name, flags=re.IGNORECASE).strip()
    base = re.sub(r"[\\/:*?\"<>|]+", "_", base).strip() or "Orders"

    with zf.ZipFile(buf, "w", compression=zf.ZIP_DEFLATED) as z:
        z.writestr(f"{base} - Report.csv", report_csv)

        for rel_name, data in review_files.items():
            z.writestr(f"Needs Review/{rel_name}", data)

        if warehouse_print_pdf is not None:
            z.writestr(f"{base} - WAREHOUSE PRINT.pdf", warehouse_print_pdf)
        for vendor, data in vendor_pdfs.items():
            safe_vendor = re.sub(r"[^\w\-. ]+", "_", vendor).strip() or "UNKNOWN"
            z.writestr(f"{base} - {safe_vendor}.pdf", data)

    return buf.getvalue()


def write_and_route_vendor_pdfs(
    vendor_pdfs: dict[str, bytes],
    base_name: str,
    retailer: str,
    output_dir: Path,
    routes: dict[tuple[str, str], Path],
    logger: logging.Logger,
) -> None:
    base = re.sub(r"\.pdf$", "", base_name, flags=re.IGNORECASE).strip()
    base = re.sub(r"[\\/:*?\"<>|]+", "_", base).strip() or "Orders"
    unmapped_dir = output_dir / "Unmapped Vendor Routes"

    for vendor, data in vendor_pdfs.items():
        safe_vendor = re.sub(r"[^\w\-. ]+", "_", vendor).strip() or "UNKNOWN"
        filename = f"{base} - {safe_vendor}.pdf"

        route_dir = resolve_route_path(routes, retailer, vendor)
        if route_dir is None:
            route_dir = unmapped_dir
            logger.warning("[%s] No vendor route for '%s'; sent to %s", retailer, vendor, route_dir)

        route_dir.mkdir(parents=True, exist_ok=True)
        routed_file = route_dir / filename
        routed_file.write_bytes(data)


# ─────────────────────────────────────────────────────────────────────────────
# Wait until a file is fully written (size-stability check)
# ─────────────────────────────────────────────────────────────────────────────

def _wait_for_file_ready(path: Path, stable_secs: float = 1.0, timeout_secs: float = 60.0) -> bool:
    """
    Poll until the file size stops growing.  Returns True if the file
    stabilised within timeout_secs, False otherwise.
    """
    deadline = time.monotonic() + timeout_secs
    prev_size = -1
    while time.monotonic() < deadline:
        try:
            size = path.stat().st_size
        except OSError:
            time.sleep(0.2)
            continue
        if size == prev_size and size > 0:
            return True
        prev_size = size
        time.sleep(stable_secs)
    return False


# ─────────────────────────────────────────────────────────────────────────────
# Main processing function
# ─────────────────────────────────────────────────────────────────────────────

def process_pdf(
    pdf_path: Path,
    retailer: str,
    crop_cfg: dict,
    output_dir: Path,
    routes: dict[tuple[str, str], Path],
    logger: logging.Logger,
) -> None:
    logger.info("[%s] Processing: %s", retailer, pdf_path.name)

    try:
        pdf_bytes = pdf_path.read_bytes()
    except OSError as e:
        logger.error("[%s] Cannot read %s: %s", retailer, pdf_path.name, e)
        return

    pdf_name = pdf_path.name

    try:
        df_map = load_vendor_map(retailer)
        lookup = build_lookup(df_map, retailer)
    except Exception as e:
        logger.error("[%s] Vendor map error: %s", retailer, e)
        return

    pages = extract_text_by_page_with_regions(pdf_bytes, retailer, crop_cfg)

    rows: list[dict] = []
    for i, pobj in enumerate(pages):
        full   = pobj.get("full", "")
        region = pobj.get("region", "")

        # Lowe's SOS tag: detect via full page, assign to previous vendor
        if retailer == "Lowe's" and is_sos_tag_page(full):
            if rows:
                final_vendor = rows[-1]["Vendor"]
                conf = max(int(rows[-1].get("Confidence %", 0)), 80)
            else:
                final_vendor = "REVIEW"
                conf = 50
            rows.append({
                "Page": i + 1,
                "Vendor": final_vendor,
                "Confidence %": conf,
                "SOS Tag": True,
                "Matched SKU/Model (first 15)": "",
            })
            continue

        scan_text = (region or "").strip()
        if not scan_text:
            rows.append({
                "Page": i + 1,
                "Vendor": "REVIEW",
                "Confidence %": 0,
                "SOS Tag": False,
                "Matched SKU/Model (first 15)": "",
            })
            continue

        vendor, matched, conf = match_vendor(scan_text, lookup)
        final_vendor = vendor
        if conf < CONFIDENCE_THRESHOLD and vendor not in ("UNKNOWN", "MIXED/REVIEW"):
            final_vendor = "REVIEW"

        rows.append({
            "Page": i + 1,
            "Vendor": final_vendor,
            "Detected Vendor": vendor,
            "Confidence %": conf,
            "SOS Tag": False,
            "Matched SKU/Model (first 15)": ", ".join(matched) if matched else "",
        })

    # Build outputs
    page_vendor_rows = [
        {
            "PageIndex": int(r["Page"]) - 1,
            "Vendor": r["Vendor"],
            "SOS Tag": bool(r.get("SOS Tag", False)),
        }
        for r in rows
    ]
    vendor_pdfs      = build_vendor_pdfs(pdf_bytes, page_vendor_rows, retailer, crop_cfg)
    warehouse_pdf    = build_warehouse_print_pdf(pdf_bytes, page_vendor_rows, WAREHOUSE_VENDORS)
    df_report        = pd.DataFrame(rows)

    base         = re.sub(r"\.pdf$", "", pdf_name, flags=re.IGNORECASE).strip()
    retailer_slug = re.sub(r"[^\w]", "_", retailer)
    out_zip      = output_dir / f"{base}_{retailer_slug}_VendorPdfs.zip"
    write_and_route_vendor_pdfs(vendor_pdfs, pdf_name, retailer, output_dir, routes, logger)

    flagged = df_report[df_report["Vendor"].isin(["REVIEW", "UNKNOWN", "MIXED/REVIEW"])].copy()
    review_count = int(flagged.shape[0])

    report_csv_bytes = df_report.to_csv(index=False).encode("utf-8")
    review_files: dict[str, bytes] = {}
    if review_count:
        review_files[pdf_name] = pdf_bytes
        review_files[f"{base}_{retailer_slug}_NeedsReview.csv"] = flagged.to_csv(index=False).encode("utf-8")

    zip_bytes = build_zip(
        vendor_pdfs,
        base_name=pdf_name,
        warehouse_print_pdf=warehouse_pdf,
        report_csv=report_csv_bytes,
        review_files=review_files,
    )
    out_zip.write_bytes(zip_bytes)

    logger.info("[%s] Done → %s", retailer, out_zip.name)
    if review_count:
        logger.warning(
            "[%s] %d page(s) flagged for review — included in ZIP under Needs Review/",
            retailer, review_count,
        )


# ─────────────────────────────────────────────────────────────────────────────
# Watchdog event handler
# ─────────────────────────────────────────────────────────────────────────────

class PDFHandler(FileSystemEventHandler):
    def __init__(
        self,
        retailer: str,
        crop_cfg: dict,
        output_dir: Path,
        routes: dict[tuple[str, str], Path],
        logger: logging.Logger,
    ) -> None:
        super().__init__()
        self.retailer  = retailer
        self.crop_cfg  = crop_cfg
        self.output_dir = output_dir
        self.routes = routes
        self.logger    = logger
        self._last_seen: dict[str, float] = {}

    def _process_if_pdf(self, path: Path, event_label: str) -> None:
        if path.suffix.lower() != ".pdf":
            return

        key = str(path).lower()
        now = time.monotonic()
        if now - self._last_seen.get(key, 0.0) < 10.0:
            return
        self._last_seen[key] = now

        self.logger.info("[%s] Detected %s file: %s", self.retailer, event_label, path.name)

        if not _wait_for_file_ready(path):
            self.logger.error(
                "[%s] Timed out waiting for %s to finish writing — skipping.",
                self.retailer, path.name,
            )
            return

        try:
            process_pdf(path, self.retailer, self.crop_cfg, self.output_dir, self.routes, self.logger)
        except Exception as e:
            self.logger.exception(
                "[%s] Unhandled error processing %s: %s", self.retailer, path.name, e
            )

    def on_created(self, event) -> None:  # type: ignore[override]
        if event.is_directory:
            return
        self._process_if_pdf(Path(event.src_path), "new")

    def on_moved(self, event) -> None:  # type: ignore[override]
        if event.is_directory:
            return
        self._process_if_pdf(Path(event.dest_path), "moved")

    def on_modified(self, event) -> None:  # type: ignore[override]
        # Some save workflows trigger modify events without create/move.
        if event.is_directory:
            return
        self._process_if_pdf(Path(event.src_path), "modified")


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    logger = logging.getLogger("watcher")
    routes = load_vendor_output_routes(ROUTES_XLSX_PATH, logger)
    route_dirs = list({p for p in routes.values()})

    if os.name != "nt" and any(str(p).startswith("\\\\") for p in [*WATCH_DIRS.values(), *OUTPUT_DIRS.values(), *route_dirs]):
        logger.error("UNC paths were configured, but this host is not Windows.")
        logger.error("Run watcher.py on a Windows machine that can access the network share.")
        return

    # Ensure all directories exist
    for d in [*WATCH_DIRS.values(), *OUTPUT_DIRS.values(), *route_dirs]:
        d.mkdir(parents=True, exist_ok=True)

    crop_cfg = load_crop_config()

    observer = Observer()
    for retailer, watch_dir in WATCH_DIRS.items():
        handler = PDFHandler(retailer, crop_cfg, OUTPUT_DIRS[retailer], routes, logger)
        observer.schedule(handler, str(watch_dir), recursive=False)
        logger.info("Watching [%-14s] → %s/", retailer, watch_dir)
        logger.info("Output   [%-14s] → %s/", retailer, OUTPUT_DIRS[retailer])

    logger.info("Watcher running. Drop PDF files into a watch folder. Press Ctrl+C to stop.\n")

    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
        logger.info("Watcher stopped.")
    observer.join()


if __name__ == "__main__":
    main()
