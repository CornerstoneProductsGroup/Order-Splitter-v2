"""
File Watcher — Auto-processes PDFs dropped into retailer watch folders.

Watch folders (created automatically on first run):
  ./watch/home_depot/      →  processed as Home Depot orders
  ./watch/lowes/           →  processed as Lowe's orders
  ./watch/tractor_supply/  →  processed as Tractor Supply orders

Output is written to:
  ./watch/output/

Vendor PDFs are also staged daily for email dispatch in:
  ./email_staging/{YYYY-MM-DD}/{VendorName}/

Run the watcher:
  python watcher.py

Send today's vendor emails (Outlook must be open on this machine):
  python send_emails.py
  python send_emails.py --send        # actually send (default is draft mode)
  python send_emails.py --date 2026-03-20  # send a specific past date

Stop:
  Ctrl+C
"""

import io
import hashlib
import json
import threading
import logging
import os
import re
import shutil
import time
import datetime
import zipfile as zf
from collections import defaultdict
from io import BytesIO
from pathlib import Path

from dataclasses import dataclass

import fitz  # PyMuPDF
import pandas as pd
import process_depot_csv_orders as depot_csv
from pypdf import PdfReader
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

# Daily rollup folder (cleared when watcher starts):
#   {root}/{VendorName}/...all vendor PDFs from all retailers for that run
DAILY_VENDOR_ROLLUP_ROOT = Path(r"\\rygarcorp.com\shares\Cornerstone\Dot Com Packing Slips\1-Orders Before Extraction\Order Splitter Output\z- Daily Vendor Orders")

ROUTES_XLSX_PATH = Path("Vendor Output Routes.xlsx")
ROUTES_REQUIRED_COLS = ["Retailer", "Vendor"]
ROUTES_PATH_COL_CANDIDATES = ["DestinationPath", "Path"]

# Depot CSV automation folders.
CSV_INPUT_DIR = Path(
    r"\\rygarcorp.com\shares\Cornerstone\Dot Com Packing Slips\1-Orders Before Extraction\6-CSV Order Files\Depot"
)
CSV_OUTPUT_DIR = Path(
    r"\\rygarcorp.com\shares\Cornerstone\Dot Com Packing Slips\1-Orders Before Extraction\Order Splitter Output\CSV File Output\Depot"
)
CSV_ARCHIVE_DIR = Path(
    r"\\rygarcorp.com\shares\Cornerstone\Dot Com Packing Slips\1-Orders Before Extraction\6-CSV Order Files\z- Archive Depot"
)

# WorldShip label input: one subfolder **per vendor** under this root.
#   e.g.  …\5-WorldShip Labels\Post Protector\shipment_label.pdf
# The watcher watches all vendor subfolders recursively; the vendor name
# is taken from the immediate parent folder of each dropped PDF.
LABEL_WATCH_ROOT = Path(
    r"\\rygarcorp.com\shares\Cornerstone\Dot Com Packing Slips\1-Orders Before Extraction\5-WorldShip Labels"
)
LABEL_WATCH_ENABLED = os.environ.get("ORDER_SPLITTER_DISABLE_LABEL_WATCH", "0").strip().lower() not in {"1", "true", "yes", "y"}

# Output size for a thermal 4×6 label (in PDF points at 72 pt/in).
LABEL_OUTPUT_WIDTH_PT  = 4.0 * 72   # 288 pt
LABEL_OUTPUT_HEIGHT_PT = 6.0 * 72   # 432 pt

# Spreadsheet that configures per-vendor label input/output paths and label size.
# Columns: Retailer | Vendor | Input | Output | Sizing
# Sizing values: "4x6"  → crop to thermal 4×6
#                "8x11" → pass through at original size (no resize)
# Rows with blank Input or Output are skipped (placeholders for future retailers).
LABEL_ROUTES_XLSX_PATH = Path("Vendor Label Paths (Input-Output).xlsx")
CSV_RULES_FILENAME = "Weights, Max Units and Printer for CSV routing.xlsx"
CSV_RULES_XLSX_PATH = Path(CSV_RULES_FILENAME)
CSV_DRY_RUN = os.environ.get("ORDER_SPLITTER_CSV_DRY_RUN", "0").strip().lower() in {"1", "true", "yes", "y"}
CSV_WATCH_ENABLED = os.environ.get("ORDER_SPLITTER_DISABLE_CSV_WATCH", "0").strip().lower() not in {"1", "true", "yes", "y"}
PDF_WATCH_ENABLED = os.environ.get("ORDER_SPLITTER_DISABLE_PDF_WATCH", "0").strip().lower() not in {"1", "true", "yes", "y"}


@dataclass
class LabelVendorRoute:
    """One row from the Label Vendor Routes workbook."""
    retailer: str
    vendor: str
    input_path: Path
    output_path: Path
    resize: bool  # True = crop to 4×6; False = pass through at original size


def load_label_vendor_routes(xlsx_path: Path, logger: logging.Logger) -> list[LabelVendorRoute]:
    """Load per-vendor label routing config from the xlsx spreadsheet.

    Required columns: Retailer, Vendor, Input, Output, Sizing
    Sizing: "4x6" → resize to thermal; "8x11" → no resize.
    Rows with blank Input or Output are skipped.
    Returns an empty list if the file is missing or cannot be parsed.
    """
    if not xlsx_path.exists():
        # Resolve relative to script directory as well.
        alt = Path(__file__).resolve().parent / xlsx_path
        if alt.exists():
            xlsx_path = alt
        else:
            logger.warning("[Labels] Routes workbook not found: %s — falling back to folder-name detection", xlsx_path)
            return []

    try:
        df = pd.read_excel(xlsx_path)
    except Exception as e:
        logger.error("[Labels] Could not read routes workbook %s: %s", xlsx_path, e)
        return []

    required = {"Vendor", "Input", "Output", "Sizing"}
    missing = required - set(df.columns)
    if missing:
        logger.error("[Labels] Routes workbook missing columns: %s", missing)
        return []

    routes: list[LabelVendorRoute] = []
    for _, row in df.iterrows():
        vendor = str(row.get("Vendor", "") or "").strip()
        input_raw = str(row.get("Input", "") or "").strip()
        output_raw = str(row.get("Output", "") or "").strip()
        size_raw = str(row.get("Sizing", "") or "").strip().lower()

        # Skip blank/placeholder rows (e.g. Lowe's/TSC vendors not yet configured).
        if not vendor or not input_raw or not output_raw or not size_raw:
            continue
        if input_raw.lower() == "nan" or output_raw.lower() == "nan":
            continue

        resize = "8x11" not in size_raw  # anything other than 8x11 gets resized
        retailer = str(row.get("Retailer", "") or "").strip()
        routes.append(LabelVendorRoute(
            retailer=retailer,
            vendor=vendor,
            input_path=Path(input_raw),
            output_path=Path(output_raw),
            resize=resize,
        ))
        logger.debug("[Labels] Route: [%s] %s → %s (%s)", retailer, vendor, output_raw, "4x6" if resize else "8x11")

    logger.info("[Labels] Loaded %d vendor route(s) from %s", len(routes), xlsx_path)
    return routes


def _resolve_csv_rules_path(configured_path: Path) -> Path:
    """Resolve CSV rules path robustly for different launch working directories."""
    candidates: list[Path] = []

    script_dir = Path(__file__).resolve().parent

    if configured_path.is_absolute():
        candidates.append(configured_path)

    # Prefer the workbook that lives with this app repository.
    candidates.extend([
        script_dir / configured_path,
        Path.cwd() / configured_path,
        Path(r"C:\OrderSplitter") / CSV_RULES_FILENAME,
        configured_path,
    ])

    for p in candidates:
        if p.exists():
            return p

    return candidates[0] if candidates else configured_path

# Daily staging folder for vendor email attachments.
# Vendor PDFs accumulate here across all retailer runs so one combined
# email per vendor can be sent at end of day via send_emails.py.
EMAIL_STAGING_ROOT = Path("email_staging")

# Persisted state file so daily rollup is not re-cleared if watcher restarts
# later on the same day.
DAILY_ROLLUP_STATE_FILE = OUTPUT_ROOT / ".daily_vendor_rollup_last_cleared.txt"

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
        "sos_output_crop": {"x0": 0.02, "x1": 0.50, "y0": 0.42, "y1": 0.98},
        "sos_output_size_in": {"width": 4.0, "height": 6.0},
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


def _default_region(retailer: str, key: str) -> dict:
    section = CROP_CONFIG_DEFAULTS.get(retailer, {})
    raw = section.get(key, {"x0": 0.0, "x1": 1.0, "y0": 0.0, "y1": 1.0})
    return _normalize_region(raw)


def _merge_retailer_config(retailer: str, raw: dict | None) -> dict:
    section = raw if isinstance(raw, dict) else {}
    merged: dict = {}

    if all(k in section for k in ("x0", "x1", "y0", "y1")):
        merged["extract_region"] = _normalize_region(section)
    else:
        merged["extract_region"] = _normalize_region(
            section.get("extract_region"),
            _default_region(retailer, "extract_region"),
        )

    if retailer == "Lowe's":
        merged["sos_output_crop"] = _normalize_region(
            section.get("sos_output_crop"),
            _default_region(retailer, "sos_output_crop"),
        )
        size_raw = section.get("sos_output_size_in", CROP_CONFIG_DEFAULTS[retailer].get("sos_output_size_in", {}))
        if isinstance(size_raw, dict):
            w = float(size_raw.get("width", 4.0))
            h = float(size_raw.get("height", 6.0))
        else:
            w, h = 4.0, 6.0
        merged["sos_output_size_in"] = {"width": max(1.0, w), "height": max(1.0, h)}
    elif retailer == "Tractor Supply":
        regs = section.get("redact_regions", CROP_CONFIG_DEFAULTS[retailer].get("redact_regions", []))
        merged["redact_regions"] = [_normalize_region(r) for r in regs if isinstance(r, dict)]

    return merged


def _extract_region_from_cfg(retailer: str, crop_cfg: dict) -> dict:
    return _merge_retailer_config(retailer, crop_cfg.get(retailer)).get("extract_region", {"x0": 0.0, "x1": 1.0, "y0": 0.0, "y1": 1.0})


def _sos_crop_region_from_cfg(crop_cfg: dict) -> dict | None:
    return _merge_retailer_config("Lowe's", crop_cfg.get("Lowe's")).get("sos_output_crop")


def _redact_regions_from_cfg(crop_cfg: dict) -> list[dict]:
    return _merge_retailer_config("Tractor Supply", crop_cfg.get("Tractor Supply")).get("redact_regions", [])


def _sos_output_size_points_from_cfg(crop_cfg: dict) -> tuple[float, float]:
    cfg = _merge_retailer_config("Lowe's", crop_cfg.get("Lowe's"))
    size = cfg.get("sos_output_size_in", {"width": 4.0, "height": 6.0})
    w_in = float(size.get("width", 4.0))
    h_in = float(size.get("height", 6.0))
    return (w_in * 72.0, h_in * 72.0)


def _region_to_rect(page: fitz.Page, region: dict) -> fitz.Rect:
    w = page.rect.width
    h = page.rect.height
    left = region["x0"] * w
    right = region["x1"] * w
    top = (1 - region["y1"]) * h
    bottom = (1 - region["y0"]) * h
    rect_rot = fitz.Rect(left, top, right, bottom)
    return rect_rot * page.derotation_matrix


def _region_to_rotated_rect(page: fitz.Page, region: dict) -> fitz.Rect:
    w = page.rect.width
    h = page.rect.height
    left = region["x0"] * w
    right = region["x1"] * w
    top = (1 - region["y1"]) * h
    bottom = (1 - region["y0"]) * h
    return fitz.Rect(left, top, right, bottom)


def _pixmap_nonwhite_ratio(pix: fitz.Pixmap) -> float:
    if pix.alpha:
        px = fitz.Pixmap(fitz.csRGB, pix)
    else:
        px = pix
    data = px.samples
    nonwhite = 0
    total = px.width * px.height
    for i in range(0, len(data), px.n):
        if not (data[i] > 245 and data[i + 1] > 245 and data[i + 2] > 245):
            nonwhite += 1
    return (nonwhite / total) if total else 0.0


def _auto_content_rect(page: fitz.Page, margin: float = 6.0) -> fitz.Rect | None:
    """Return the bounding rect of all vector content (text + drawings) on the page.

    This auto-detects where the label actually is so no manual coordinate
    configuration is needed for Lowe's SOS pages.
    """
    bbox = fitz.Rect()  # starts empty/infinite
    for block in page.get_text("blocks"):
        bbox |= fitz.Rect(block[:4])
    for draw in page.get_drawings():
        r = draw.get("rect")
        if r:
            bbox |= fitz.Rect(r)
    # Some barcodes are embedded as images. Include their rectangles too.
    try:
        for img in page.get_images(full=True):
            xref = img[0]
            for r in page.get_image_rects(xref):
                bbox |= fitz.Rect(r)
    except Exception:
        pass
    if bbox.is_empty or bbox.is_infinite:
        return None
    w, h = page.rect.width, page.rect.height
    bottom_margin = max(18.0, margin * 3.0)
    return fitz.Rect(
        max(0.0, bbox.x0 - margin),
        max(0.0, bbox.y0 - margin),
        min(w, bbox.x1 + margin),
        min(h, bbox.y1 + bottom_margin),
    )


def _render_sos_clip_pixmap(src_page: fitz.Page, region: dict) -> tuple[fitz.Pixmap, fitz.Rect]:
    # Auto-detect the exact label bounds from the PDF's own vector content.
    # This is reliable regardless of any picker-drawn coordinates.
    clip = _auto_content_rect(src_page)
    if clip is None:
        # Fallback to the configured region if auto-detection finds nothing.
        clip = _region_to_rotated_rect(src_page, region)
    try:
        pix = src_page.get_pixmap(matrix=fitz.Matrix(2, 2), clip=clip, alpha=False)
        if _pixmap_nonwhite_ratio(pix) > 0.003:
            return pix, clip
    except Exception:
        pass
    # Last resort: full page.
    full = src_page.get_pixmap(matrix=fitz.Matrix(2, 2), alpha=False)
    return full, src_page.rect


def _fit_rect_contain(dst_w: float, dst_h: float, src_w: float, src_h: float) -> fitz.Rect:
    if src_w <= 0 or src_h <= 0:
        return fitz.Rect(0, 0, dst_w, dst_h)
    scale = min(dst_w / src_w, dst_h / src_h)
    w = src_w * scale
    h = src_h * scale
    x0 = (dst_w - w) / 2.0
    y0 = (dst_h - h) / 2.0
    return fitz.Rect(x0, y0, x0 + w, y0 + h)


def resize_thermal_label_pdf(pdf_bytes: bytes) -> bytes:
    """Crop a 4×6 thermal label off each page of an 8.5×11 PDF and re-pack it
    onto a proper 4×6 page (288×432 pt).

    Uses the same auto-content-detect approach as the Lowe's SOS tag resize so
    no manual coordinate configuration is needed — the label is auto-detected
    from its own vector content no matter where on the source page it appears.
    """
    src_doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    out_doc = fitz.open()

    for i in range(src_doc.page_count):
        src_page = src_doc.load_page(i)

        # Auto-detect bounds; fall back to full page if nothing is found.
        clip = _auto_content_rect(src_page)
        if clip is None:
            clip = src_page.rect

        # Render at 3× for sharp output at 4×6 physical size.
        pix = src_page.get_pixmap(matrix=fitz.Matrix(3, 3), clip=clip, alpha=False)

        page = out_doc.new_page(width=LABEL_OUTPUT_WIDTH_PT, height=LABEL_OUTPUT_HEIGHT_PT)
        img_rect = _fit_rect_contain(
            LABEL_OUTPUT_WIDTH_PT, LABEL_OUTPUT_HEIGHT_PT,
            float(pix.width), float(pix.height),
        )
        page.insert_image(img_rect, pixmap=pix)

    buf = BytesIO()
    out_doc.save(buf)
    out_doc.close()
    src_doc.close()
    return buf.getvalue()


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
                return {r: _merge_retailer_config(r, data.get(r)) for r in CROP_CONFIG_DEFAULTS}
    except Exception:
        pass
    return {r: _merge_retailer_config(r, None) for r in CROP_CONFIG_DEFAULTS}


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
    sos_page_w, sos_page_h = _sos_output_size_points_from_cfg(crop_cfg) if retailer == "Lowe's" else (0.0, 0.0)
    redact_regions = _redact_regions_from_cfg(crop_cfg) if retailer == "Tractor Supply" else []

    vendor_pdfs: dict[str, bytes] = {}
    for vendor, idxs in pages_by_vendor.items():
        out_doc = fitz.open()
        for i in idxs:
            row = row_by_page.get(i, {})
            is_sos_page = sos_crop is not None and bool(row.get("SOS Tag", False))

            if is_sos_page:
                src_page = src_doc.load_page(i)
                pix, clip_rect = _render_sos_clip_pixmap(src_page, sos_crop)
                page = out_doc.new_page(width=sos_page_w, height=sos_page_h)
                img_rect = _fit_rect_contain(sos_page_w, sos_page_h, float(pix.width), float(pix.height))
                page.insert_image(img_rect, pixmap=pix)
            else:
                out_doc.insert_pdf(src_doc, from_page=i, to_page=i)
                page = out_doc[-1]

            if redact_regions:
                for reg in redact_regions:
                    page.draw_rect(_region_to_rect(page, reg), color=(1, 1, 1), fill=(1, 1, 1), overlay=True)

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
    retailer: str,
    crop_cfg: dict,
) -> bytes | None:
    src_doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    pages_by_vendor: dict[str, list[int]] = defaultdict(list)
    row_by_page: dict[int, dict] = {}
    for r in page_vendor_rows:
        pages_by_vendor[r["Vendor"]].append(r["PageIndex"])
        row_by_page[r["PageIndex"]] = r

    target = [v for v in vendors if v in pages_by_vendor]
    if not target:
        src_doc.close()
        return None

    sos_crop = _sos_crop_region_from_cfg(crop_cfg) if retailer == "Lowe's" else None
    sos_page_w, sos_page_h = _sos_output_size_points_from_cfg(crop_cfg) if retailer == "Lowe's" else (0.0, 0.0)
    redact_regions = _redact_regions_from_cfg(crop_cfg) if retailer == "Tractor Supply" else []

    out_doc = fitz.open()
    for vendor in sorted(target, key=lambda x: x.lower()):
        for i in sorted(pages_by_vendor[vendor]):
            row = row_by_page.get(i, {})
            is_sos_page = sos_crop is not None and bool(row.get("SOS Tag", False))

            if is_sos_page:
                src_page = src_doc.load_page(i)
                pix, _ = _render_sos_clip_pixmap(src_page, sos_crop)
                page = out_doc.new_page(width=sos_page_w, height=sos_page_h)
                img_rect = _fit_rect_contain(sos_page_w, sos_page_h, float(pix.width), float(pix.height))
                page.insert_image(img_rect, pixmap=pix)
            else:
                out_doc.insert_pdf(src_doc, from_page=i, to_page=i)
                page = out_doc[-1]

            if redact_regions:
                for reg in redact_regions:
                    page.draw_rect(_region_to_rect(page, reg), color=(1, 1, 1), fill=(1, 1, 1), overlay=True)

    buf = BytesIO()
    out_doc.save(buf)
    out_doc.close()
    src_doc.close()
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
    today = datetime.date.today().isoformat()
    safe_retailer = re.sub(r"[^\w\-. ]+", " ", retailer).strip() or "Retailer"
    unmapped_dir = output_dir / "Unmapped Vendor Routes"

    # If this same source file name is re-run, remove previous routed outputs first
    # so stale vendor files are not left behind.
    _remove_existing_routed_files_for_base(base, retailer, routes, unmapped_dir, logger)

    for vendor, data in vendor_pdfs.items():
        safe_vendor = re.sub(r"[^\w\-. ]+", "_", vendor).strip() or "UNKNOWN"
        filename = f"{safe_retailer} {safe_vendor} {today} ORDER.pdf"

        route_dir = resolve_route_path(routes, retailer, vendor)
        if route_dir is None:
            route_dir = unmapped_dir
            logger.warning("[%s] No vendor route for '%s'; sent to %s", retailer, vendor, route_dir)

        route_dir.mkdir(parents=True, exist_ok=True)
        routed_file = route_dir / filename
        routed_file.write_bytes(data)

    # Copy vendor PDFs into the daily email staging folder so send_emails.py
    # can combine all retailers into one email per vendor at end of day.
    _stage_vendor_pdfs_for_email(vendor_pdfs, base, retailer, logger)

    # Also copy vendor PDFs into a single daily rollup folder grouped by vendor
    # across all retailers.
    _stage_vendor_pdfs_for_daily_rollup(vendor_pdfs, base, retailer, logger)


def _remove_existing_routed_files_for_base(
    base: str,
    retailer: str,
    routes: dict[tuple[str, str], Path],
    unmapped_dir: Path,
    logger: logging.Logger,
) -> None:
    pattern = f"{base} - *.pdf"
    candidate_dirs: set[Path] = {p for (r, _v), p in routes.items() if r == retailer}
    candidate_dirs.add(unmapped_dir)

    for d in candidate_dirs:
        if not d.exists():
            continue
        for fp in d.glob(pattern):
            try:
                fp.unlink()
            except OSError as e:
                logger.warning("[%s] Could not remove old routed file %s: %s", retailer, fp, e)


def _clear_directory_contents(dir_path: Path, logger: logging.Logger, label: str) -> None:
    if not dir_path.exists():
        dir_path.mkdir(parents=True, exist_ok=True)
        return

    for child in dir_path.iterdir():
        try:
            if child.is_dir():
                shutil.rmtree(child)
            else:
                child.unlink()
        except OSError as e:
            logger.warning("[%s] Could not clear %s item %s: %s", label, dir_path, child, e)


def _stage_vendor_pdfs_for_email(
    vendor_pdfs: dict[str, bytes],
    base: str,
    retailer: str,
    logger: logging.Logger,
) -> None:
    """Write vendor PDFs to the daily email staging folder.

    Layout:  email_staging/{YYYY-MM-DD}/{VendorName}/{base} - {vendor}.pdf
    The send_emails.py script reads this folder to build one email per vendor
    with all retailers' attachments combined.
    """
    today = datetime.date.today().isoformat()   # e.g. "2026-03-20"
    safe_retailer = re.sub(r"[^\w\-. ]+", " ", retailer).strip() or "Retailer"

    for vendor, data in vendor_pdfs.items():
        safe_vendor = re.sub(r"[^\w\-. ]+", "_", vendor).strip() or "UNKNOWN"
        vendor_dir = EMAIL_STAGING_ROOT / today / safe_vendor
        try:
            vendor_dir.mkdir(parents=True, exist_ok=True)
            filename = f"{safe_retailer} {safe_vendor} {today} ORDER.pdf"
            (vendor_dir / filename).write_bytes(data)
        except OSError as e:
            logger.warning("[%s] Could not stage email PDF for vendor '%s': %s", retailer, vendor, e)


def _stage_vendor_pdfs_for_daily_rollup(
    vendor_pdfs: dict[str, bytes],
    base: str,
    retailer: str,
    logger: logging.Logger,
) -> None:
    """Write vendor PDFs to one run-level rollup folder grouped by vendor.

    Layout:  {DAILY_VENDOR_ROLLUP_ROOT}/{VendorName}/{base} - {retailer} - {vendor}.pdf
    This lets each vendor folder contain that day's orders across all retailers.
    """
    _ensure_daily_rollup_current_day(logger)

    today = datetime.date.today().isoformat()
    safe_retailer = re.sub(r"[^\w\-. ]+", " ", retailer).strip() or "Retailer"

    for vendor, data in vendor_pdfs.items():
        safe_vendor = re.sub(r"[^\w\-. ]+", "_", vendor).strip() or "UNKNOWN"
        vendor_dir = DAILY_VENDOR_ROLLUP_ROOT / safe_vendor
        try:
            vendor_dir.mkdir(parents=True, exist_ok=True)
            filename = f"{safe_retailer} {safe_vendor} {today} ORDER.pdf"
            (vendor_dir / filename).write_bytes(data)
        except OSError as e:
            logger.warning("[%s] Could not write daily rollup PDF for vendor '%s': %s", retailer, vendor, e)


def _save_individual_label_backup(
    output_dir: Path,
    source_stem: str,
    label_data: bytes,
    logger: logging.Logger,
) -> None:
    """Write one resized label as its own timestamped PDF in an 'Individual Labels'
    subfolder under *output_dir*.  These files are never appended to — they are
    plain single-label PDFs that can be merged manually if the combined file
    ever has problems.
    """
    backup_dir = output_dir / "Individual Labels"
    try:
        backup_dir.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        logger.warning("[Labels] Could not create individual label backup dir: %s", e)
        return

    stamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:19]  # up to seconds
    filename = f"{source_stem}_{stamp}.pdf"
    try:
        (backup_dir / filename).write_bytes(label_data)
        logger.info("[Labels] Saved individual label → %s", backup_dir / filename)
    except OSError as e:
        logger.warning("[Labels] Could not save individual label '%s': %s", filename, e)


def _stage_label_for_daily_rollup(
    retailer: str,
    vendor: str,
    output_dir: Path,
    label_data: bytes,
    logger: logging.Logger,
) -> bool:
    """Append label page(s) into a single combined PDF for the vendor.

    Output file: {output_dir}/{safe_vendor} - Labels.pdf
    The first label dropped creates the file; every subsequent label's pages
    are appended to it.  Source files in LABEL_WATCH_ROOT are never touched.
    """
    safe_vendor = re.sub(r"[^\w\-. ]+", "_", vendor).strip() or "UNKNOWN"
    try:
        output_dir.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        logger.warning("[Labels] Could not create output dir for '%s': %s", vendor, e)
        return False

    safe_retailer = re.sub(r"[^\w\-. ]+", " ", retailer).strip() or "Retailer"
    today = datetime.date.today().isoformat()
    combined_name = f"{safe_retailer} {safe_vendor} {today} LABEL.pdf"
    combined_path = output_dir / combined_name
    try:
        if combined_path.exists():
            existing_doc = None
            new_doc = None
            try:
                # Open from file path and save incrementally to avoid full rewrites
                # on every append, which can become very slow and hold file locks.
                existing_doc = fitz.open(str(combined_path))
                new_doc = fitz.open(stream=label_data, filetype="pdf")
                existing_doc.insert_pdf(new_doc)
                try:
                    existing_doc.saveIncr()
                except Exception:
                    # Fallback when incremental save is unavailable.
                    buf = BytesIO()
                    existing_doc.save(buf)
                    combined_path.write_bytes(buf.getvalue())
            finally:
                if new_doc is not None:
                    new_doc.close()
                if existing_doc is not None:
                    existing_doc.close()
            logger.info("[Labels] Appended label page(s) to combined file → %s", combined_path)
        else:
            combined_path.write_bytes(label_data)
            logger.info("[Labels] Created combined label file → %s", combined_path)
        return True
    except Exception as e:
        logger.warning("[Labels] Could not write combined label for vendor '%s': %s", vendor, e)
        return False


def _ensure_daily_rollup_current_day(logger: logging.Logger) -> None:
    """Clear daily rollup once per calendar day before writing files.

    Uses a small persisted state file so same-day watcher restarts do not
    trigger another clear.
    """
    today = datetime.date.today().isoformat()

    last_cleared = ""
    try:
        if DAILY_ROLLUP_STATE_FILE.exists():
            last_cleared = DAILY_ROLLUP_STATE_FILE.read_text(encoding="utf-8").strip()
    except OSError as e:
        logger.warning("Could not read daily rollup state file %s: %s", DAILY_ROLLUP_STATE_FILE, e)

    if last_cleared == today:
        return

    _clear_directory_contents(DAILY_VENDOR_ROLLUP_ROOT, logger, "daily-rollup")
    try:
        DAILY_ROLLUP_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        DAILY_ROLLUP_STATE_FILE.write_text(today, encoding="utf-8")
    except OSError as e:
        logger.warning("Could not write daily rollup state file %s: %s", DAILY_ROLLUP_STATE_FILE, e)

    logger.info("Daily rollup reset for %s", today)


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


def _file_signature(path: Path) -> tuple[int, int, int] | None:
    """Return a file identity signature used for startup-baseline comparisons.

    Signature fields:
      1) mtime_ns
      2) size bytes
      3) ctime_ns

    This lets us ignore exact files that already existed at startup, while still
    processing files newly copied into the folder even if they carry an old
    modified time.
    """
    try:
        st = path.stat()
    except OSError:
        return None
    return (int(st.st_mtime_ns), int(st.st_size), int(st.st_ctime_ns))


def _file_stable_signature(path: Path) -> tuple[int, int] | None:
    """Return a stable dedupe signature (mtime_ns, size) for repeat-event filtering.

    We intentionally exclude ctime here because SMB/network shares can surface
    ctime/metadata jitter that causes false "changed" detections.
    """
    try:
        st = path.stat()
    except OSError:
        return None
    return (int(st.st_mtime_ns), int(st.st_size))


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

    # Keep output folder limited to the current run only.
    _clear_directory_contents(output_dir, logger, retailer)

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

        # Lowe's can vary layout; fall back to full-page text if region text is weak.
        if retailer == "Lowe's":
            if not scan_text or len(scan_text) < 8:
                scan_text = (full or "").strip()

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

        if retailer == "Lowe's" and vendor in ("UNKNOWN", "MIXED/REVIEW"):
            fallback_vendor, fallback_matched, fallback_conf = match_vendor((full or "").strip(), lookup)
            if fallback_vendor not in ("UNKNOWN", "MIXED/REVIEW"):
                vendor, matched, conf = fallback_vendor, fallback_matched, fallback_conf

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
    warehouse_pdf    = build_warehouse_print_pdf(pdf_bytes, page_vendor_rows, WAREHOUSE_VENDORS, retailer, crop_cfg)
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
        self._existing_pdf_signatures: dict[str, tuple[int, int, int]] = {}

    def ignore_existing_pdfs(self, input_dir: Path) -> None:
        """Record existing PDFs so only new/changed-after-start files process."""
        pending = sorted(input_dir.glob("*.pdf"), key=lambda p: p.name.lower())
        if not pending:
            self.logger.info("[%s] No existing PDF files found at startup in %s", self.retailer, input_dir)
            return

        recorded = 0
        for fp in pending:
            sig = _file_signature(fp)
            if sig is None:
                continue
            self._existing_pdf_signatures[str(fp).lower()] = sig
            recorded += 1

        self.logger.info(
            "[%s] Ignoring %d existing PDF file(s) at startup; only new/changed files will process",
            self.retailer,
            recorded,
        )

    def _process_if_pdf(self, path: Path, event_label: str) -> None:
        if path.suffix.lower() != ".pdf":
            return

        key = str(path).lower()
        current_sig = _file_signature(path)
        baseline_sig = self._existing_pdf_signatures.get(key)
        if baseline_sig is not None and current_sig == baseline_sig:
            self.logger.info("[%s] Ignoring existing startup file unchanged since watcher start: %s", self.retailer, path.name)
            return
        if baseline_sig is not None and current_sig != baseline_sig:
            self._existing_pdf_signatures.pop(key, None)

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


class DepotCSVHandler(FileSystemEventHandler):
    def __init__(
        self,
        rules_path: Path,
        output_dir: Path,
        archive_dir: Path,
        dry_run: bool,
        logger: logging.Logger,
    ) -> None:
        super().__init__()
        self.rules_path = _resolve_csv_rules_path(rules_path)
        self.output_dir = output_dir
        self.archive_dir = archive_dir
        self.dry_run = dry_run
        self.logger = logger
        self._last_seen: dict[str, float] = {}
        self._existing_csv_signatures: dict[str, tuple[int, int, int]] = {}
        self.rules: dict[str, depot_csv.SkuRule] = {}
        self._poll_interval_sec = 5.0
        self._next_poll_at = 0.0
        self._next_poll_log_at = 0.0

        self._load_rules()

    def _load_rules(self) -> None:
        self.logger.info("[Depot CSV] Resolving rules workbook at %s", self.rules_path)
        self.logger.info("[Depot CSV] Rules workbook exists: %s", self.rules_path.exists())
        try:
            loaded_rules = depot_csv.load_sku_rules(self.rules_path)
            self.rules = loaded_rules
            self.logger.info("[Depot CSV] Loaded %d SKU rule(s) from %s", len(self.rules), self.rules_path)
        except Exception as e:
            if self.rules:
                self.logger.error(
                    "[Depot CSV] Could not reload SKU rules from %s: %s (keeping %d previously loaded rule(s))",
                    self.rules_path,
                    e,
                    len(self.rules),
                )
            else:
                self.rules = {}
                self.logger.error("[Depot CSV] Could not load SKU rules from %s: %s", self.rules_path, e)

    def ignore_existing_csvs(self, input_dir: Path) -> None:
        """Record existing CSVs so only new/changed-after-start files process."""
        pending = sorted(input_dir.glob("*.csv"), key=lambda p: p.name.lower())
        if not pending:
            self.logger.info("[Depot CSV] No existing CSV files found at startup in %s", input_dir)
            return

        recorded = 0
        for fp in pending:
            sig = _file_signature(fp)
            if sig is None:
                continue
            self._existing_csv_signatures[str(fp).lower()] = sig
            recorded += 1

        self.logger.info(
            "[Depot CSV] Ignoring %d existing CSV file(s) at startup; only new/changed files will process",
            recorded,
        )

    def _process_if_csv(self, path: Path, event_label: str) -> None:
        if path.suffix.lower() != ".csv":
            return

        key = str(path).lower()
        current_sig = _file_signature(path)
        baseline_sig = self._existing_csv_signatures.get(key)
        if baseline_sig is not None and current_sig == baseline_sig:
            self.logger.info("[Depot CSV] Ignoring existing startup file unchanged since watcher start: %s", path.name)
            return
        if baseline_sig is not None and current_sig != baseline_sig:
            self._existing_csv_signatures.pop(key, None)

        now = time.monotonic()
        if now - self._last_seen.get(key, 0.0) < 10.0:
            return
        self._last_seen[key] = now

        # Always refresh rules before processing so same-day workbook edits
        # (new SKUs/vendors/printers) are picked up without restarting watcher.
        self._load_rules()
        if not self.rules:
            self.logger.error("[Depot CSV] Rules are unavailable; skipping %s", path.name)
            return

        self.logger.info("[Depot CSV] Detected %s file: %s", event_label, path.name)

        if not _wait_for_file_ready(path):
            self.logger.error("[Depot CSV] Timed out waiting for %s to finish writing — skipping.", path.name)
            return

        try:
            out_path, out_rows, unknown_skus, archived_to = depot_csv.process_one_csv(
                raw_csv=path,
                rules=self.rules,
                output_dir=self.output_dir,
                archive_dir=self.archive_dir,
                dry_run=self.dry_run,
            )
            if self.dry_run:
                self.logger.info("[Depot CSV] DRY RUN for %s → would create %d row(s)", path.name, out_rows)
            else:
                self.logger.info("[Depot CSV] Processed %s -> %s (%d rows)", path.name, out_path, out_rows)
                self.logger.info("[Depot CSV] Archived copy -> %s", archived_to)

            if unknown_skus:
                self.logger.warning("[Depot CSV] %d row(s) had unknown SKU and were skipped", unknown_skus)
        except Exception as e:
            self.logger.exception("[Depot CSV] Unhandled error processing %s: %s", path.name, e)

    def poll_input_dir(self, input_dir: Path) -> None:
        """Fallback polling for network shares where file events can be missed."""
        now = time.monotonic()
        if now < self._next_poll_at:
            return
        self._next_poll_at = now + self._poll_interval_sec

        try:
            pending = sorted(input_dir.glob("*.csv"), key=lambda p: p.name.lower())
        except Exception as e:
            self.logger.error("[Depot CSV] Poll failed for %s: %s", input_dir, e)
            return

        if now >= self._next_poll_log_at:
            self._next_poll_log_at = now + 60.0
            self.logger.info(
                "[Depot CSV] Poll heartbeat: %d CSV file(s) visible in %s (rules loaded: %d)",
                len(pending),
                input_dir,
                len(self.rules),
            )

        for fp in pending:
            self._process_if_csv(fp, "polled")

    def on_created(self, event) -> None:  # type: ignore[override]
        if event.is_directory:
            return
        self._process_if_csv(Path(event.src_path), "new")

    def on_moved(self, event) -> None:  # type: ignore[override]
        if event.is_directory:
            return
        self._process_if_csv(Path(event.dest_path), "moved")

    def on_modified(self, event) -> None:  # type: ignore[override]
        if event.is_directory:
            return
        self._process_if_csv(Path(event.src_path), "modified")


# ─────────────────────────────────────────────────────────────────────────────
# WorldShip label handler
# ─────────────────────────────────────────────────────────────────────────────

class LabelHandler(FileSystemEventHandler):
    """Watch configured vendor label input folders for WorldShip label PDFs.

    Behaviour is driven by a list of LabelVendorRoute entries loaded from
    'Label Vendor Routes.xlsx'.  Each entry defines:
      - input_path  : folder to watch for incoming label PDFs
      - output_path : folder where the combined {Vendor} - Labels.pdf is written
      - resize      : True → crop 8.5×11 page down to 4×6 thermal size
                      False → pass through at original page size

    When no routes config is found, falls back to watching LABEL_WATCH_ROOT
    recursively and deriving the vendor name from the subfolder name.
    """

    def __init__(self, routes: list[LabelVendorRoute], logger: logging.Logger) -> None:
        super().__init__()
        self.routes = routes
        self.logger = logger
        self._last_seen: dict[str, float] = {}
        self._existing_label_signatures: dict[str, tuple[int, int, int]] = {}
        self._processed_label_signatures: dict[str, tuple[int, int]] = {}
        self._processed_label_hashes_by_output: dict[str, set[str]] = {}
        self._in_progress: set[str] = set()  # keys currently being processed
        self._lock = threading.Lock()  # guards all shared state
        self._poll_interval_sec = 5.0
        self._next_poll_at = 0.0
        self._next_poll_log_at = 0.0
        # Build fast lookup: normalised input_path string → route
        self._route_by_input: dict[str, LabelVendorRoute] = {
            str(r.input_path).lower(): r for r in routes
        }

    def _route_for_path(self, path: Path) -> LabelVendorRoute | None:
        """Return the route whose input_path matches the file's parent directory."""
        return self._route_by_input.get(str(path.parent).lower())

    def _vendor_fallback(self, path: Path) -> str:
        """Derive vendor name from the immediate parent folder (fallback only)."""
        name = path.parent.name.strip()
        return name if name else "UNKNOWN"

    def _retailer_from_input_path(self, input_dir: Path) -> str:
        """Derive retailer name from input path segment like '2-Home Depot'."""
        for part in input_dir.parts:
            text = str(part).strip()
            m = re.match(r"^\d+\s*-\s*(.+)$", text)
            if m:
                return m.group(1).strip() or "Retailer"
        return "Retailer"

    def ignore_existing_labels(self) -> None:
        """Snapshot all existing label PDFs so only newly-added files process."""
        recorded = 0
        if self.routes:
            paths_to_scan = [r.input_path for r in self.routes]
        else:
            paths_to_scan = [LABEL_WATCH_ROOT]

        for scan_root in paths_to_scan:
            try:
                for fp in scan_root.rglob("*.pdf"):
                    sig = _file_signature(fp)
                    if sig is None:
                        continue
                    self._existing_label_signatures[str(fp).lower()] = sig
                    recorded += 1
            except Exception:
                continue

        if recorded:
            self.logger.info("[Labels] Ignoring %d existing label PDF(s) at startup", recorded)

    def _process_if_label(self, path: Path, event_label: str) -> None:
        if path.suffix.lower() != ".pdf":
            return

        key = str(path).lower()

        # ── Gate check (thread-safe) ───────────────────────────────────────
        # The Observer thread and the main-loop polling thread can both call
        # this method concurrently for the same file.  All shared-state reads
        # and the _in_progress mark must be atomic.
        with self._lock:
            current_sig = _file_signature(path)
            if current_sig is None:
                return

            current_stable_sig = _file_stable_signature(path)
            if current_stable_sig is None:
                return

            baseline_sig = self._existing_label_signatures.get(key)
            if baseline_sig is not None and current_sig == baseline_sig:
                return  # unchanged since startup — skip silently
            if baseline_sig is not None and current_sig != baseline_sig:
                self._existing_label_signatures.pop(key, None)

            # Already successfully processed and file hasn't changed.
            if self._processed_label_signatures.get(key) == current_stable_sig:
                return

            # Already being processed right now by another thread.
            if key in self._in_progress:
                return

            now = time.monotonic()
            if now - self._last_seen.get(key, 0.0) < 30.0:
                return
            self._last_seen[key] = now

            # Claim this key — any concurrent call will see it and bail out.
            self._in_progress.add(key)

        # ── Long-running work (outside the lock so other files aren't blocked) ──
        try:
            if not _wait_for_file_ready(path):
                self.logger.error("[Labels] Timed out waiting for %s to finish writing — skipping.", path.name)
                return

            route = self._route_for_path(path)
            if route:
                retailer = route.retailer.strip() or self._retailer_from_input_path(route.input_path)
                vendor = route.vendor
                output_dir = route.output_path
                resize = route.resize
            else:
                # Fallback: no matching config row — use folder name, rollup root, resize
                retailer = self._retailer_from_input_path(path.parent)
                vendor = self._vendor_fallback(path)
                safe_vendor = re.sub(r"[^\w\-. ]+", "_", vendor).strip() or "UNKNOWN"
                output_dir = DAILY_VENDOR_ROLLUP_ROOT / safe_vendor
                resize = True

            self.logger.info(
                "[Labels] Detected %s label for vendor '%s': %s%s",
                event_label, vendor, path.name,
                " (8×11 passthrough — no resize)" if not resize else " (resizing to 4×6)",
            )

            try:
                pdf_bytes = path.read_bytes()
            except OSError as e:
                self.logger.error("[Labels] Cannot read %s: %s", path.name, e)
                return

            if resize:
                try:
                    output_bytes = resize_thermal_label_pdf(pdf_bytes)
                except Exception as e:
                    self.logger.exception("[Labels] Error resizing %s: %s", path.name, e)
                    return
            else:
                output_bytes = pdf_bytes

            # Save a resized individual-label backup before attempting the merge.
            # This gives a clean per-label file to manually combine if the merged
            # file ever has duplicate or corruption issues.
            _save_individual_label_backup(output_dir, path.stem, output_bytes, self.logger)

            safe_vendor = re.sub(r"[^\w\-. ]+", "_", vendor).strip() or "UNKNOWN"
            safe_retailer = re.sub(r"[^\w\-. ]+", " ", retailer).strip() or "Retailer"
            today = datetime.date.today().isoformat()
            combined_name = f"{safe_retailer} {safe_vendor} {today} LABEL.pdf"
            output_key = str(output_dir / combined_name).lower()

            content_hash = hashlib.sha1(output_bytes).hexdigest()
            with self._lock:
                seen_hashes = self._processed_label_hashes_by_output.setdefault(output_key, set())
                if content_hash in seen_hashes:
                    self.logger.info("[Labels] Duplicate label content skipped for vendor '%s': %s", vendor, path.name)
                    self._processed_label_signatures[key] = current_stable_sig
                    return

            staged_ok = _stage_label_for_daily_rollup(retailer, vendor, output_dir, output_bytes, self.logger)
            with self._lock:
                if staged_ok:
                    seen_hashes = self._processed_label_hashes_by_output.setdefault(output_key, set())
                    seen_hashes.add(content_hash)
                    self._processed_label_signatures[key] = current_stable_sig
        finally:
            # Always release the in-progress claim so future runs can re-try.
            with self._lock:
                self._in_progress.discard(key)

    def poll_all_inputs(self) -> None:
        """Polling fallback for network shares where file-system events can be missed."""
        now = time.monotonic()
        if now < self._next_poll_at:
            return
        self._next_poll_at = now + self._poll_interval_sec

        if self.routes:
            pending: list[Path] = []
            for r in self.routes:
                try:
                    pending.extend(r.input_path.glob("*.pdf"))
                except Exception as e:
                    self.logger.error("[Labels] Poll failed for %s: %s", r.input_path, e)
        else:
            try:
                pending = list(LABEL_WATCH_ROOT.rglob("*.pdf"))
            except Exception as e:
                self.logger.error("[Labels] Poll failed for %s: %s", LABEL_WATCH_ROOT, e)
                return

        if now >= self._next_poll_log_at:
            self._next_poll_log_at = now + 60.0
            self.logger.info("[Labels] Poll heartbeat: %d label PDF(s) across all vendor input folders", len(pending))

        for fp in pending:
            self._process_if_label(fp, "polled")

    def on_created(self, event) -> None:  # type: ignore[override]
        if event.is_directory:
            return
        self._process_if_label(Path(event.src_path), "new")

    def on_moved(self, event) -> None:  # type: ignore[override]
        if event.is_directory:
            return
        self._process_if_label(Path(event.dest_path), "moved")

    def on_modified(self, event) -> None:  # type: ignore[override]
        if event.is_directory:
            return
        self._process_if_label(Path(event.src_path), "modified")


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

    unc_paths = [
        *WATCH_DIRS.values(),
        *OUTPUT_DIRS.values(),
        *route_dirs,
        CSV_INPUT_DIR,
        CSV_OUTPUT_DIR,
        CSV_ARCHIVE_DIR,
        LABEL_WATCH_ROOT,
    ]

    if os.name != "nt" and any(str(p).startswith("\\\\") for p in unc_paths):
        logger.error("UNC paths were configured, but this host is not Windows.")
        logger.error("Run watcher.py on a Windows machine that can access the network share.")
        return

    # Ensure all directories exist
    for d in [*WATCH_DIRS.values(), *OUTPUT_DIRS.values(), *route_dirs, CSV_INPUT_DIR, CSV_OUTPUT_DIR, CSV_ARCHIVE_DIR, LABEL_WATCH_ROOT]:
        d.mkdir(parents=True, exist_ok=True)

    # Reset the daily vendor rollup at startup and then once per day if the
    # watcher remains running across midnight.
    _ensure_daily_rollup_current_day(logger)

    crop_cfg = load_crop_config()

    observer = Observer()

    if not PDF_WATCH_ENABLED and not CSV_WATCH_ENABLED and not LABEL_WATCH_ENABLED:
        logger.error("All PDF, CSV, and Label watchers are disabled by environment settings.")
        return

    if PDF_WATCH_ENABLED:
        for retailer, watch_dir in WATCH_DIRS.items():
            handler = PDFHandler(retailer, crop_cfg, OUTPUT_DIRS[retailer], routes, logger)
            handler.ignore_existing_pdfs(watch_dir)
            observer.schedule(handler, str(watch_dir), recursive=False)
            logger.info("Watching [%-14s] → %s/", retailer, watch_dir)
            logger.info("Output   [%-14s] → %s/", retailer, OUTPUT_DIRS[retailer])
    else:
        logger.info("PDF watcher disabled by ORDER_SPLITTER_DISABLE_PDF_WATCH")

    csv_handler: DepotCSVHandler | None = None
    if CSV_WATCH_ENABLED:
        csv_handler = DepotCSVHandler(
            rules_path=CSV_RULES_XLSX_PATH,
            output_dir=CSV_OUTPUT_DIR,
            archive_dir=CSV_ARCHIVE_DIR,
            dry_run=CSV_DRY_RUN,
            logger=logger,
        )
        logger.info("[Depot CSV] Input folder exists: %s", CSV_INPUT_DIR.exists())
        logger.info("[Depot CSV] Output folder exists: %s", CSV_OUTPUT_DIR.exists())
        logger.info("[Depot CSV] Archive folder exists: %s", CSV_ARCHIVE_DIR.exists())
        csv_handler.ignore_existing_csvs(CSV_INPUT_DIR)
        observer.schedule(csv_handler, str(CSV_INPUT_DIR), recursive=False)
        logger.info("Watching [Depot CSV      ] → %s/", CSV_INPUT_DIR)
        logger.info("CSV output              → %s/", CSV_OUTPUT_DIR)
        logger.info("CSV archive             → %s/", CSV_ARCHIVE_DIR)
        logger.info("CSV rules file          → %s", csv_handler.rules_path)
        logger.info("CSV dry-run mode        → %s", CSV_DRY_RUN)
    else:
        logger.info("CSV watcher disabled by ORDER_SPLITTER_DISABLE_CSV_WATCH")

    label_handler: LabelHandler | None = None
    if PDF_WATCH_ENABLED and LABEL_WATCH_ENABLED:
        label_routes = load_label_vendor_routes(LABEL_ROUTES_XLSX_PATH, logger)
        label_handler = LabelHandler(label_routes, logger)
        label_handler.ignore_existing_labels()
        if label_routes:
            # Watch each configured input path individually.
            watched_label_dirs: set[str] = set()
            for route in label_routes:
                route.input_path.mkdir(parents=True, exist_ok=True)
                route.output_path.mkdir(parents=True, exist_ok=True)
                dir_key = str(route.input_path)
                if dir_key not in watched_label_dirs:
                    observer.schedule(label_handler, dir_key, recursive=False)
                    watched_label_dirs.add(dir_key)
                logger.info(
                    "Watching [Labels %-8s] → %s  (%s)",
                    route.vendor[:8], route.input_path,
                    "4×6 resize" if route.resize else "8×11 passthrough",
                )
                logger.info("  Label output          → %s", route.output_path)
        else:
            # No config found — fall back to recursive watch on LABEL_WATCH_ROOT.
            LABEL_WATCH_ROOT.mkdir(parents=True, exist_ok=True)
            observer.schedule(label_handler, str(LABEL_WATCH_ROOT), recursive=True)
            logger.info("Watching [Labels        ] → %s/ (recursive, no routes config)", LABEL_WATCH_ROOT)
    elif not PDF_WATCH_ENABLED:
        logger.info("Label watcher skipped (requires PDF watcher to be enabled)")
    else:
        logger.info("Label watcher disabled by ORDER_SPLITTER_DISABLE_LABEL_WATCH")

    logger.info("Daily rollup output     → %s/", DAILY_VENDOR_ROLLUP_ROOT)

    logger.info("Watcher running. Drop PDF/CSV files into configured watch folders. Press Ctrl+C to stop.\n")

    observer.start()
    try:
        while True:
            if csv_handler is not None:
                csv_handler.poll_input_dir(CSV_INPUT_DIR)
            if label_handler is not None:
                label_handler.poll_all_inputs()
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
        logger.info("Watcher stopped.")
    observer.join()


if __name__ == "__main__":
    main()
