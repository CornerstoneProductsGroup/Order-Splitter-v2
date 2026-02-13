import streamlit as st
import pandas as pd
from io import BytesIO
import zipfile
import re
import json
from collections import defaultdict

import fitz  # PyMuPDF
from PIL import Image, ImageDraw

from pypdf import PdfReader, PdfWriter


# -----------------------------
# Defaults (packaged in repo)
# -----------------------------
# Vendors we ship from our own warehouse (used to create a combined print file)
WAREHOUSE_VENDORS = [
    "Cord Mate",
    "Gate Latch",
    "Home Selects",
    "Nisus",
    "Post Protector-Here",
    "Soft Seal",
    "Weedshark",
    "Zaca",
]

DEFAULT_MAPS = {
    "Home Depot": "vendor_map_hd.xlsx",
    "Lowe's": "vendor_map_lowes.xlsx",
    "Tractor Supply": "vendor_map_tsc.xlsx",
}

# Column names differ slightly by retailer
MAP_KEY_COL = {
    "Home Depot": "Model Number",
    "Lowe's": "SKU",
    "Tractor Supply": "SKU",
}
MAP_VENDOR_COL = "Vendor"

# Text-extraction crop regions (fractions of page height).
# Coordinates in PDF space are from the bottom (0.0) to top (1.0).
# These defaults remove header/footer areas where order/customer numbers often appear.
CROP_CONFIG_PATH = "crop_config.json"

CROP_CONFIG = {
    # NOTE: These regions are tuned to the packing slip "line items" / model-number area for each retailer.
    # Fractions are relative to page height (0.0 = bottom, 1.0 = top).
    # If a retailer changes their slip layout in the future, these are the only numbers you should need to adjust.

    # Home Depot: the Model Number / Internet Number table sits in the lower half of the slip (above the footer).
    "Home Depot": {"y0": 0.05, "y1": 0.60},

    # Lowe's: the Item / Model # line-items table is mid-lower; headers contain long customer order numbers.
    # SOS tags are detected from FULL text, but matching uses this cropped region.
    "Lowe's": {"y0": 0.25, "y1": 0.70},

    # Tractor Supply: the LINE/SKU/VENDOR PN table is mid-lower.
    "Tractor Supply": {"y0": 0.25, "y1": 0.75},
}

def load_crop_config() -> dict:
    """Load persisted crop config if available, else return defaults."""
    try:
        import os, json
        if os.path.exists(CROP_CONFIG_PATH):
            with open(CROP_CONFIG_PATH, "r", encoding="utf-8") as f:
                data = json.load(f)
            # basic validation
            if isinstance(data, dict):
                return data
    except Exception:
        pass
    return CROP_CONFIG


def save_crop_config(cfg: dict) -> bool:
    """Persist crop config to a json file. Note: on Streamlit Community Cloud this may reset after sleep."""
    try:
        import json
        with open(CROP_CONFIG_PATH, "w", encoding="utf-8") as f:
            json.dump(cfg, f, indent=2)
        return True
    except Exception:
        return False


def render_scan_area_overlay(pdf_bytes: bytes, retailer: str, page_index: int, y0_frac: float, y1_frac: float, zoom: float = 2.0) -> bytes:
    """
    Render a PDF page to PNG and draw the scan area rectangle.
    Fractions are in PDF coordinates (0=bottom, 1=top).
    Returns PNG bytes.
    """
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    page_index = max(0, min(page_index, doc.page_count - 1))
    page = doc.load_page(page_index)
    mat = fitz.Matrix(zoom, zoom)
    pix = page.get_pixmap(matrix=mat)
    img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)

    draw = ImageDraw.Draw(img)

    # fitz coordinates origin top-left; our fractions are bottom-based
    h = page.rect.height
    w = page.rect.width

    # Convert to top-left coordinate system in points, then scale by zoom
    top_y = (1 - y1_frac) * h
    bot_y = (1 - y0_frac) * h

    x0 = 0
    x1 = w

    # scale
    x0 *= zoom; x1 *= zoom
    top_y *= zoom; bot_y *= zoom

    # draw rectangle
    draw.rectangle([x0, top_y, x1, bot_y], outline="red", width=6)

    import io
    buff = io.BytesIO()
    img.save(buff, format="PNG")
    return buff.getvalue()


# -----------------------------
# Helpers
# -----------------------------
def normalize_key(x: str) -> str:
    """Normalize SKU/Model strings for matching."""
    if x is None:
        return ""
    s = str(x).strip().upper()
    # remove spaces and common separators
    s = re.sub(r"[\s\-_]", "", s)
    return s


def load_vendor_map(retailer: str, uploaded_file=None) -> pd.DataFrame:
    """
    Load a vendor map for the retailer.
    Priority:
      1) Uploaded file (if provided)
      2) Packaged default file in repo
    """
    if uploaded_file is not None:
        df = pd.read_excel(uploaded_file)
        return df

    default_path = DEFAULT_MAPS[retailer]
    df = pd.read_excel(default_path)
    return df


def build_lookup(df: pd.DataFrame, retailer: str) -> dict:
    """Build dict: normalized SKU/Model -> Vendor"""
    key_col = MAP_KEY_COL[retailer]
    if key_col not in df.columns or MAP_VENDOR_COL not in df.columns:
        raise ValueError(
            f"Vendor map for {retailer} must include columns: '{key_col}' and '{MAP_VENDOR_COL}'. "
            f"Found: {list(df.columns)}"
        )

    lookup = {}
    for _, row in df.iterrows():
        k = normalize_key(row.get(key_col))
        v = str(row.get(MAP_VENDOR_COL)).strip() if pd.notna(row.get(MAP_VENDOR_COL)) else ""
        if k and v:
            lookup[k] = v
    return lookup


def extract_text_by_page_with_regions(pdf_bytes: bytes, retailer: str, crop_cfg: dict) -> list[dict]:
    """
    Return a list of dicts per page:
      {"full": <full page text>, "region": <cropped region text>}
    Region cropping is used for vendor matching to avoid false positives from headers/footers.
    """
    from copy import copy
    from pypdf.generic import RectangleObject

    cfg = crop_cfg.get(retailer, {"y0": 0.0, "y1": 1.0})
    y0_frac = float(cfg.get("y0", 0.0))
    y1_frac = float(cfg.get("y1", 1.0))

    reader = PdfReader(BytesIO(pdf_bytes))
    out = []

    for page in reader.pages:
        # Full text (used for SOS detection and general fallback)
        try:
            full_text = page.extract_text() or ""
        except Exception:
            full_text = ""

        # Region text (used for SKU/Model matching)
        try:
            # Copy the page object so we don't mutate the original
            p2 = copy(page)

            # Determine page bounds
            mb = page.mediabox
            x0 = float(mb.left)
            x1 = float(mb.right)
            h = float(mb.top) - float(mb.bottom)

            ry0 = float(mb.bottom) + y0_frac * h
            ry1 = float(mb.bottom) + y1_frac * h

            # Clamp
            ry0 = max(float(mb.bottom), min(ry0, float(mb.top)))
            ry1 = max(float(mb.bottom), min(ry1, float(mb.top)))
            if ry1 <= ry0:
                ry0, ry1 = float(mb.bottom), float(mb.top)

            p2.cropbox = RectangleObject([x0, ry0, x1, ry1])

            region_text = p2.extract_text() or ""
        except Exception:
            # Fallback if crop-based extraction fails
            region_text = full_text

        out.append({"full": full_text, "region": region_text})

    return out

def extract_search_region(text: str, retailer: str) -> str:
    """
    Limit the text region used for SKU/Model matching to reduce false positives.

    Home Depot: Only scan the items table area (below the 'Model Number' header),
    stopping before the footer (e.g., 'Page:' / thank-you line).
    Other retailers: scan full page text.
    """
    if retailer != "Home Depot":
        return text or ""

    if not text:
        return ""

    lines = [ln.strip() for ln in (text.splitlines() or [])]

    header_idx = None
    for i, ln in enumerate(lines):
        up = ln.upper()
        if "MODEL NUMBER" in up and ("INTERNET NUMBER" in up or "ITEM DESCRIPTION" in up):
            header_idx = i
            break

    if header_idx is None:
        return text  # fallback

    start = header_idx + 1

    end = len(lines)
    for j in range(start, len(lines)):
        up = lines[j].upper()
        if up.startswith("PAGE:") or "THANK YOU FOR SHOPPING AT THE HOME DEPOT" in up:
            end = j
            break

    region = "\n".join(lines[start:end]).strip()
    return region if region else text





def is_sos_tag_page(text: str) -> bool:
    """
    Heuristic detector for Lowe's SOS (Ship-to-Store) tag/label pages.
    These pages typically follow the order page and should inherit the prior page's vendor.
    """
    t_raw = (text or "").upper()
    # Common signals. Adjust/add as you encounter variants in real PDFs.
    keywords = [
        "SOS",               # SOS tag
        "SHIP TO STORE",
        "STORE PICKUP",
        "PICK UP IN STORE",
        "S2S",               # sometimes used shorthand
        "SPECIAL ORDER",     # sometimes appears near SOS wording
    ]
    return any(k in t_raw for k in keywords)

def match_vendor(text: str, lookup: dict, retailer: str) -> tuple[str, list[str], int]:
    """
    Find SKUs/Models present in page text.
    Returns:
      (vendor, matched_keys, confidence_percent)

    Confidence is a heuristic:
      - UNKNOWN: 0%
      - MIXED/REVIEW: 25%
      - Single-vendor pages: confidence rises with number of SKU/Model hits
    """
        # Normalize the cropped region text (header/footer removed)
    t = normalize_key(text)

    matched = []
    vendors = set()

    # Fast-ish approach: check each key as substring of normalized page text
    for k, vendor in lookup.items():
        if k and k in t:
            matched.append(k)
            vendors.add(vendor)

    if not vendors:
        return "UNKNOWN", [], 0

    if len(vendors) > 1:
        return "MIXED/REVIEW", matched[:15], 25

    hit_count = len(set(matched))

    # Confidence calibration:
    # Many order pages only contain 1 clear SKU/Model hit; bias upward while keeping MIXED/UNKNOWN low.
    if hit_count >= 5:
        conf = 98
    elif hit_count == 4:
        conf = 95
    elif hit_count == 3:
        conf = 92
    elif hit_count == 2:
        conf = 88
    elif hit_count == 1:
        conf = 80
    else:
        conf = 60

    return next(iter(vendors)), matched[:15], conf


def build_vendor_pdfs(pdf_bytes: bytes, page_vendor_rows: list[dict]) -> dict[str, bytes]:
    """
    Rebuild each vendor PDF from the original PDF using the page->vendor assignments.
    This avoids 'overwrite' bugs and guarantees each vendor contains all its pages.
    """
    reader = PdfReader(BytesIO(pdf_bytes))

    pages_by_vendor = defaultdict(list)
    for r in page_vendor_rows:
        pages_by_vendor[r["Vendor"]].append(r["PageIndex"])

    vendor_pdfs = {}
    for vendor, idxs in pages_by_vendor.items():
        writer = PdfWriter()
        for i in idxs:
            writer.add_page(reader.pages[i])
        buff = BytesIO()
        writer.write(buff)
        vendor_pdfs[vendor] = buff.getvalue()

    return vendor_pdfs

def build_warehouse_print_pdf(pdf_bytes: bytes, page_vendor_rows: list[dict], vendors: list[str]) -> bytes | None:
    """
    Build a single PDF that concatenates pages for a set of vendors (e.g., warehouse-shipped items).
    Ordering:
      - Vendors in alphabetical order (case-insensitive)
      - Pages within each vendor in ascending page order
    Returns bytes for the combined PDF, or None if no pages matched.
    """
    reader = PdfReader(BytesIO(pdf_bytes))

    pages_by_vendor = defaultdict(list)
    for r in page_vendor_rows:
        pages_by_vendor[r["Vendor"]].append(r["PageIndex"])

    target = [v for v in vendors if v in pages_by_vendor]
    if not target:
        return None

    writer = PdfWriter()
    for vendor in sorted(target, key=lambda x: x.lower()):
        for i in sorted(pages_by_vendor[vendor]):
            writer.add_page(reader.pages[i])

    buff = BytesIO()
    writer.write(buff)
    return buff.getvalue()


def build_zip(
    vendor_pdfs: dict[str, bytes],
    retailer: str,
    base_name: str,
    warehouse_print_pdf: bytes | None = None,
) -> bytes:
    """
    Build a ZIP containing one PDF per vendor.

    Filenames:
      <base_name> - <Vendor>.pdf

    If warehouse_print_pdf is provided, also includes:
      <base_name> - WAREHOUSE PRINT.pdf
    """
    buff = BytesIO()
    base = re.sub(r"\.pdf$", "", base_name, flags=re.IGNORECASE).strip()
    base = re.sub(r"[\\/:*?\"<>|]+", "_", base).strip() or retailer.replace(" ", "")

    with zipfile.ZipFile(buff, "w", compression=zipfile.ZIP_DEFLATED) as z:
        if warehouse_print_pdf is not None:
            z.writestr(f"{base} - WAREHOUSE PRINT.pdf", warehouse_print_pdf)

        for vendor, pdf_data in vendor_pdfs.items():
            safe_vendor = re.sub(r"[^\w\-\. ]+", "_", vendor).strip() or "UNKNOWN"
            filename = f"{base} - {safe_vendor}.pdf"
            z.writestr(filename, pdf_data)
    return buff.getvalue()


# -----------------------------
# UI
# -----------------------------
# UI
# -----------------------------
st.set_page_config(page_title="Retail Order Splitter (Basic)", layout="wide")
st.title("Retail Order Splitter (Basic)")

st.caption(
    "Upload a PDF. The app scans each page for SKUs/Models from the vendor map, assigns a vendor, "
    "then builds a ZIP with one PDF per vendor containing *all* pages for that vendor. "
    "Lowe's SOS tag pages inherit the vendor from the prior page."
)

retailer = st.selectbox("Retailer", ["Home Depot", "Lowe's", "Tractor Supply"], index=1)

# Load persisted crop config once per session
if "crop_cfg" not in st.session_state:
    st.session_state["crop_cfg"] = load_crop_config()

confidence_threshold = st.slider(
    "Confidence threshold (pages below this will be flagged as REVIEW)",
    min_value=0,
    max_value=100,
    value=70,
    step=5,
    help="If a page's confidence is below the threshold, it will be labeled REVIEW instead of being assigned to a vendor."
)

with st.expander("Scan area tuning (advanced)"):
    st.caption(
        "Use this to visualize and adjust the scan box used for SKU/Model matching. "
        "Settings are per retailer. Click **Show scan area** to preview the red box on a selected page."
    )

    crop_cfg = st.session_state.get("crop_cfg", load_crop_config())
    cur = crop_cfg.get(retailer, {"y0": 0.0, "y1": 1.0})

    y0 = st.slider("Bottom (y0) fraction", 0.0, 1.0, float(cur.get("y0", 0.0)), 0.01)
    y1 = st.slider("Top (y1) fraction", 0.0, 1.0, float(cur.get("y1", 1.0)), 0.01)
    if y1 < y0:
        st.warning("Top (y1) is below bottom (y0). The app will swap them internally, but you should fix it here.")

    page_preview = st.number_input("Preview page number", min_value=1, value=1, step=1)

    cA, cB, cC = st.columns([1,1,1])
    with cA:
        show_box = st.button("Show scan area", key=f"show_scan_{retailer}")
    with cB:
        save_defaults = st.button("Save as default", key=f"save_scan_{retailer}")
    with cC:
        st.download_button(
            "Download config JSON",
            data=json.dumps(crop_cfg, indent=2).encode("utf-8"),
            file_name="crop_config.json",
            mime="application/json",
            key=f"dl_cfg_{retailer}"
        )

    cfg_upload = st.file_uploader("Upload config JSON (optional)", type=["json"], key=f"cfg_up_{retailer}")
    if cfg_upload is not None:
        try:
            uploaded_cfg = json.load(cfg_upload)
            if isinstance(uploaded_cfg, dict):
                st.session_state["crop_cfg"] = uploaded_cfg
                st.success("Loaded config JSON into this session.")
                crop_cfg = uploaded_cfg
        except Exception as e:
            st.error(f"Could not load config JSON: {e}")

    # Update current retailer config in session
    crop_cfg = st.session_state.get("crop_cfg", crop_cfg)
    crop_cfg[retailer] = {"y0": float(y0), "y1": float(y1)}
    st.session_state["crop_cfg"] = crop_cfg

    if save_defaults:
        ok = save_crop_config(crop_cfg)
        if ok:
            st.success("Saved scan area defaults to crop_config.json.")
            st.caption("Note: Streamlit Community Cloud may reset local files after the app sleeps/restarts. Keep a downloaded JSON as backup.")
        else:
            st.error("Could not save crop_config.json on this environment.")

    if show_box:
        if pdf_file is None:
            st.warning("Upload a PDF first to preview the scan area.")
        else:
            try:
                pdf_bytes_preview = pdf_file.getvalue()
                png = render_scan_area_overlay(
                    pdf_bytes_preview,
                    retailer,
                    page_index=int(page_preview) - 1,
                    y0_frac=float(y0),
                    y1_frac=float(y1),
                )
                st.image(png, caption=f"{retailer} scan area preview (page {int(page_preview)})", use_container_width=True)
            except Exception as e:
                st.error(f"Could not render preview: {e}")

with st.expander("Vendor Map (built in by default)"):
:
    st.write(f"Default map file: `{DEFAULT_MAPS[retailer]}`")
    st.write("If you upload a map here, it will be used for this run only (it won't persist after a redeploy).")
    map_upload = st.file_uploader(f"Optional: Upload a {retailer} vendor map (xlsx)", type=["xlsx"], key=f"map_{retailer}")

pdf_file = st.file_uploader(f"Upload {retailer} PDF", type=["pdf"], key=f"pdf_{retailer}")

colA, colB = st.columns([1, 1])
with colA:
    process = st.button("Process PDF", type="primary", disabled=(pdf_file is None))
with colB:
    clear = st.button("Clear Results", disabled=(f"rows_{retailer}" not in st.session_state))

if clear:
    for k in [f"rows_{retailer}", f"lookup_{retailer}", f"pdfbytes_{retailer}", f"vendors_{retailer}"]:
        if k in st.session_state:
            del st.session_state[k]
    st.rerun()

def _ensure_state_loaded():
    if f"rows_{retailer}" not in st.session_state:
        return False
    return True

def _process_pdf_and_store_state():
    if pdf_file is None:
        return

    pdf_bytes = pdf_file.read()
    pdf_name = getattr(pdf_file, 'name', 'uploaded.pdf')

    # Load map and lookup
    try:
        df_map = load_vendor_map(retailer, uploaded_file=map_upload)
        lookup = build_lookup(df_map, retailer)
    except Exception as e:
        st.error(f"Vendor map error: {e}")
        st.stop()

    # Vendor list (from map) for dropdowns + special buckets
    vendor_list = sorted(set(lookup.values()))
    vendor_list_extended = vendor_list + ["REVIEW", "UNKNOWN", "MIXED/REVIEW"]

    with st.spinner("Reading PDF and matching vendors..."):
                crop_cfg = st.session_state.get('crop_cfg', load_crop_config())
        pages = extract_text_by_page_with_regions(pdf_bytes, retailer, crop_cfg)

    rows = []
    for i, page_obj in enumerate(pages):
        full_text = page_obj.get("full", "")
        region_text = page_obj.get("region", full_text)

        # Lowe's SOS tag pages: inherit vendor from the prior page (the order page)
        if retailer == "Lowe's" and is_sos_tag_page(full_text):
            if rows:
                final_vendor = rows[-1]["Vendor"]
                detected_vendor = rows[-1].get("Detected Vendor", final_vendor)
                confidence = max(int(rows[-1].get("Confidence %", 0)), 80)
            else:
                final_vendor = "REVIEW"
                detected_vendor = "SOS (no prior page)"
                confidence = 50

            rows.append({
                "Page": i + 1,
                "Vendor": final_vendor,
                "Detected Vendor": detected_vendor,
                "Confidence %": confidence,
                "Matched SKU/Model (first 15)": "",
            })
            continue

        vendor, matched, confidence = match_vendor(region_text, lookup, retailer)

        # Apply threshold: low-confidence pages get routed to REVIEW
        final_vendor = vendor
        if confidence < confidence_threshold and vendor not in ("UNKNOWN", "MIXED/REVIEW"):
            final_vendor = "REVIEW"

        rows.append({
            "Page": i + 1,
            "Vendor": final_vendor,
            "Detected Vendor": vendor,
            "Confidence %": confidence,
            "Matched SKU/Model (first 15)": ", ".join(matched) if matched else ""
        })

    # Store state
    st.session_state[f"rows_{retailer}"] = rows
    st.session_state[f"lookup_{retailer}"] = lookup
    st.session_state[f"pdfbytes_{retailer}"] = pdf_bytes
    st.session_state[f"pdfname_{retailer}"] = pdf_name
    st.session_state[f"vendors_{retailer}"] = vendor_list_extended

if process:
    _process_pdf_and_store_state()

if _ensure_state_loaded():
    rows = st.session_state[f"rows_{retailer}"]
    pdf_bytes = st.session_state[f"pdfbytes_{retailer}"]
    pdf_name = st.session_state.get(f"pdfname_{retailer}", f"{retailer}.pdf")
    vendor_list_extended = st.session_state[f"vendors_{retailer}"]

    df_report = pd.DataFrame(rows)

    st.subheader("Page → Vendor Report (with confidence)")
    st.dataframe(df_report, use_container_width=True, hide_index=True)

    # Build vendor PDFs (from final Vendor column) + ZIP
    page_vendor_rows = [{"PageIndex": r["Page"] - 1, "Vendor": r["Vendor"]} for r in rows]
    vendor_pdfs = build_vendor_pdfs(pdf_bytes, page_vendor_rows)
    warehouse_pdf = build_warehouse_print_pdf(pdf_bytes, page_vendor_rows, WAREHOUSE_VENDORS)
    zip_bytes = build_zip(vendor_pdfs, retailer, base_name=pdf_name, warehouse_print_pdf=warehouse_pdf)

    st.subheader("Downloads")
    zip_download_name = re.sub(r"\.pdf$", "", pdf_name, flags=re.IGNORECASE).strip() + "_VendorPdfs.zip"
    st.download_button(
        "Download Vendor ZIP",
        data=zip_bytes,
        file_name=zip_download_name,
        mime="application/zip"
    )

    csv = df_report.to_csv(index=False).encode("utf-8")
    st.download_button(
        "Download Report CSV",
        data=csv,
        file_name=f"{retailer.replace(' ', '')}_PageVendorReport.csv",
        mime="text/csv"
    )

    st.subheader("Summary")
    counts = df_report["Vendor"].value_counts().reset_index()
    counts.columns = ["Vendor", "Pages"]
    st.dataframe(counts, use_container_width=True, hide_index=True)

    # -----------------------------
    # Manual Override / Page Fixes
    # -----------------------------
    st.divider()
    st.subheader("Fix Pages That Need Review")

    st.caption(
        "This list only includes pages that were not confidently sorted (REVIEW / UNKNOWN / MIXED). "
        "You can correct them in bulk below. After applying, the report and ZIP will update."
    )

    needs_review_mask = df_report["Vendor"].isin(["REVIEW", "UNKNOWN", "MIXED/REVIEW"])
    df_needs = df_report.loc[needs_review_mask, ["Page", "Vendor", "Detected Vendor", "Confidence %", "Matched SKU/Model (first 15)"]].copy()

    if df_needs.empty:
        st.success("All pages were sorted to a vendor — nothing to review.")
    else:
        st.write("Bulk corrections (edit the Vendor column):")

        edited = st.data_editor(
            df_needs,
            use_container_width=True,
            hide_index=True,
            num_rows="fixed",
            column_config={
                "Vendor": st.column_config.SelectboxColumn(
                    "Vendor (set correct one)",
                    options=vendor_list_extended,
                    required=True,
                ),
                "Page": st.column_config.NumberColumn("Page", disabled=True),
                "Detected Vendor": st.column_config.TextColumn("Detected Vendor", disabled=True),
                "Confidence %": st.column_config.NumberColumn("Confidence %", disabled=True),
                "Matched SKU/Model (first 15)": st.column_config.TextColumn("Matched SKU/Model (first 15)", disabled=True),
            },
            key=f"bulk_editor_{retailer}"
        )

        apply_bulk = st.button("Apply bulk changes", type="secondary", key=f"apply_bulk_{retailer}")

        if apply_bulk:
            # Apply edits back to rows
            edited_map = {int(r["Page"]): r["Vendor"] for _, r in edited.iterrows()}
            for r in rows:
                pg = int(r["Page"])
                if pg in edited_map:
                    r["Vendor"] = edited_map[pg]
                    r["Detected Vendor"] = r.get("Detected Vendor", edited_map[pg])
                    r["Confidence %"] = max(int(r.get("Confidence %", 0) or 0), 99)

            st.session_state[f"rows_{retailer}"] = rows
            st.success("Applied bulk changes.")
            st.rerun()

    # -----------------------------
    # Optional: single page override
    # -----------------------------
    st.divider()
    st.subheader("Change Any Specific Page")

    st.caption("Use this if you want to change a page that was already assigned, or if you missed something above.")

    page_numbers_all = df_report["Page"].tolist()
    sel_page = st.selectbox("Page number to change", page_numbers_all, index=0, key=f"override_page_{retailer}")

    current_row = next((r for r in rows if r["Page"] == sel_page), None)
    if current_row is None:
        st.warning("Could not locate that page in the current run.")
    else:
        c1, c2, c3 = st.columns([1.2, 1.2, 2])

        with c1:
            st.text_input("Currently assigned vendor", value=str(current_row.get("Vendor", "")), disabled=True)
        with c2:
            st.text_input("Confidence %", value=str(current_row.get("Confidence %", "")), disabled=True)
        with c3:
            st.text_input("Matched SKU/Model (first 15)", value=str(current_row.get("Matched SKU/Model (first 15)", "")), disabled=True)

        new_vendor = st.selectbox(
            "Change to vendor",
            vendor_list_extended,
            index=vendor_list_extended.index(current_row.get("Vendor", "REVIEW")) if current_row.get("Vendor", "REVIEW") in vendor_list_extended else 0,
            key=f"override_vendor_{retailer}"
        )

        apply_change = st.button("Apply single change", type="secondary", key=f"apply_override_{retailer}")

        if apply_change:
            for r in rows:
                if r["Page"] == sel_page:
                    r["Vendor"] = new_vendor
                    r["Detected Vendor"] = r.get("Detected Vendor", new_vendor)
                    r["Confidence %"] = max(int(r.get("Confidence %", 0) or 0), 99)
                    break

            st.session_state[f"rows_{retailer}"] = rows
            st.success(f"Updated page {sel_page} to vendor: {new_vendor}")
            st.rerun()

else:
    st.info("Upload a PDF and click **Process PDF** to generate the report and vendor ZIP.")