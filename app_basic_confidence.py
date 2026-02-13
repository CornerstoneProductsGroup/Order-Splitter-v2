import streamlit as st
import pandas as pd
from io import BytesIO
import zipfile
import re
from collections import defaultdict

from pypdf import PdfReader, PdfWriter


# -----------------------------
# Defaults (packaged in repo)
# -----------------------------
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


def extract_text_by_page(pdf_bytes: bytes) -> list[str]:
    reader = PdfReader(BytesIO(pdf_bytes))
    out = []
    for page in reader.pages:
        try:
            out.append(page.extract_text() or "")
        except Exception:
            out.append("")
    return out



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

def match_vendor(text: str, lookup: dict) -> tuple[str, list[str], int]:
    """
    Find SKUs/Models present in page text.
    Returns:
      (vendor, matched_keys, confidence_percent)

    Confidence is a heuristic:
      - UNKNOWN: 0%
      - MIXED/REVIEW: 25%
      - Single-vendor pages: confidence rises with number of SKU/Model hits
    """
    # normalize page text similarly
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
    if hit_count >= 4:
        conf = 95
    elif hit_count == 3:
        conf = 90
    elif hit_count == 2:
        conf = 80
    elif hit_count == 1:
        conf = 65
    else:
        conf = 50

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


def build_zip(vendor_pdfs: dict[str, bytes], retailer: str) -> bytes:
    buff = BytesIO()
    with zipfile.ZipFile(buff, "w", compression=zipfile.ZIP_DEFLATED) as z:
        for vendor, pdf_data in vendor_pdfs.items():
            safe_vendor = re.sub(r"[^\w\-. ]+", "_", vendor).strip() or "UNKNOWN"
            filename = f"{retailer} - {safe_vendor}.pdf"
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

confidence_threshold = st.slider(
    "Confidence threshold (pages below this will be flagged as REVIEW)",
    min_value=0,
    max_value=100,
    value=70,
    step=5,
    help="If a page's confidence is below the threshold, it will be labeled REVIEW instead of being assigned to a vendor."
)

with st.expander("Vendor Map (built in by default)"):
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
        texts = extract_text_by_page(pdf_bytes)

    rows = []
    for i, text in enumerate(texts):
        # Lowe's SOS tag pages: inherit vendor from the prior page (the order page)
        if retailer == "Lowe's" and is_sos_tag_page(text):
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

        vendor, matched, confidence = match_vendor(text, lookup)

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
    st.session_state[f"vendors_{retailer}"] = vendor_list_extended

if process:
    _process_pdf_and_store_state()

if _ensure_state_loaded():
    rows = st.session_state[f"rows_{retailer}"]
    pdf_bytes = st.session_state[f"pdfbytes_{retailer}"]
    vendor_list_extended = st.session_state[f"vendors_{retailer}"]

    df_report = pd.DataFrame(rows)

    st.subheader("Page → Vendor Report (with confidence)")
    st.dataframe(df_report, use_container_width=True, hide_index=True)

    # Build vendor PDFs (from final Vendor column) + ZIP
    page_vendor_rows = [{"PageIndex": r["Page"] - 1, "Vendor": r["Vendor"]} for r in rows]
    vendor_pdfs = build_vendor_pdfs(pdf_bytes, page_vendor_rows)
    zip_bytes = build_zip(vendor_pdfs, retailer)

    st.subheader("Downloads")
    st.download_button(
        "Download Vendor ZIP",
        data=zip_bytes,
        file_name=f"{retailer.replace(' ', '')}_VendorPdfs.zip",
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
    st.subheader("Fix Page Assignments")

    st.caption(
        "If a page was assigned to the wrong vendor (or routed to REVIEW due to low confidence), "
        "you can override it here. After applying, the report and ZIP will update immediately."
    )

    page_numbers = df_report["Page"].tolist()
    sel_page = st.selectbox("Page number to change", page_numbers, index=0, key=f"override_page_{retailer}")

    # Pull current row info
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

        apply_change = st.button("Apply change", type="secondary", key=f"apply_override_{retailer}")

        if apply_change:
            # Update row
            for r in rows:
                if r["Page"] == sel_page:
                    r["Vendor"] = new_vendor
                    # Keep original detected vendor for transparency; if absent, set it
                    r["Detected Vendor"] = r.get("Detected Vendor", new_vendor)
                    # If user overrides, set confidence high so it doesn't get re-flagged by threshold later
                    r["Confidence %"] = max(int(r.get("Confidence %", 0) or 0), 99)
                    break

            st.session_state[f"rows_{retailer}"] = rows
            st.success(f"Updated page {sel_page} to vendor: {new_vendor}")
            st.rerun()
else:
    st.info("Upload a PDF and click **Process PDF** to generate the report and vendor ZIP.")
