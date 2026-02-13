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


def match_vendor(text: str, lookup: dict) -> tuple[str, list[str]]:
    """
    Find SKUs/Models present in page text.
    Returns:
      (vendor, matched_keys)
    If multiple vendors are found -> "MIXED/REVIEW".
    If none -> "UNKNOWN".
    """
    # normalize page text similarly
    t = normalize_key(text)

    matched = []
    vendors = set()

    # Fast-ish approach: check each key as substring of normalized page text
    # (works well for SKU-style strings like BGC10 etc.)
    for k, vendor in lookup.items():
        if k and k in t:
            matched.append(k)
            vendors.add(vendor)

    if not vendors:
        return "UNKNOWN", []
    if len(vendors) > 1:
        return "MIXED/REVIEW", matched[:15]
    return next(iter(vendors)), matched[:15]


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
st.set_page_config(page_title="Retail Order Splitter (Basic)", layout="wide")
st.title("Retail Order Splitter (Basic)")

st.caption(
    "Upload a PDF. The app scans each page for SKUs/Models from the vendor map, assigns a vendor, "
    "then builds a ZIP with one PDF per vendor containing *all* pages for that vendor."
)

retailer = st.selectbox("Retailer", ["Home Depot", "Lowe's", "Tractor Supply"], index=1)

with st.expander("Vendor Map (built in by default)"):
    st.write(f"Default map file: `{DEFAULT_MAPS[retailer]}`")
    st.write("If you upload a map here, it will be used for this run only (it won't persist after a redeploy).")
    map_upload = st.file_uploader(f"Optional: Upload a {retailer} vendor map (xlsx)", type=["xlsx"], key=f"map_{retailer}")

pdf_file = st.file_uploader(f"Upload {retailer} PDF", type=["pdf"], key=f"pdf_{retailer}")

process = st.button("Process PDF", type="primary", disabled=(pdf_file is None))

if process and pdf_file is not None:
    pdf_bytes = pdf_file.read()

    # Load map and lookup
    try:
        df_map = load_vendor_map(retailer, uploaded_file=map_upload)
        lookup = build_lookup(df_map, retailer)
    except Exception as e:
        st.error(f"Vendor map error: {e}")
        st.stop()

    # Extract page texts
    with st.spinner("Reading PDF and matching vendors..."):
        texts = extract_text_by_page(pdf_bytes)

    rows = []
    for i, text in enumerate(texts):
        vendor, matched = match_vendor(text, lookup)
        rows.append({
            "Page": i + 1,
            "Vendor": vendor,
            "Matched SKU/Model (first 15)": ", ".join(matched) if matched else ""
        })

    df_report = pd.DataFrame(rows)
    st.subheader("Page → Vendor Report")
    st.dataframe(df_report, use_container_width=True, hide_index=True)

    # Build vendor PDFs (from report)
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

    # Optional: download report
    csv = df_report.to_csv(index=False).encode("utf-8")
    st.download_button(
        "Download Report CSV",
        data=csv,
        file_name=f"{retailer.replace(' ', '')}_PageVendorReport.csv",
        mime="text/csv"
    )

    # Quick stats
    st.subheader("Summary")
    counts = df_report["Vendor"].value_counts().reset_index()
    counts.columns = ["Vendor", "Pages"]
    st.dataframe(counts, use_container_width=True, hide_index=True)
