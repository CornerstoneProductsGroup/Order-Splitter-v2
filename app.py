import streamlit as st
import pandas as pd
from io import BytesIO
import zipfile
import re
import json
import io
from collections import defaultdict

import fitz  # PyMuPDF
from PIL import Image, ImageDraw

from pypdf import PdfReader, PdfWriter

DEFAULT_MAPS = {
    "Home Depot": "vendor_map_hd.xlsx",
    "Lowe's": "vendor_map_lowes.xlsx",
    "Tractor Supply": "vendor_map_tsc.xlsx",
}

MAP_KEY_COL = {
    "Home Depot": "Model Number",
    "Lowe's": "SKU",
    "Tractor Supply": "SKU",
}
MAP_VENDOR_COL = "Vendor"

WAREHOUSE_VENDORS = [
    "Cord Mate", "Gate Latch", "Home Selects", "Nisus",
    "Post Protector-Here", "Soft Seal", "Weedshark", "Zaca",
]

CROP_CONFIG_PATH = "crop_config.json"

# 🔒 LOCKED DEFAULTS (your tuned values)
CROP_CONFIG_DEFAULTS = {
    "Home Depot": {"x0": 0.02, "x1": 0.14, "y0": 0.26, "y1": 0.54},
    "Lowe's": {"x0": 0.52, "x1": 0.79, "y0": 0.25, "y1": 0.67},
    "Tractor Supply": {"x0": 0.14, "x1": 0.30, "y0": 0.20, "y1": 0.55},
}

def normalize_key(x: str) -> str:
    if x is None:
        return ""
    s = str(x).strip().upper()
    return re.sub(r"[\s\-_]", "", s)

def load_vendor_map(retailer: str, uploaded_file=None) -> pd.DataFrame:
    return pd.read_excel(uploaded_file) if uploaded_file is not None else pd.read_excel(DEFAULT_MAPS[retailer])

def build_lookup(df: pd.DataFrame, retailer: str) -> dict:
    key_col = MAP_KEY_COL[retailer]
    if key_col not in df.columns or MAP_VENDOR_COL not in df.columns:
        raise ValueError(f"Vendor map for {retailer} must include '{key_col}' and '{MAP_VENDOR_COL}'. Found: {list(df.columns)}")
    out = {}
    for _, row in df.iterrows():
        k = normalize_key(row.get(key_col))
        v = str(row.get(MAP_VENDOR_COL)).strip() if pd.notna(row.get(MAP_VENDOR_COL)) else ""
        if k and v:
            out[k] = v
    return out

def is_sos_tag_page(text: str) -> bool:
    t = (text or "").upper()
    return any(k in t for k in ["SOS","SHIP TO STORE","STORE PICKUP","PICK UP IN STORE","S2S","SPECIAL ORDER"])

def load_crop_config() -> dict:
    try:
        import os
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

def save_crop_config(cfg: dict) -> bool:
    try:
        with open(CROP_CONFIG_PATH, "w", encoding="utf-8") as f:
            json.dump(cfg, f, indent=2)
        return True
    except Exception:
        return False

def render_scan_area_overlay(pdf_bytes: bytes, page_index: int, rect_cfg: dict, zoom: float = 2.0) -> bytes:
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    page_index = max(0, min(page_index, doc.page_count - 1))
    page = doc.load_page(page_index)
    pix = page.get_pixmap(matrix=fitz.Matrix(zoom, zoom))
    img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
    draw = ImageDraw.Draw(img)

    h = page.rect.height
    w = page.rect.width

    x0f = float(rect_cfg.get("x0", 0.0)); x1f = float(rect_cfg.get("x1", 1.0))
    y0f = float(rect_cfg.get("y0", 0.0)); y1f = float(rect_cfg.get("y1", 1.0))
    if x1f < x0f: x0f, x1f = x1f, x0f
    if y1f < y0f: y0f, y1f = y1f, y0f

    left = x0f * w
    right = x1f * w
    top = (1 - y1f) * h
    bottom = (1 - y0f) * h
    left *= zoom; right *= zoom; top *= zoom; bottom *= zoom
    draw.rectangle([left, top, right, bottom], outline="red", width=6)

    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()

def extract_text_by_page_with_regions(pdf_bytes: bytes, retailer: str, crop_cfg: dict) -> list[dict]:
    cfg = crop_cfg.get(retailer, {"x0":0.0,"x1":1.0,"y0":0.0,"y1":1.0})
    x0f = float(cfg.get("x0",0.0)); x1f = float(cfg.get("x1",1.0))
    y0f = float(cfg.get("y0",0.0)); y1f = float(cfg.get("y1",1.0))
    if x1f < x0f: x0f, x1f = x1f, x0f
    if y1f < y0f: y0f, y1f = y1f, y0f

    reader = PdfReader(BytesIO(pdf_bytes))
    out = []
    for page in reader.pages:
        try:
            full_text = page.extract_text() or ""
        except Exception:
            full_text = ""
        try:
            mb = page.mediabox
            left = float(mb.left); right = float(mb.right)
            bottom = float(mb.bottom); top = float(mb.top)
            w = right - left; h = top - bottom
            x0 = left + x0f * w
            x1 = left + x1f * w
            y0 = bottom + y0f * h
            y1 = bottom + y1f * h

            chunks=[]
            def visitor_text(text, cm, tm, font_dict, font_size):
                try:
                    x = float(tm[4]); y = float(tm[5])
                except Exception:
                    return
                if x0 <= x <= x1 and y0 <= y <= y1:
                    chunks.append(text)
            page.extract_text(visitor_text=visitor_text)
            region_text = "".join(chunks) or full_text
        except Exception:
            region_text = full_text
        out.append({"full": full_text, "region": region_text})
    return out

def match_vendor(text: str, lookup: dict) -> tuple[str, list[str], int]:
    t = normalize_key(text)
    matched=[]
    vendors=set()
    for k,vendor in lookup.items():
        if k and k in t:
            matched.append(k); vendors.add(vendor)
    if not vendors:
        return "UNKNOWN", [], 0
    if len(vendors)>1:
        return "MIXED/REVIEW", matched[:15], 25
    hit=len(set(matched))
    conf = 98 if hit>=5 else 95 if hit==4 else 92 if hit==3 else 88 if hit==2 else 80 if hit==1 else 60
    return next(iter(vendors)), matched[:15], conf

def build_vendor_pdfs(pdf_bytes: bytes, page_vendor_rows: list[dict]) -> dict[str, bytes]:
    reader = PdfReader(BytesIO(pdf_bytes))
    pages_by_vendor=defaultdict(list)
    for r in page_vendor_rows:
        pages_by_vendor[r["Vendor"]].append(r["PageIndex"])
    out={}
    for vendor, idxs in pages_by_vendor.items():
        w=PdfWriter()
        for i in idxs:
            w.add_page(reader.pages[i])
        buf=BytesIO(); w.write(buf)
        out[vendor]=buf.getvalue()
    return out

def build_warehouse_print_pdf(pdf_bytes: bytes, page_vendor_rows: list[dict], vendors: list[str]) -> bytes | None:
    reader = PdfReader(BytesIO(pdf_bytes))
    pages_by_vendor=defaultdict(list)
    for r in page_vendor_rows:
        pages_by_vendor[r["Vendor"]].append(r["PageIndex"])
    target=[v for v in vendors if v in pages_by_vendor]
    if not target: return None
    w=PdfWriter()
    for vendor in sorted(target, key=lambda x: x.lower()):
        for i in sorted(pages_by_vendor[vendor]):
            w.add_page(reader.pages[i])
    buf=BytesIO(); w.write(buf)
    return buf.getvalue()

def build_zip(vendor_pdfs: dict[str, bytes], retailer: str, base_name: str, warehouse_print_pdf: bytes | None=None) -> bytes:
    buf=BytesIO()
    base=re.sub(r"\.pdf$","",base_name,flags=re.IGNORECASE).strip()
    base=re.sub(r"[\\/:*?\"<>|]+","_",base).strip() or retailer.replace(" ","")
    with zipfile.ZipFile(buf,"w",compression=zipfile.ZIP_DEFLATED) as z:
        if warehouse_print_pdf is not None:
            z.writestr(f"{base} - WAREHOUSE PRINT.pdf", warehouse_print_pdf)
        for vendor, data in vendor_pdfs.items():
            safe=re.sub(r"[^\w\-. ]+","_",vendor).strip() or "UNKNOWN"
            z.writestr(f"{base} - {safe}.pdf", data)
    return buf.getvalue()

st.set_page_config(page_title="Retail Order Splitter", layout="wide")
st.title("Retail Order Splitter")

if "crop_cfg" not in st.session_state:
    st.session_state["crop_cfg"] = load_crop_config()

tab_splitter, tab_tuning = st.tabs(["Order Splitter", "Scan Area Tuning"])

with tab_tuning:
    st.subheader("Scan Area Tuning")
    st.caption("Defaults are locked into the app. You can still preview/adjust here. For persistence after sleep, commit crop_config.json to your repo.")
    t_retailer = st.selectbox("Retailer", ["Home Depot","Lowe's","Tractor Supply"], index=0, key="tuning_retailer")
    crop_cfg = st.session_state.get("crop_cfg", load_crop_config())
    cur = crop_cfg.get(t_retailer, CROP_CONFIG_DEFAULTS[t_retailer])

    c1,c2=st.columns(2)
    with c1:
        x0=st.slider("Left (x0)",0.0,1.0,float(cur.get("x0",0.0)),0.01)
        y0=st.slider("Bottom (y0)",0.0,1.0,float(cur.get("y0",0.0)),0.01)
    with c2:
        x1=st.slider("Right (x1)",0.0,1.0,float(cur.get("x1",1.0)),0.01)
        y1=st.slider("Top (y1)",0.0,1.0,float(cur.get("y1",1.0)),0.01)

    pdf_preview=st.file_uploader("Upload a PDF to preview", type=["pdf"], key="tuning_pdf")
    page_preview=st.number_input("Preview page number", min_value=1, value=1, step=1, key="tuning_page")

    xx0,xx1=float(x0),float(x1); yy0,yy1=float(y0),float(y1)
    if xx1<xx0: xx0,xx1=xx1,xx0
    if yy1<yy0: yy0,yy1=yy1,yy0
    crop_cfg[t_retailer]={"x0":xx0,"x1":xx1,"y0":yy0,"y1":yy1}
    st.session_state["crop_cfg"]=crop_cfg

    a,b,c=st.columns([1,1,1])
    with a:
        show=st.button("Show scan area", key="tuning_show")
    with b:
        save=st.button("Save as default (writes crop_config.json)", key="tuning_save")
    with c:
        st.download_button("Download config JSON", data=json.dumps(crop_cfg,indent=2).encode("utf-8"),
                           file_name="crop_config.json", mime="application/json", key="tuning_dl")

    cfg_up=st.file_uploader("Upload config JSON (optional)", type=["json"], key="tuning_cfg_up")
    if cfg_up is not None:
        try:
            up=json.load(cfg_up)
            if isinstance(up,dict):
                for r,d in CROP_CONFIG_DEFAULTS.items():
                    up.setdefault(r,d)
                st.session_state["crop_cfg"]=up
                crop_cfg=up
                st.success("Loaded config JSON into this session.")
        except Exception as e:
            st.error(f"Could not load config JSON: {e}")

    if save:
        st.success("Saved crop_config.json.") if save_crop_config(st.session_state.get("crop_cfg",crop_cfg)) else st.error("Could not save crop_config.json.")
    if show:
        if pdf_preview is None:
            st.warning("Upload a PDF first.")
        else:
            overlay=render_scan_area_overlay(pdf_preview.getvalue(), int(page_preview)-1, crop_cfg[t_retailer], zoom=2.0)
            st.image(overlay, caption=f"{t_retailer} scan area preview (page {int(page_preview)})", use_container_width=True)

with tab_splitter:
    retailer=st.selectbox("Retailer", ["Home Depot","Lowe's","Tractor Supply"], index=1, key="splitter_retailer")
    confidence_threshold=st.slider("Confidence threshold (pages below this will be flagged as REVIEW)",0,100,70,5)

    with st.expander("Vendor Map (built in by default)"):
        st.write(f"Default map file: `{DEFAULT_MAPS[retailer]}`")
        map_upload=st.file_uploader(f"Optional: Upload a {retailer} vendor map (xlsx)", type=["xlsx"], key=f"map_{retailer}")

    pdf_file=st.file_uploader(f"Upload {retailer} PDF", type=["pdf"], key=f"pdf_{retailer}")

    colA,colB=st.columns([1,1])
    with colA:
        process=st.button("Process PDF", type="primary", disabled=(pdf_file is None), key="process_btn")
    with colB:
        clear=st.button("Clear Results", disabled=(f"rows_{retailer}" not in st.session_state), key="clear_btn")

    if clear:
        for k in [f"rows_{retailer}", f"lookup_{retailer}", f"pdfbytes_{retailer}", f"vendors_{retailer}", f"pdfname_{retailer}"]:
            st.session_state.pop(k, None)
        st.rerun()

    if process and pdf_file is not None:
        pdf_bytes=pdf_file.read()
        pdf_name=getattr(pdf_file,"name","uploaded.pdf")
        try:
            df_map=load_vendor_map(retailer, uploaded_file=map_upload)
            lookup=build_lookup(df_map, retailer)
        except Exception as e:
            st.error(f"Vendor map error: {e}")
            st.stop()

        vendor_list=sorted(set(lookup.values()))
        vendor_list_extended=vendor_list+["REVIEW","UNKNOWN","MIXED/REVIEW"]

        crop_cfg=st.session_state.get("crop_cfg", load_crop_config())
        pages=extract_text_by_page_with_regions(pdf_bytes, retailer, crop_cfg)

        rows=[]
        for i, pobj in enumerate(pages):
            full=pobj.get("full",""); region=pobj.get("region",full)
            if retailer=="Lowe's" and is_sos_tag_page(full):
                if rows:
                    final_vendor=rows[-1]["Vendor"]
                    detected=rows[-1].get("Detected Vendor", final_vendor)
                    conf=max(int(rows[-1].get("Confidence %",0)),80)
                else:
                    final_vendor="REVIEW"; detected="SOS (no prior page)"; conf=50
                rows.append({"Page":i+1,"Vendor":final_vendor,"Detected Vendor":detected,"Confidence %":conf,"Matched SKU/Model (first 15)":"","SOS Tag":True})
                continue

            vendor, matched, conf = match_vendor(region, lookup)
            final_vendor=vendor
            if conf < confidence_threshold and vendor not in ("UNKNOWN","MIXED/REVIEW"):
                final_vendor="REVIEW"
            rows.append({"Page":i+1,"Vendor":final_vendor,"Detected Vendor":vendor,"Confidence %":conf,
                         "Matched SKU/Model (first 15)":", ".join(matched) if matched else "","SOS Tag":False})

        st.session_state[f"rows_{retailer}"]=rows
        st.session_state[f"pdfbytes_{retailer}"]=pdf_bytes
        st.session_state[f"pdfname_{retailer}"]=pdf_name
        st.session_state[f"vendors_{retailer}"]=vendor_list_extended

    if f"rows_{retailer}" in st.session_state:
        rows=st.session_state[f"rows_{retailer}"]
        pdf_bytes=st.session_state[f"pdfbytes_{retailer}"]
        pdf_name=st.session_state.get(f"pdfname_{retailer}", f"{retailer}.pdf")
        vendor_list_extended=st.session_state[f"vendors_{retailer}"]

        df=pd.DataFrame(rows)
        st.subheader("Page → Vendor Report (with confidence)")
        st.dataframe(df, use_container_width=True, hide_index=True)

        page_vendor_rows=[{"PageIndex":int(r["Page"])-1,"Vendor":r["Vendor"]} for r in rows]
        vendor_pdfs=build_vendor_pdfs(pdf_bytes, page_vendor_rows)
        warehouse_pdf=build_warehouse_print_pdf(pdf_bytes, page_vendor_rows, WAREHOUSE_VENDORS)
        zip_bytes=build_zip(vendor_pdfs, retailer, base_name=pdf_name, warehouse_print_pdf=warehouse_pdf)

        st.subheader("Downloads")
        st.download_button("Download Vendor ZIP", data=zip_bytes,
                           file_name=f"{re.sub(r'\\.pdf$','',pdf_name,flags=re.IGNORECASE)}_VendorPdfs.zip",
                           mime="application/zip")

        st.divider()
        st.subheader("Fix Pages That Need Review")
        needs=df["Vendor"].isin(["REVIEW","UNKNOWN","MIXED/REVIEW"])
        df_needs=df.loc[needs, ["Page","Vendor","Detected Vendor","Confidence %","Matched SKU/Model (first 15)","SOS Tag"]].copy()
        if df_needs.empty:
            st.success("All pages were sorted to a vendor — nothing to review.")
        else:
            edited=st.data_editor(
                df_needs,
                use_container_width=True,
                hide_index=True,
                num_rows="fixed",
                column_config={
                    "Vendor": st.column_config.SelectboxColumn("Vendor (set correct one)", options=vendor_list_extended, required=True),
                    "Page": st.column_config.NumberColumn("Page", disabled=True),
                    "Detected Vendor": st.column_config.TextColumn("Detected Vendor", disabled=True),
                    "Confidence %": st.column_config.NumberColumn("Confidence %", disabled=True),
                    "Matched SKU/Model (first 15)": st.column_config.TextColumn("Matched SKU/Model (first 15)", disabled=True),
                    "SOS Tag": st.column_config.CheckboxColumn("SOS Tag", disabled=True),
                },
                key=f"bulk_editor_{retailer}",
            )
            if st.button("Apply bulk changes", type="secondary", key=f"apply_bulk_{retailer}"):
                mp={int(r["Page"]): r["Vendor"] for _, r in edited.iterrows()}
                for r in rows:
                    pg=int(r["Page"])
                    if pg in mp:
                        r["Vendor"]=mp[pg]
                        r["Confidence %"]=max(int(r.get("Confidence %",0) or 0), 99)
                st.session_state[f"rows_{retailer}"]=rows
                st.success("Applied bulk changes.")
                st.rerun()
