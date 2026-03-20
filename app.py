import streamlit as st
import pandas as pd
from io import BytesIO
import zipfile as zf
import re
import json
import io
from collections import defaultdict

from pypdf import PdfReader, PdfWriter
import fitz  # PyMuPDF
from PIL import Image, ImageDraw

# -----------------------------
# Config / Defaults
# -----------------------------
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

# 🔒 Locked defaults (your tuned values)
CROP_CONFIG_DEFAULTS = {
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


def normalize_region(region: dict | None, fallback: dict | None = None) -> dict:
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


def default_region(retailer: str, key: str) -> dict:
    section = CROP_CONFIG_DEFAULTS.get(retailer, {})
    raw = section.get(key, {"x0": 0.0, "x1": 1.0, "y0": 0.0, "y1": 1.0})
    return normalize_region(raw)


def merge_retailer_config(retailer: str, raw: dict | None) -> dict:
    section = raw if isinstance(raw, dict) else {}
    merged: dict = {}

    if all(k in section for k in ("x0", "x1", "y0", "y1")):
        merged["extract_region"] = normalize_region(section)
    else:
        merged["extract_region"] = normalize_region(
            section.get("extract_region"),
            default_region(retailer, "extract_region"),
        )

    if retailer == "Lowe's":
        merged["sos_output_crop"] = normalize_region(
            section.get("sos_output_crop"),
            default_region(retailer, "sos_output_crop"),
        )
    elif retailer == "Tractor Supply":
        regs = section.get("redact_regions", CROP_CONFIG_DEFAULTS[retailer].get("redact_regions", []))
        merged["redact_regions"] = [normalize_region(r) for r in regs if isinstance(r, dict)]

    return merged


def extract_region_from_cfg(retailer: str, crop_cfg: dict) -> dict:
    return merge_retailer_config(retailer, crop_cfg.get(retailer)).get("extract_region", {"x0": 0.0, "x1": 1.0, "y0": 0.0, "y1": 1.0})


def sos_crop_region_from_cfg(crop_cfg: dict) -> dict | None:
    return merge_retailer_config("Lowe's", crop_cfg.get("Lowe's")).get("sos_output_crop")


def redact_regions_from_cfg(crop_cfg: dict) -> list[dict]:
    return merge_retailer_config("Tractor Supply", crop_cfg.get("Tractor Supply")).get("redact_regions", [])


def region_to_rect(page: fitz.Page, region: dict) -> fitz.Rect:
    w = page.rect.width
    h = page.rect.height
    left = region["x0"] * w
    right = region["x1"] * w
    top = (1 - region["y1"]) * h
    bottom = (1 - region["y0"]) * h
    rect_rot = fitz.Rect(left, top, right, bottom)
    return rect_rot * page.derotation_matrix


def region_to_rotated_rect(page: fitz.Page, region: dict) -> fitz.Rect:
    w = page.rect.width
    h = page.rect.height
    left = region["x0"] * w
    right = region["x1"] * w
    top = (1 - region["y1"]) * h
    bottom = (1 - region["y0"]) * h
    return fitz.Rect(left, top, right, bottom)


# -----------------------------
# Helpers
# -----------------------------
def normalize_key(x: str) -> str:
    if x is None:
        return ""
    s = str(x).strip().upper()
    s = re.sub(r"[\s\-_]", "", s)
    return s


def load_vendor_map(retailer: str, uploaded_file=None) -> pd.DataFrame:
    if uploaded_file is not None:
        return pd.read_excel(uploaded_file)
    return pd.read_excel(DEFAULT_MAPS[retailer])


def build_lookup(df: pd.DataFrame, retailer: str) -> dict:
    """
    Build SKU/Model -> Vendor lookup.

    Important: we intentionally skip "too-short" alpha-only keys (e.g., "ORU"),
    because substring matching on short words can create false positives from
    common phrases like "FOR OUR", "YOUR", etc.
    Rule:
      - keep if the normalized key contains ANY digit, OR length >= 4
      - skip if alpha-only and length < 4
    """
    key_col = MAP_KEY_COL[retailer]
    if key_col not in df.columns or MAP_VENDOR_COL not in df.columns:
        raise ValueError(
            f"Vendor map for {retailer} must include columns '{key_col}' and '{MAP_VENDOR_COL}'. "
            f"Found: {list(df.columns)}"
        )
    lookup = {}
    skipped_short = 0

    for _, row in df.iterrows():
        k_raw = row.get(key_col)
        k = normalize_key(k_raw)
        v = row.get(MAP_VENDOR_COL)

        if pd.notna(v):
            v = str(v).strip()
        else:
            v = ""

        if not k or not v:
            continue

        has_digit = any(ch.isdigit() for ch in k)
        if (not has_digit) and len(k) < 4:
            skipped_short += 1
            continue

        lookup[k] = v

    # stash a small diagnostic for UI
    try:
        st.session_state["_skipped_short_keys"] = int(skipped_short)
    except Exception:
        pass

    return lookup


def is_sos_tag_page(text: str) -> bool:
    t = (text or "").upper()
    return any(k in t for k in ["SOS", "SHIP TO STORE", "STORE PICKUP", "PICK UP IN STORE", "S2S", "SPECIAL ORDER"])


def load_crop_config() -> dict:
    # Priority: crop_config.json (if exists) else locked defaults
    try:
        import os
        if os.path.exists(CROP_CONFIG_PATH):
            with open(CROP_CONFIG_PATH, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict):
                return {r: merge_retailer_config(r, data.get(r)) for r in CROP_CONFIG_DEFAULTS}
    except Exception:
        pass
    return {r: merge_retailer_config(r, None) for r in CROP_CONFIG_DEFAULTS}


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

    w = page.rect.width
    h = page.rect.height

    cfg = normalize_region(rect_cfg)
    x0f = cfg["x0"]
    x1f = cfg["x1"]
    y0f = cfg["y0"]
    y1f = cfg["y1"]

    left = x0f * w
    right = x1f * w
    top = (1 - y1f) * h
    bottom = (1 - y0f) * h

    left *= zoom
    right *= zoom
    top *= zoom
    bottom *= zoom

    draw.rectangle([left, top, right, bottom], outline="red", width=6)

    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()




def extract_text_by_page_with_regions(pdf_bytes: bytes, retailer: str, crop_cfg: dict) -> list[dict]:
    """
    Rotation-safe extraction:
    - Full text via pypdf (for SOS detection etc.)
    - Region text built by filtering PyMuPDF "words" that fall inside the scan rectangle.
      We compute the scan rectangle in the rendered (rotated) page space, then transform it
      into the unrotated text space using page.derotation_matrix.
    """
    cfg = extract_region_from_cfg(retailer, crop_cfg)
    x0f = cfg["x0"]
    x1f = cfg["x1"]
    y0f = cfg["y0"]
    y1f = cfg["y1"]

    # Full text via pypdf (stable for whole-page)
    reader = PdfReader(BytesIO(pdf_bytes))
    full_texts = []
    for page in reader.pages:
        try:
            full_texts.append(page.extract_text() or "")
        except Exception:
            full_texts.append("")

    region_texts = []
    try:
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        for i in range(doc.page_count):
            page = doc.load_page(i)

            # Build scan rect in rotated/rendered page space (same as the preview box)
            w = page.rect.width
            h = page.rect.height
            left = x0f * w
            right = x1f * w
            top = (1 - y1f) * h
            bottom = (1 - y0f) * h
            rect_rot = fitz.Rect(left, top, right, bottom)

            # Transform to unrotated text space so it matches PyMuPDF word coordinates
            rect = rect_rot * page.derotation_matrix

            words = page.get_text("words")  # (x0,y0,x1,y1,word,block,line,wordno) in unrotated space
            # Filter words fully inside rect (with small tolerance)
            tol = 1.0
            picked = [w for w in words if (w[0] >= rect.x0 - tol and w[2] <= rect.x1 + tol and w[1] >= rect.y0 - tol and w[3] <= rect.y1 + tol)]

            # Sort in reading order (top->bottom then left->right)
            picked.sort(key=lambda x: (round(x[1], 1), x[0]))
            txt = " ".join([w[4] for w in picked]).strip()
            if not txt:
                txt = full_texts[i]
            region_texts.append(txt)
    except Exception:
        region_texts = full_texts[:]

    return [{"full": full_texts[i], "region": region_texts[i]} for i in range(len(full_texts))]


def match_vendor(text: str, lookup: dict) -> tuple[str, list[str], int]:
    raw = (text or "").upper()
    compact = normalize_key(text)
    matched = []
    vendors = set()

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
    conf = 98 if hit >= 5 else 95 if hit == 4 else 92 if hit == 3 else 88 if hit == 2 else 80 if hit == 1 else 60
    return next(iter(vendors)), matched[:15], conf


def build_vendor_pdfs(pdf_bytes: bytes, page_vendor_rows: list[dict], retailer: str, crop_cfg: dict) -> dict[str, bytes]:
    src_doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    pages_by_vendor = defaultdict(list)
    row_by_page: dict[int, dict] = {}
    for r in page_vendor_rows:
        pages_by_vendor[r["Vendor"]].append(r["PageIndex"])
        row_by_page[r["PageIndex"]] = r

    sos_crop = sos_crop_region_from_cfg(crop_cfg) if retailer == "Lowe's" else None
    redact_regions = redact_regions_from_cfg(crop_cfg) if retailer == "Tractor Supply" else []

    vendor_pdfs = {}
    for vendor, idxs in pages_by_vendor.items():
        out_doc = fitz.open()
        for i in idxs:
            row = row_by_page.get(i, {})
            is_sos_page = sos_crop is not None and bool(row.get("SOS Tag", False))

            if is_sos_page:
                src_page = src_doc.load_page(i)
                clip_rect = region_to_rotated_rect(src_page, sos_crop)
                pix = src_page.get_pixmap(matrix=fitz.Matrix(2, 2), clip=clip_rect, alpha=False)
                page = out_doc.new_page(width=clip_rect.width, height=clip_rect.height)
                page.insert_image(page.rect, pixmap=pix)
            else:
                out_doc.insert_pdf(src_doc, from_page=i, to_page=i)
                page = out_doc[-1]

            if redact_regions:
                for reg in redact_regions:
                    page.draw_rect(region_to_rect(page, reg), color=(1, 1, 1), fill=(1, 1, 1), overlay=True)

        buf = BytesIO()
        out_doc.save(buf)
        out_doc.close()
        vendor_pdfs[vendor] = buf.getvalue()

    src_doc.close()
    return vendor_pdfs


def build_warehouse_print_pdf(pdf_bytes: bytes, page_vendor_rows: list[dict], vendors: list[str]) -> bytes | None:
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

    buf = BytesIO()
    writer.write(buf)
    return buf.getvalue()


def build_zip(vendor_pdfs: dict[str, bytes], base_name: str, warehouse_print_pdf: bytes | None) -> bytes:
    buf = BytesIO()
    base = re.sub(r"\.pdf$", "", base_name, flags=re.IGNORECASE).strip()
    base = re.sub(r"[\\/:*?\"<>|]+", "_", base).strip() or "Orders"

    with zf.ZipFile(buf, "w", compression=zf.ZIP_DEFLATED) as z:
        if warehouse_print_pdf is not None:
            z.writestr(f"{base} - WAREHOUSE PRINT.pdf", warehouse_print_pdf)

        for vendor, data in vendor_pdfs.items():
            safe_vendor = re.sub(r"[^\w\-. ]+", "_", vendor).strip() or "UNKNOWN"
            z.writestr(f"{base} - {safe_vendor}.pdf", data)

    return buf.getvalue()


# -----------------------------
# UI
# -----------------------------
st.set_page_config(page_title="Retail Order Splitter", layout="wide")
st.title("Retail Order Splitter")

if "crop_cfg" not in st.session_state:
    st.session_state["crop_cfg"] = load_crop_config()

tab_split, tab_tune = st.tabs(["Order Splitter", "Scan Area Tuning"])

with tab_tune:
    st.subheader("Scan Area Tuning")
    st.caption(
        "Your tuned scan rectangles are built into the app as defaults. "
        "You can preview/adjust here. To persist changes after Streamlit sleep, "
        "download crop_config.json and commit it to the repo."
    )

    t_retailer = st.selectbox("Retailer", ["Home Depot", "Lowe's", "Tractor Supply"], key="tune_retailer")
    cfg = st.session_state["crop_cfg"]
    cur = extract_region_from_cfg(t_retailer, cfg)

    c1, c2 = st.columns(2)
    with c1:
        x0 = st.slider("Left (x0)", 0.0, 1.0, float(cur.get("x0", 0.0)), 0.01)
        y0 = st.slider("Bottom (y0)", 0.0, 1.0, float(cur.get("y0", 0.0)), 0.01)
    with c2:
        x1 = st.slider("Right (x1)", 0.0, 1.0, float(cur.get("x1", 1.0)), 0.01)
        y1 = st.slider("Top (y1)", 0.0, 1.0, float(cur.get("y1", 1.0)), 0.01)

    # normalize
    if x1 < x0:
        x0, x1 = x1, x0
    if y1 < y0:
        y0, y1 = y1, y0

    cfg.setdefault(t_retailer, {})
    cfg[t_retailer]["extract_region"] = {"x0": float(x0), "x1": float(x1), "y0": float(y0), "y1": float(y1)}
    st.session_state["crop_cfg"] = cfg

    pdf_prev = st.file_uploader("Upload a PDF to preview", type=["pdf"], key="tune_pdf")
    page_prev = st.number_input("Preview page number", min_value=1, value=1, step=1, key="tune_page")

    b1, b2, b3 = st.columns([1, 1, 1])
    with b1:
        show = st.button("Show scan area", key="tune_show")
    with b2:
        save = st.button("Save as default (writes crop_config.json)", key="tune_save")
    with b3:
        st.download_button(
            "Download config JSON",
            data=json.dumps(cfg, indent=2).encode("utf-8"),
            file_name="crop_config.json",
            mime="application/json",
            key="tune_dl",
        )

    if save:
        ok = save_crop_config(cfg)
        st.success("Saved crop_config.json") if ok else st.error("Could not save crop_config.json")

    if show:
        if pdf_prev is None:
            st.warning("Upload a PDF first.")
        else:
            try:
                img = render_scan_area_overlay(pdf_prev.getvalue(), int(page_prev) - 1, extract_region_from_cfg(t_retailer, cfg), zoom=2.0)
                st.image(img, caption=f"{t_retailer} scan area preview (page {int(page_prev)})", use_container_width=True)
            except Exception as e:
                st.error(f"Preview failed: {e}")


with tab_split:
    retailer = st.selectbox("Retailer", ["Home Depot", "Lowe's", "Tractor Supply"], index=1, key="retailer")
    confidence_threshold = st.slider("Confidence threshold", 0, 100, 70, 5)
    fallback_full = st.checkbox("Fallback to full-page text if scan area extraction is empty (not recommended)", value=False)

    with st.expander("Vendor Map (built in by default)"):
        st.write(f"Default map file: `{DEFAULT_MAPS[retailer]}`")
        map_upload = st.file_uploader("Optional: upload vendor map (xlsx)", type=["xlsx"], key="map_upload")
        st.caption("Note: the app ignores alpha-only keys shorter than 4 characters (ex: 'ORU') to prevent false positives.")
        if "_skipped_short_keys" in st.session_state:
            st.caption(f"Last run: skipped {st.session_state.get('_skipped_short_keys',0)} short keys from the map.")


    pdf_file = st.file_uploader("Upload PDF", type=["pdf"], key="pdf_upload")

    cA, cB = st.columns([1, 1])
    with cA:
        do_process = st.button("Process PDF", type="primary", disabled=(pdf_file is None))
    with cB:
        do_clear = st.button("Clear results")

    state_key = f"run_{retailer}"

    if do_clear:
        for k in list(st.session_state.keys()):
            if k.startswith("run_"):
                st.session_state.pop(k, None)
        st.rerun()

    if do_process and pdf_file is not None:
        pdf_bytes = pdf_file.getvalue()
        pdf_name = pdf_file.name

        try:
            df_map = load_vendor_map(retailer, uploaded_file=map_upload)
            lookup = build_lookup(df_map, retailer)
        except Exception as e:
            st.error(f"Vendor map error: {e}")
            st.stop()

        pages = extract_text_by_page_with_regions(pdf_bytes, retailer, st.session_state["crop_cfg"])

        vendor_list = sorted(set(lookup.values()))
        vendor_list_extended = vendor_list + ["REVIEW", "UNKNOWN", "MIXED/REVIEW"]

        rows = []
        for i, pobj in enumerate(pages):
            full = pobj.get("full", "")
            region = pobj.get("region", "")

            # Lowe's SOS tag: detect by scanning the FULL page text, then assign to vendor above.
            # IMPORTANT: for normal matching we still only use the scan-area (region) text.
            if retailer == "Lowe's" and is_sos_tag_page(full):
                if rows:
                    final_vendor = rows[-1]["Vendor"]
                    detected = rows[-1].get("Detected Vendor", final_vendor)
                    conf = max(int(rows[-1].get("Confidence %", 0)), 80)
                else:
                    final_vendor = "REVIEW"
                    detected = "SOS (no prior page)"
                    conf = 50

                rows.append({
                    "Page": i + 1,
                    "Vendor": final_vendor,
                    "Detected Vendor": detected,
                    "Confidence %": conf,
                    "Matched SKU/Model (first 15)": "",
                    "SOS Tag": True,
                })
                continue

            # For Home Depot and Tractor Supply: ONLY use scan-area text.
            # For Lowe's: also only use scan-area for matching (SOS detection handled above).
            scan_text = (region or "").strip()

            if not scan_text:
                if fallback_full:
                    scan_text = (full or "").strip()
                else:
                    rows.append({
                        "Page": i + 1,
                        "Vendor": "REVIEW",
                        "Detected Vendor": "NO_TEXT_IN_SCAN_AREA",
                        "Confidence %": 0,
                        "Matched SKU/Model (first 15)": "",
                        "SOS Tag": False,
                    })
                    continue

            vendor, matched, conf = match_vendor(scan_text, lookup)

            final_vendor = vendor
            if conf < confidence_threshold and vendor not in ("UNKNOWN", "MIXED/REVIEW"):
                final_vendor = "REVIEW"

            rows.append({
                "Page": i + 1,
                "Vendor": final_vendor,
                "Detected Vendor": vendor,
                "Confidence %": conf,
                "Matched SKU/Model (first 15)": ", ".join(matched) if matched else "",
                "SOS Tag": False,
            })

        st.session_state[state_key] = {
            "rows": rows,
            "pdf_bytes": pdf_bytes,
            "pdf_name": pdf_name,
            "vendor_list_extended": vendor_list_extended,
            "pages": pages,
        }
        st.success("Processed. Scroll down for downloads and overrides.")
        st.caption("Scan rule: Home Depot & Tractor Supply match ONLY within the scan box. Lowe’s matches within the scan box, but SOS detection uses the full page.")

    if state_key in st.session_state:
        run = st.session_state[state_key]
        rows = run["rows"]
        pdf_bytes = run["pdf_bytes"]
        pdf_name = run["pdf_name"]
        vendor_list_extended = run["vendor_list_extended"]
        pages = run.get("pages", [])

        df_report = pd.DataFrame(rows)
        st.subheader("Page → Vendor Report")
        st.dataframe(df_report, use_container_width=True, hide_index=True)

        page_vendor_rows = [
            {
                "PageIndex": int(r["Page"]) - 1,
                "Vendor": r["Vendor"],
                "SOS Tag": bool(r.get("SOS Tag", False)),
            }
            for r in rows
        ]
        vendor_pdfs = build_vendor_pdfs(pdf_bytes, page_vendor_rows, retailer, st.session_state["crop_cfg"])
        warehouse_pdf = build_warehouse_print_pdf(pdf_bytes, page_vendor_rows, WAREHOUSE_VENDORS)
        zip_bytes = build_zip(vendor_pdfs, base_name=pdf_name, warehouse_print_pdf=warehouse_pdf)
        download_base = re.sub(r"\.pdf$", "", pdf_name, flags=re.IGNORECASE)

        st.subheader("Downloads")
        st.download_button("Download Vendor ZIP", data=zip_bytes, file_name=f"{download_base}_VendorPdfs.zip", mime="application/zip")
        st.download_button("Download Report CSV", data=df_report.to_csv(index=False).encode("utf-8"), file_name=f"{download_base}_Report.csv", mime="text/csv")

        st.divider()
        st.subheader("Fix / Override Page Assignments (always available)")
        st.caption("Use bulk editing for flagged pages, or override any page manually.")

        st.markdown("### Pages flagged for review")
        mask = df_report["Vendor"].isin(["REVIEW", "UNKNOWN", "MIXED/REVIEW"])
        df_needs = df_report.loc[mask, ["Page", "Vendor", "Detected Vendor", "Confidence %", "Matched SKU/Model (first 15)", "SOS Tag"]].copy()

        if df_needs.empty:
            st.info("No pages are currently flagged for review.")
        else:
            edited = st.data_editor(
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
            if st.button("Apply bulk changes"):
                mp = {int(r["Page"]): r["Vendor"] for _, r in edited.iterrows()}
                for r in rows:
                    pg = int(r["Page"])
                    if pg in mp:
                        r["Vendor"] = mp[pg]
                        r["Confidence %"] = max(int(r.get("Confidence %", 0) or 0), 99)
                st.session_state[state_key]["rows"] = rows
                st.success("Applied bulk changes.")
                st.rerun()

        st.markdown("### Override any specific page")
        page_list = df_report["Page"].tolist()
        sel_page = st.selectbox("Page number", page_list, index=0, key=f"override_page_{retailer}")

        with st.expander("Debug: show extracted scan text for selected page", expanded=False):
            try:
                page_idx = int(sel_page) - 1
                scan_text = pages[page_idx]["region"] if pages and page_idx < len(pages) else ""
                st.write("Extracted region text (first 800 chars):")
                st.code((scan_text or "")[:800])
            except Exception as e:
                st.write(f"Debug unavailable: {e}")

        # Preview selected page with scan box
        try:
            rect = extract_region_from_cfg(retailer, st.session_state["crop_cfg"])
            preview_png = render_scan_area_overlay(pdf_bytes, int(sel_page) - 1, rect, zoom=2.0)
            st.image(preview_png, caption=f"Preview of page {int(sel_page)} with scan box (red)", use_container_width=True)
        except Exception:
            st.caption("Preview unavailable for this page.")

        cur_row = next((r for r in rows if int(r.get("Page", -1)) == int(sel_page)), None)
        if cur_row is not None:
            cc1, cc2, cc3 = st.columns([1, 1, 2])
            with cc1:
                st.text_input("Current vendor", value=str(cur_row.get("Vendor", "")), disabled=True)
            with cc2:
                st.text_input("Confidence %", value=str(cur_row.get("Confidence %", "")), disabled=True)
            with cc3:
                st.text_input("Matched", value=str(cur_row.get("Matched SKU/Model (first 15)", "")), disabled=True)

        new_vendor = st.selectbox("Change vendor to", vendor_list_extended, key=f"override_vendor_{retailer}")
        if st.button("Apply override"):
            for r in rows:
                if int(r["Page"]) == int(sel_page):
                    r["Vendor"] = new_vendor
                    r["Confidence %"] = max(int(r.get("Confidence %", 0) or 0), 99)
                    break
            st.session_state[state_key]["rows"] = rows
            st.success(f"Updated page {sel_page} → {new_vendor}")
            st.rerun()
