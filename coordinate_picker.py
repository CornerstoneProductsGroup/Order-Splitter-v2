#!/usr/bin/env python3
import argparse
import json
from pathlib import Path
import tkinter as tk
from tkinter import ttk, messagebox

import fitz
from PIL import Image, ImageTk


CROP_CONFIG_PATH = Path("crop_config.json")
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


def normalize_region(region: dict) -> dict:
    x0 = max(0.0, min(1.0, float(region.get("x0", 0.0))))
    x1 = max(0.0, min(1.0, float(region.get("x1", 1.0))))
    y0 = max(0.0, min(1.0, float(region.get("y0", 0.0))))
    y1 = max(0.0, min(1.0, float(region.get("y1", 1.0))))
    if x1 < x0:
        x0, x1 = x1, x0
    if y1 < y0:
        y0, y1 = y1, y0
    return {"x0": x0, "x1": x1, "y0": y0, "y1": y1}


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
            section.get("extract_region", default_region(retailer, "extract_region"))
        )

    if retailer == "Lowe's":
        merged["sos_output_crop"] = normalize_region(
            section.get("sos_output_crop", default_region(retailer, "sos_output_crop"))
        )
    elif retailer == "Tractor Supply":
        regs = section.get("redact_regions", CROP_CONFIG_DEFAULTS[retailer].get("redact_regions", []))
        merged["redact_regions"] = [normalize_region(r) for r in regs if isinstance(r, dict)]

    return merged


def load_config(path: Path) -> dict:
    if path.exists():
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                return {retailer: merge_retailer_config(retailer, data.get(retailer)) for retailer in CROP_CONFIG_DEFAULTS}
        except Exception:
            pass
    return {retailer: merge_retailer_config(retailer, None) for retailer in CROP_CONFIG_DEFAULTS}


class PickerApp:
    def __init__(self, pdf_path: Path, retailer: str):
        self.pdf_path = pdf_path
        self.doc = fitz.open(pdf_path)
        self.page_index = 0
        self.zoom = 1.5

        self.cfg_path = CROP_CONFIG_PATH
        self.cfg = load_config(self.cfg_path)

        self.root = tk.Tk()
        self.root.title("PDF Coordinate Picker")
        self.root.geometry("1280x900")

        self.retailer_var = tk.StringVar(value=retailer)
        self.mode_var = tk.StringVar(value="extract_region")
        self.status_var = tk.StringVar(value="Drag on page to create a rectangle.")

        self.image = None
        self.tk_image = None
        self.page_width = 1.0
        self.page_height = 1.0
        self.display_scale = 1.0
        self.offset_x = 0
        self.offset_y = 0

        self.drag_start = None
        self.drag_end = None
        self.drag_rect_id = None
        self.live_region = None

        self._build_ui()
        self._render_page()

    def _build_ui(self):
        toolbar = ttk.Frame(self.root, padding=8)
        toolbar.pack(fill=tk.X)

        ttk.Label(toolbar, text="Retailer:").pack(side=tk.LEFT)
        retailer_box = ttk.Combobox(
            toolbar,
            state="readonly",
            width=18,
            values=["Home Depot", "Lowe's", "Tractor Supply"],
            textvariable=self.retailer_var,
        )
        retailer_box.pack(side=tk.LEFT, padx=(4, 12))
        retailer_box.bind("<<ComboboxSelected>>", lambda _: self._render_page())

        ttk.Label(toolbar, text="Mode:").pack(side=tk.LEFT)
        mode_box = ttk.Combobox(
            toolbar,
            state="readonly",
            width=18,
            values=["extract_region", "sos_output_crop", "redact_regions"],
            textvariable=self.mode_var,
        )
        mode_box.pack(side=tk.LEFT, padx=(4, 12))
        mode_box.bind("<<ComboboxSelected>>", lambda _: self._render_page())

        ttk.Button(toolbar, text="Prev Page", command=self.prev_page).pack(side=tk.LEFT)
        ttk.Button(toolbar, text="Next Page", command=self.next_page).pack(side=tk.LEFT, padx=(4, 10))

        ttk.Button(toolbar, text="Save Drawn Region", command=self.save_drawn_region).pack(side=tk.LEFT)
        ttk.Button(toolbar, text="Undo Last Redaction", command=self.undo_last_redaction).pack(side=tk.LEFT, padx=(4, 0))
        ttk.Button(toolbar, text="Clear Mode Regions", command=self.clear_mode_regions).pack(side=tk.LEFT, padx=(4, 0))
        ttk.Button(toolbar, text="Save Config", command=self.save_config).pack(side=tk.LEFT, padx=(8, 0))

        self.page_label = ttk.Label(toolbar, text="Page 1/1")
        self.page_label.pack(side=tk.RIGHT)

        self.canvas = tk.Canvas(self.root, bg="#f2f2f2", highlightthickness=0)
        self.canvas.pack(fill=tk.BOTH, expand=True)
        self.canvas.bind("<Configure>", lambda _: self._render_page())
        self.canvas.bind("<ButtonPress-1>", self.on_mouse_down)
        self.canvas.bind("<B1-Motion>", self.on_mouse_drag)
        self.canvas.bind("<ButtonRelease-1>", self.on_mouse_up)

        status_bar = ttk.Label(self.root, textvariable=self.status_var, anchor="w", padding=8)
        status_bar.pack(fill=tk.X)

    def run(self):
        self.root.mainloop()

    def current_page(self):
        return self.doc.load_page(self.page_index)

    def _render_page(self):
        page = self.current_page()
        self.page_width = float(page.rect.width)
        self.page_height = float(page.rect.height)

        pix = page.get_pixmap(matrix=fitz.Matrix(self.zoom, self.zoom), alpha=False)
        image = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)

        canvas_w = max(1, self.canvas.winfo_width())
        canvas_h = max(1, self.canvas.winfo_height())

        self.display_scale = min(canvas_w / image.width, canvas_h / image.height, 1.0)
        draw_w = int(image.width * self.display_scale)
        draw_h = int(image.height * self.display_scale)
        if draw_w < 1 or draw_h < 1:
            return

        self.image = image.resize((draw_w, draw_h), Image.Resampling.LANCZOS)
        self.tk_image = ImageTk.PhotoImage(self.image)

        self.canvas.delete("all")
        self.offset_x = (canvas_w - draw_w) // 2
        self.offset_y = (canvas_h - draw_h) // 2
        self.canvas.create_image(self.offset_x, self.offset_y, image=self.tk_image, anchor=tk.NW)

        self._draw_saved_regions()

        if self.live_region:
            self._draw_region(self.live_region, outline="#0055cc", width=2, dash=(6, 4))

        self.page_label.config(text=f"Page {self.page_index + 1}/{self.doc.page_count}")

    def _draw_saved_regions(self):
        retailer = self.retailer_var.get()
        mode = self.mode_var.get()
        section = self.cfg.setdefault(retailer, {})

        if mode == "extract_region":
            reg = section.get("extract_region")
            if isinstance(reg, dict):
                self._draw_region(normalize_region(reg), outline="#007a3d", width=3)
        elif mode == "sos_output_crop":
            reg = section.get("sos_output_crop")
            if isinstance(reg, dict):
                self._draw_region(normalize_region(reg), outline="#6a3d9a", width=3)
        else:
            regs = section.get("redact_regions", [])
            if isinstance(regs, list):
                for idx, reg in enumerate(regs, start=1):
                    if isinstance(reg, dict):
                        self._draw_region(normalize_region(reg), outline="#b30000", width=2)
                        self._draw_region_label(normalize_region(reg), f"R{idx}")

    def _draw_region_label(self, region: dict, label: str):
        left, top, _, _ = self._region_to_canvas_rect(region)
        self.canvas.create_text(left + 14, top + 12, text=label, fill="#b30000", font=("TkDefaultFont", 10, "bold"))

    def _draw_region(self, region: dict, outline: str, width: int = 2, dash=None):
        left, top, right, bottom = self._region_to_canvas_rect(region)
        self.canvas.create_rectangle(left, top, right, bottom, outline=outline, width=width, dash=dash)

    def _region_to_canvas_rect(self, region: dict):
        left_pt = region["x0"] * self.page_width
        right_pt = region["x1"] * self.page_width
        top_pt = (1.0 - region["y1"]) * self.page_height
        bottom_pt = (1.0 - region["y0"]) * self.page_height

        left_px = left_pt * self.zoom
        right_px = right_pt * self.zoom
        top_px = top_pt * self.zoom
        bottom_px = bottom_pt * self.zoom

        left = self.offset_x + left_px * self.display_scale
        right = self.offset_x + right_px * self.display_scale
        top = self.offset_y + top_px * self.display_scale
        bottom = self.offset_y + bottom_px * self.display_scale
        return left, top, right, bottom

    def _canvas_to_region(self, x1, y1, x2, y2):
        x1_img = (x1 - self.offset_x) / self.display_scale
        y1_img = (y1 - self.offset_y) / self.display_scale
        x2_img = (x2 - self.offset_x) / self.display_scale
        y2_img = (y2 - self.offset_y) / self.display_scale

        page_x1 = x1_img / self.zoom
        page_y1 = y1_img / self.zoom
        page_x2 = x2_img / self.zoom
        page_y2 = y2_img / self.zoom

        page_x1 = max(0.0, min(self.page_width, page_x1))
        page_x2 = max(0.0, min(self.page_width, page_x2))
        page_y1 = max(0.0, min(self.page_height, page_y1))
        page_y2 = max(0.0, min(self.page_height, page_y2))

        left = min(page_x1, page_x2)
        right = max(page_x1, page_x2)
        top = min(page_y1, page_y2)
        bottom = max(page_y1, page_y2)

        if right - left < 2 or bottom - top < 2:
            return None

        x0 = left / self.page_width
        x1n = right / self.page_width

        y1n = 1.0 - (top / self.page_height)
        y0n = 1.0 - (bottom / self.page_height)

        return normalize_region({"x0": x0, "x1": x1n, "y0": y0n, "y1": y1n})

    def on_mouse_down(self, event):
        self.drag_start = (event.x, event.y)
        self.drag_end = (event.x, event.y)
        if self.drag_rect_id:
            self.canvas.delete(self.drag_rect_id)
            self.drag_rect_id = None

    def on_mouse_drag(self, event):
        if not self.drag_start:
            return
        self.drag_end = (event.x, event.y)

        if self.drag_rect_id:
            self.canvas.delete(self.drag_rect_id)

        x1, y1 = self.drag_start
        x2, y2 = self.drag_end
        self.drag_rect_id = self.canvas.create_rectangle(x1, y1, x2, y2, outline="#0055cc", width=2, dash=(6, 4))

    def on_mouse_up(self, event):
        if not self.drag_start:
            return

        self.drag_end = (event.x, event.y)
        region = self._canvas_to_region(self.drag_start[0], self.drag_start[1], self.drag_end[0], self.drag_end[1])
        self.drag_start = None
        self.drag_end = None

        if region is None:
            self.live_region = None
            self.status_var.set("Selection too small. Drag a larger box.")
            self._render_page()
            return

        self.live_region = region
        self.status_var.set(f"Selected region: {json.dumps(region)}")
        self._render_page()

    def save_drawn_region(self):
        if not self.live_region:
            messagebox.showinfo("No selection", "Draw a rectangle first.")
            return

        retailer = self.retailer_var.get()
        mode = self.mode_var.get()
        section = self.cfg.setdefault(retailer, {})

        if mode == "extract_region":
            section["extract_region"] = dict(self.live_region)
        elif mode == "sos_output_crop":
            section["sos_output_crop"] = dict(self.live_region)
        else:
            regs = section.setdefault("redact_regions", [])
            if not isinstance(regs, list):
                regs = []
                section["redact_regions"] = regs
            regs.append(dict(self.live_region))

        self.status_var.set(f"Saved to {retailer}.{mode}: {json.dumps(self.live_region)}")
        self._render_page()

    def clear_mode_regions(self):
        retailer = self.retailer_var.get()
        mode = self.mode_var.get()
        section = self.cfg.setdefault(retailer, {})

        if mode == "extract_region":
            section.pop("extract_region", None)
        elif mode == "sos_output_crop":
            section.pop("sos_output_crop", None)
        else:
            section["redact_regions"] = []

        self.status_var.set(f"Cleared {retailer}.{mode}")
        self._render_page()

    def undo_last_redaction(self):
        retailer = self.retailer_var.get()
        section = self.cfg.setdefault(retailer, {})
        regs = section.get("redact_regions", [])
        if isinstance(regs, list) and regs:
            regs.pop()
            self.status_var.set("Removed last redaction region.")
        else:
            self.status_var.set("No redaction regions to remove.")
        self._render_page()

    def save_config(self):
        try:
            self.cfg_path.write_text(json.dumps(self.cfg, indent=2), encoding="utf-8")
            self.status_var.set(f"Saved config to {self.cfg_path}")
            messagebox.showinfo("Saved", f"Config saved to {self.cfg_path}")
        except Exception as exc:
            messagebox.showerror("Save failed", str(exc))

    def prev_page(self):
        self.page_index = (self.page_index - 1) % self.doc.page_count
        self._render_page()

    def next_page(self):
        self.page_index = (self.page_index + 1) % self.doc.page_count
        self._render_page()


def main():
    parser = argparse.ArgumentParser(description="Interactive PDF coordinate picker for crop/redaction regions.")
    parser.add_argument("pdf", type=Path, help="Path to sample PDF")
    parser.add_argument(
        "--retailer",
        choices=["Home Depot", "Lowe's", "Tractor Supply"],
        default="Lowe's",
        help="Retailer section to edit first",
    )
    args = parser.parse_args()

    if not args.pdf.exists():
        raise FileNotFoundError(f"PDF not found: {args.pdf}")

    app = PickerApp(args.pdf, args.retailer)
    app.run()


if __name__ == "__main__":
    main()
