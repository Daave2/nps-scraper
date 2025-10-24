#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Retail Performance Dashboard → Daily Summary (OCR-first) → Google Chat

Key updates in this version:
- Auto-scales ROI coordinates to the actual screenshot size (so boxes line up even if
  the runtime zoom or DPR changes).
- Forces viewport 1365×768 with device_scale_factor=1, but still robust via auto-scale.
- Logs devicePixelRatio, viewport size, and screenshot size for debugging.
- Saves per-ROI crop PNGs and a green overlay PNG.
"""

import os
import re
import csv
import json
import time
import logging
import configparser
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union

import requests
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

# OCR deps (no pandas needed)
try:
    from PIL import Image, ImageDraw, ImageStat
    from io import BytesIO
    import pytesseract
    OCR_AVAILABLE = True
except Exception:
    OCR_AVAILABLE = False

# ──────────────────────────────────────────────────────────────────────────────
# Paths / constants
# ──────────────────────────────────────────────────────────────────────────────
BASE_DIR        = Path(__file__).resolve().parent
AUTH_STATE_PATH = BASE_DIR / "auth_state.json"
LOG_FILE_PATH   = BASE_DIR / "scrape_daily.log"
DAILY_LOG_CSV   = BASE_DIR / "daily_report_log.csv"
SCREENS_DIR     = BASE_DIR / "screens"
ROI_MAP_PATH    = Path(os.getenv("ROI_MAP_FILE", BASE_DIR / "roi_map.json"))

DASHBOARD_URL = (
    "https://lookerstudio.google.com/embed/u/0/reporting/"
    "d93a03c7-25dc-439d-abaa-dd2f3780daa5/page/BLfDE"
    "?params=%7B%22f20f0n9kld%22:%22include%25EE%2580%25803%25EE%2580%2580T%22%7D"
)

# Base design size used when you drew the ROI boxes
BASE_W, BASE_H = 1365, 768

# Preferred viewport; boxes will still align even if the real screenshot differs
VIEWPORT = {"width": BASE_W, "height": BASE_H}
DEVICE_SCALE_FACTOR = 1

# ──────────────────────────────────────────────────────────────────────────────
# Logging
# ──────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler(LOG_FILE_PATH)],
)
log = logging.getLogger("daily")
log.addHandler(logging.StreamHandler())

# ──────────────────────────────────────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────────────────────────────────────
config = configparser.ConfigParser()
config.read(BASE_DIR / "config.ini")

MAIN_WEBHOOK   = config["DEFAULT"].get("DAILY_WEBHOOK") or config["DEFAULT"].get("MAIN_WEBHOOK", os.getenv("MAIN_WEBHOOK", ""))
ALERT_WEBHOOK  = config["DEFAULT"].get("ALERT_WEBHOOK",  os.getenv("ALERT_WEBHOOK", ""))
CI_RUN_URL     = os.getenv("CI_RUN_URL", "")

# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────
def _post_with_backoff(url: str, payload: dict) -> bool:
    backoff, max_backoff = 2.0, 30.0
    while True:
        try:
            r = requests.post(url, json=payload, timeout=25)
            if r.status_code == 200:
                return True
            if r.status_code == 429:
                delay = min(float(r.headers.get("Retry-After") or backoff), max_backoff)
                log.error(f"429 from webhook — sleeping {delay:.1f}s")
                time.sleep(delay); backoff = min(backoff * 1.7, max_backoff); continue
            log.error(f"Webhook error {r.status_code}: {r.text[:300]}")
            return False
        except Exception as e:
            log.error(f"Webhook exception: {e}")
            time.sleep(backoff); backoff = min(backoff * 1.7, max_backoff)

def alert(lines: List[str]):
    if not ALERT_WEBHOOK or "chat.googleapis.com" not in ALERT_WEBHOOK:
        log.warning("No valid ALERT_WEBHOOK configured.")
        return
    if CI_RUN_URL:
        lines.append(f"• CI run: {CI_RUN_URL}")
    _post_with_backoff(ALERT_WEBHOOK, {"text": "\n".join(lines)})

def save_bytes(path: Path, data: bytes):
    try:
        SCREENS_DIR.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
        log.info(f"Saved {path.name}")
    except Exception:
        pass

def save_text(path: Path, text: str):
    try:
        SCREENS_DIR.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
        log.info(f"Saved {path.name}")
    except Exception:
        pass

# ──────────────────────────────────────────────────────────────────────────────
# Browser automation
# ──────────────────────────────────────────────────────────────────────────────
def click_this_week(page):
    for kind, value in [("role", ("button", "This Week")), ("text", "This Week"), ("text", "This week")]:
        try:
            btn = page.get_by_role(*value) if kind == "role" else page.get_by_text(value)
            if btn.count():
                btn.first.click(timeout=2500)
                page.wait_for_timeout(900)
                return True
        except Exception:
            continue
    return False

def click_proceed_overlays(page) -> int:
    clicked = 0
    for fr in page.frames:
        try:
            btn = fr.get_by_text("PROCEED", exact=True)
            for i in range(btn.count()):
                try:
                    btn.nth(i).click(timeout=1500)
                    clicked += 1
                    fr.wait_for_timeout(350)
                except Exception:
                    continue
        except Exception:
            continue
    if clicked:
        log.info(f"Clicked {clicked} 'PROCEED' overlay(s). Waiting for render…")
        page.wait_for_timeout(1500)
    return clicked

def open_and_prepare(page) -> bool:
    log.info("Opening Retail Performance Dashboard…")
    try:
        page.goto(DASHBOARD_URL, wait_until="domcontentloaded", timeout=120_000)
    except PlaywrightTimeoutError:
        log.error("Timeout loading dashboard.")
        return False

    if "accounts.google.com" in page.url:
        log.warning("Redirected to login — auth state missing/invalid.")
        return False

    log.info("Waiting 12s for dynamic content…")
    page.wait_for_timeout(12_000)

    click_this_week(page)
    click_proceed_overlays(page)

    try:
        body_text = page.inner_text("body")
    except Exception:
        body_text = ""
    if "You are about to interact with a community visualisation" in body_text:
        log.info("Placeholder detected — retrying PROCEED and waiting longer.")
        click_proceed_overlays(page)
        page.wait_for_timeout(2000)

    return True

# ──────────────────────────────────────────────────────────────────────────────
# Screenshot + OCR
# ──────────────────────────────────────────────────────────────────────────────
def screenshot_viewport(page) -> Optional[Image.Image]:
    try:
        # Log DPR & viewport info
        dpr = page.evaluate("() => window.devicePixelRatio")
        vp  = page.viewport_size
        log.info(f"Runtime DPR={dpr}, viewport={vp}")

        img_bytes = page.screenshot(full_page=False, type="png")
        ts = int(time.time())
        path = SCREENS_DIR / f"{ts}_viewport.png"
        save_bytes(path, img_bytes)
        img = Image.open(BytesIO(img_bytes))
        log.info(f"Screenshot size: {img.size}")
        return img
    except Exception as e:
        log.error(f"Viewport screenshot failed: {e}")
        return None

def ocr_fullpage(img: Image.Image) -> Tuple[Optional[Image.Image], List[dict]]:
    if not OCR_AVAILABLE or img is None:
        return img, []
    try:
        W, H = img.size
        if max(W, H) < 1400:
            scale = 1400 / max(W, H)
            img = img.resize((int(W*scale), int(H*scale)))

        data = pytesseract.image_to_data(img, output_type=pytesseract.Output.DICT)
        words: List[dict] = []
        parts = []
        for i in range(len(data["text"])):
            t = (data["text"][i] or "").strip()
            if not t:
                continue
            words.append({
                "text": t,
                "left": int(data["left"][i]),
                "top": int(data["top"][i]),
                "width": int(data["width"][i]),
                "height": int(data["height"][i]),
                "conf": float(data["conf"][i]),
            })
            parts.append(t)
        ts = int(time.time())
        save_text(SCREENS_DIR / f"{ts}_ocr_text.txt", " ".join(parts))
        return img, words
    except Exception as e:
        log.error(f"OCR failed: {e}")
        return img, []

# ──────────────────────────────────────────────────────────────────────────────
# OCR by labels
# ──────────────────────────────────────────────────────────────────────────────
NumPat = re.compile(r"[£]?-?\d+(?:\.\d+)?(?:[KMB]|%)?|\b\d{2}:\d{2}\b", re.I)

def nearest_number(words: List[dict], label: str, window: int = 180) -> str:
    labs = [w for w in words if w["text"].lower() == label.lower()]
    if not labs:
        labs = [w for w in words if label.lower() in w["text"].lower()]
    if not labs:
        return "—"
    lx, ly = labs[0]["left"], labs[0]["top"]
    best = None; bestd = 1e9
    for w in words:
        if w is labs[0]:
            continue
        t = w["text"]
        if not NumPat.fullmatch(t):
            continue
        dx = abs(w["left"] - lx); dy = abs(w["top"] - ly)
        d = (dx*dx + dy*dy) ** 0.5
        if d < bestd and dx < window and dy < window:
            best, bestd = t, d
    return best if best else "—"

def build_metrics_from_ocr_labels(words: List[dict]) -> Dict[str, str]:
    m: Dict[str,str] = {}

    m["page_timestamp"] = "—"
    m["period_range"]   = "—"

    m["sales_total"]     = "—"
    m["sales_lfl"]       = "—"
    m["sales_vs_target"] = "—"

    m["supermarket_nps"]     = nearest_number(words, "Supermarket")
    m["colleague_happiness"] = nearest_number(words, "Colleague")
    m["home_delivery_nps"]   = nearest_number(words, "Home")
    m["cafe_nps"]            = nearest_number(words, "Cafe")
    m["click_collect_nps"]   = nearest_number(words, "Collect")
    m["customer_toilet_nps"] = nearest_number(words, "Toilet")

    m["sco_utilisation"]          = nearest_number(words, "Utilisation")
    m["efficiency"]               = nearest_number(words, "Efficiency")
    m["scan_rate"]                = nearest_number(words, "Scan")
    m["scan_vs_target"]           = "—"
    m["interventions"]            = nearest_number(words, "Interventions")
    m["interventions_vs_target"]  = "—"
    m["mainbank_closed"]          = nearest_number(words, "Mainbank")
    m["mainbank_vs_target"]       = "—"

    m["availability_pct"]   = nearest_number(words, "Availability")
    m["despatched_on_time"] = nearest_number(words, "Despatched")
    m["delivered_on_time"]  = nearest_number(words, "Delivered")
    m["cc_avg_wait"]        = nearest_number(words, "wait")

    m["waste_total"]        = "—"
    m["markdowns_total"]    = "—"
    m["wm_total"]           = "—"
    m["wm_delta"]           = "—"
    m["wm_delta_pct"]       = "—"

    m["payroll_outturn"]     = nearest_number(words, "Payroll")
    m["absence_outturn"]     = nearest_number(words, "Absence")
    m["productive_outturn"]  = nearest_number(words, "Productive")
    m["holiday_outturn"]     = nearest_number(words, "Holiday")
    m["current_base_cost"]   = nearest_number(words, "Cost")

    m["moa"]                  = nearest_number(words, "Adjustments")
    m["waste_validation"]     = nearest_number(words, "Validation")
    m["unrecorded_waste_pct"] = nearest_number(words, "Unrecorded")
    m["shrink_vs_budget_pct"] = nearest_number(words, "Budget")

    m["swipe_rate"]     = nearest_number(words, "Swipe")
    m["swipes_wow_pct"] = nearest_number(words, "WOW")
    m["new_customers"]  = nearest_number(words, "Customers")
    m["swipes_yoy_pct"] = nearest_number(words, "YOY")

    m["data_provided"]   = nearest_number(words, "Provided")
    m["trusted_data"]    = nearest_number(words, "Trusted")
    m["complaints_key"]  = nearest_number(words, "Complaints")
    m["my_reports"]      = nearest_number(words, "Reports")
    m["weekly_activity"] = nearest_number(words, "Activity")

    return m

# ──────────────────────────────────────────────────────────────────────────────
# ROI map + scaling
# ──────────────────────────────────────────────────────────────────────────────
DEFAULT_ROI_MAP: Dict[str, Tuple[float,float,float,float]] = {
    # Gauges
    "colleague_happiness": (0.23, 0.20, 0.09, 0.12),
    "supermarket_nps":     (0.38, 0.20, 0.09, 0.12),
    "cafe_nps":            (0.53, 0.20, 0.09, 0.12),
    "click_collect_nps":   (0.67, 0.20, 0.09, 0.12),
    "home_delivery_nps":   (0.81, 0.20, 0.09, 0.12),
    "customer_toilet_nps": (0.95, 0.20, 0.09, 0.12),

    # Waste & Markdowns — TOTAL row
    "waste_total":     (0.10, 0.42, 0.06, 0.03),
    "markdowns_total": (0.17, 0.42, 0.06, 0.03),
    "wm_total":        (0.24, 0.42, 0.06, 0.03),
    "wm_delta":        (0.31, 0.42, 0.06, 0.03),
    "wm_delta_pct":    (0.38, 0.42, 0.06, 0.03),

    # Front End Service
    "sco_utilisation": (0.68, 0.59, 0.06, 0.05),
    "efficiency":      (0.95, 0.60, 0.07, 0.10),
    "scan_rate":       (0.68, 0.65, 0.06, 0.04),
    "interventions":   (0.81, 0.59, 0.06, 0.05),
    "mainbank_closed": (0.81, 0.65, 0.06, 0.04),

    # Online
    "availability_pct":   (0.45, 0.59, 0.06, 0.07),
    "despatched_on_time": (0.52, 0.59, 0.08, 0.05),
    "delivered_on_time":  (0.59, 0.59, 0.08, 0.05),
    "cc_avg_wait":        (0.62, 0.65, 0.06, 0.07),

    # Payroll
    "payroll_outturn":    (0.53, 0.46, 0.08, 0.10),
    "absence_outturn":    (0.64, 0.45, 0.05, 0.04),
    "productive_outturn": (0.64, 0.50, 0.05, 0.04),
    "holiday_outturn":    (0.73, 0.45, 0.05, 0.04),
    "current_base_cost":  (0.73, 0.50, 0.05, 0.04),

    # Card Engagement
    "swipe_rate":     (0.83, 0.45, 0.05, 0.04),
    "swipes_wow_pct": (0.90, 0.45, 0.05, 0.04),
    "new_customers":  (0.83, 0.50, 0.05, 0.04),
    "swipes_yoy_pct": (0.90, 0.50, 0.05, 0.04),

    # Production Planning + misc
    "data_provided":   (0.07, 0.60, 0.06, 0.06),
    "trusted_data":    (0.07, 0.66, 0.06, 0.06),
    "my_reports":      (0.34, 0.46, 0.05, 0.06),
    "weekly_activity": (0.44, 0.48, 0.06, 0.06),
    "complaints_key":  (0.96, 0.27, 0.05, 0.06),
}

def _normalize_rect(value: Union[list, tuple, dict]) -> Optional[Tuple[float,float,float,float]]:
    if isinstance(value, (list, tuple)) and len(value) == 4:
        rect = tuple(float(v) for v in value)
    elif isinstance(value, dict):
        if "rect" in value and isinstance(value["rect"], (list, tuple)) and len(value["rect"]) == 4:
            rect = tuple(float(v) for v in value["rect"])
        elif all(k in value for k in ("x","y","w","h")):
            rect = (float(value["x"]), float(value["y"]), float(value["w"]), float(value["h"]))
        elif all(k in value for k in ("left","top","width","height")):
            rect = (float(value["left"]), float(value["top"]), float(value["width"]), float(value["height"]))
        else:
            return None
    else:
        return None
    x,y,w,h = rect
    if not (0 <= x <= 1 and 0 <= y <= 1 and 0 <= w <= 1 and 0 <= h <= 1):
        return None
    return (x,y,w,h)

def load_roi_map() -> Dict[str, Tuple[float,float,float,float]]:
    merged = dict(DEFAULT_ROI_MAP)
    if ROI_MAP_PATH.exists():
        try:
            data = json.loads(ROI_MAP_PATH.read_text(encoding="utf-8"))
            applied = 0
            for k, v in data.items():
                rect = _normalize_rect(v)
                if rect:
                    merged[k] = rect
                    applied += 1
            log.info(f"Loaded ROI overrides from {ROI_MAP_PATH.name}: {applied} entrie(s).")
        except Exception as e:
            log.error(f"Failed to read {ROI_MAP_PATH.name}: {e}. Using defaults.")
    else:
        log.info("No roi_map.json found — using default ROI map.")
    return merged

def scale_roi_map(roi_map: Dict[str, Tuple[float,float,float,float]], shot_w: int, shot_h: int) -> Dict[str, Tuple[float,float,float,float]]:
    """Return a map expressed as absolute pixels scaled to the actual screenshot size."""
    sx = shot_w / BASE_W if BASE_W else 1.0
    sy = shot_h / BASE_H if BASE_H else 1.0
    scaled = {}
    for k, (x,y,w,h) in roi_map.items():
        # convert normalized → pixel on base → scale to actual → back to normalized *for this image*
        # but we will crop using pixel coords directly, so store pixels
        scaled[k] = (int(x*shot_w), int(y*shot_h), int(w*shot_w), int(h*shot_h))
    log.info(f"ROI auto-scale factors: sx={sx:.3f}, sy={sy:.3f} (shot {shot_w}×{shot_h}, base {BASE_W}×{BASE_H})")
    return scaled

ROI_MAP_NORM = load_roi_map()

# ──────────────────────────────────────────────────────────────────────────────
# ROI helpers
# ──────────────────────────────────────────────────────────────────────────────
def _crop_roi_pixels(img: Image.Image, rect_px: Tuple[int,int,int,int]) -> Image.Image:
    x,y,w,h = rect_px
    box = (x, y, x+w, y+h)
    return img.crop(box)

def _looks_black(img: Image.Image) -> bool:
    try:
        g = img.convert("L")
        stat = ImageStat.Stat(g)
        return stat.mean[0] < 3 and stat.var[0] < 5
    except Exception:
        return False

def _ocr_from_image(img: Image.Image, want_time=False, allow_percent=True) -> str:
    if not OCR_AVAILABLE:
        return "—"
    try:
        w,h = img.size
        if max(w,h) < 240:
            img = img.resize((int(w*2.0), int(h*2.0)))
        g = img.convert("L")
        txt = pytesseract.image_to_string(g, config="--psm 7")
        if want_time:
            m = re.search(r"\b\d{2}:\d{2}\b", txt)
            if m: return m.group(0)
        m = re.search(r"[£]?-?\d+(?:\.\d+)?[KMB]?", txt, re.I)
        if m and m.group(0): return m.group(0)
        if allow_percent:
            m = re.search(r"-?\d+(?:\.\d+)?%", txt)
            if m: return m.group(0)
        m = re.search(r"\b-?\d{1,3}\b", txt)
        if m: return m.group(0)
    except Exception:
        pass
    return "—"

def draw_roi_overlay(img: Image.Image, outfile: Path, roi_px_map: Dict[str, Tuple[int,int,int,int]]):
    try:
        dbg = img.copy().convert("RGB")
        draw = ImageDraw.Draw(dbg)
        for key, (x,y,w,h) in roi_px_map.items():
            box = (x, y, x+w, y+h)
            draw.rectangle(box, outline=(0,255,0), width=2)
            draw.text((x+3, y+3), key, fill=(0,255,0))
        dbg.save(outfile)
        log.info(f"ROI overlay saved → {outfile.name}")
    except Exception:
        pass

def fill_from_roi(metrics: Dict[str,str], img: Optional[Image.Image], roi_norm_map: Dict[str, Tuple[float,float,float,float]]):
    if img is None:
        return
    W,H = img.size
    roi_px_map = scale_roi_map(roi_norm_map, W, H)

    ts = int(time.time())
    any_filled = False
    for key, rect_px in roi_px_map.items():
        if metrics.get(key) and metrics[key] != "—":
            continue
        crop = _crop_roi_pixels(img, rect_px)
        try:
            crop.save(SCREENS_DIR / f"{ts}_{key}_crop.png")
        except Exception:
            pass
        black_flag = _looks_black(crop)
        val = _ocr_from_image(crop, want_time=(key=="cc_avg_wait"), allow_percent=not key.endswith("_nps"))
        log.info(f"ROI {key}: px={rect_px} black={black_flag} -> OCR='{val}'")
        if val and val != "—":
            metrics[key] = val
            any_filled = True

    draw_roi_overlay(img, SCREENS_DIR / f"{ts}_roi_overlay.png", roi_px_map)
    if any_filled:
        log.info("Filled some missing values from ROI OCR.")
    else:
        log.warning("ROI OCR did not fill any values (check crops/overlay & mapping).")

# ──────────────────────────────────────────────────────────────────────────────
# Card + CSV
# ──────────────────────────────────────────────────────────────────────────────
def kv(label: str, val: str) -> dict:
    return {"decoratedText": {"topLabel": label, "text": (val or "—")}}

def title_widget(text: str) -> dict:
    return {"textParagraph": {"text": f"<b>{text}</b>"}}

def build_chat_card(metrics: Dict[str, str]) -> dict:
    header = {
        "title": "📊 Retail Daily Summary (OCR)",
        "subtitle": (metrics.get("store_line") or "").replace("\n", "  "),
    }
    sections = [
        {"widgets": [kv("Report Time", metrics.get("page_timestamp","—")),
                     kv("Period",      metrics.get("period_range","—"))]},
        {"widgets": [title_widget("Sales & NPS"),
                     kv("Sales Total", metrics.get("sales_total","—")),
                     kv("LFL",         metrics.get("sales_lfl","—")),
                     kv("vs Target",   metrics.get("sales_vs_target","—")),
                     kv("Supermarket NPS",     metrics.get("supermarket_nps","—")),
                     kv("Colleague Happiness", metrics.get("colleague_happiness","—")),
                     kv("Home Delivery NPS",   metrics.get("home_delivery_nps","—")),
                     kv("Cafe NPS",            metrics.get("cafe_nps","—")),
                     kv("Click & Collect NPS", metrics.get("click_collect_nps","—")),
                     kv("Customer Toilet NPS", metrics.get("customer_toilet_nps","—"))]},
        {"widgets": [title_widget("Front End Service"),
                     kv("SCO Utilisation", metrics.get("sco_utilisation","—")),
                     kv("Efficiency",      metrics.get("efficiency","—")),
                     kv("Scan Rate",       f"{metrics.get('scan_rate','—')} (vs {metrics.get('scan_vs_target','—')})"),
                     kv("Interventions",   f"{metrics.get('interventions','—')} (vs {metrics.get('interventions_vs_target','—')})"),
                     kv("Mainbank Closed", f"{metrics.get('mainbank_closed','—')} (vs {metrics.get('mainbank_vs_target','—')})")]},
        {"widgets": [title_widget("Online"),
                     kv("Availability",              metrics.get("availability_pct","—")),
                     kv("Despatched on Time",        metrics.get("despatched_on_time","—")),
                     kv("Delivered on Time",         metrics.get("delivered_on_time","—")),
                     kv("Click & Collect Avg Wait",  metrics.get("cc_avg_wait","—"))]},
        {"widgets": [title_widget("Waste & Markdowns (Total)"),
                     kv("Waste",     metrics.get("waste_total","—")),
                     kv("Markdowns", metrics.get("markdowns_total","—")),
                     kv("Total",     metrics.get("wm_total","—")),
                     kv("+/−",       metrics.get("wm_delta","—")),
                     kv("+/− %",     metrics.get("wm_delta_pct","—"))]},
        {"widgets": [title_widget("Payroll"),
                     kv("Payroll Outturn",    metrics.get("payroll_outturn","—")),
                     kv("Absence Outturn",    metrics.get("absence_outturn","—")),
                     kv("Productive Outturn", metrics.get("productive_outturn","—")),
                     kv("Holiday Outturn",    metrics.get("holiday_outturn","—")),
                     kv("Current Base Cost",  metrics.get("current_base_cost","—"))]},
        {"widgets": [title_widget("Shrink"),
                     kv("Morrisons Order Adjustments", metrics.get("moa","—")),
                     kv("Waste Validation",            metrics.get("waste_validation","—")),
                     kv("Unrecorded Waste %",          metrics.get("unrecorded_waste_pct","—")),
                     kv("Shrink vs Budget %",          metrics.get("shrink_vs_budget_pct","—"))]},
        {"widgets": [title_widget("Card Engagement & Misc"),
                     kv("Swipe Rate",      metrics.get("swipe_rate","—")),
                     kv("Swipes WOW %",    metrics.get("swipes_wow_pct","—")),
                     kv("New Customers",   metrics.get("new_customers","—")),
                     kv("Swipes YOY %",    metrics.get("swipes_yoy_pct","—")),
                     kv("Key Complaints",  metrics.get("complaints_key","—")),
                     kv("Data Provided",   metrics.get("data_provided","—")),
                     kv("Trusted Data",    metrics.get("trusted_data","—")),
                     kv("My Reports",      metrics.get("my_reports","—")),
                     kv("Weekly Activity %",metrics.get("weekly_activity","—"))]},
    ]
    return {"cardsV2": [{"cardId": f"daily_{int(time.time())}", "card": {"header": header, "sections": sections}}]}

def send_daily_card(metrics: Dict[str, str]) -> bool:
    if not MAIN_WEBHOOK or "chat.googleapis.com" not in MAIN_WEBHOOK:
        log.error("MAIN_WEBHOOK/DAILY_WEBHOOK missing or invalid — cannot send daily report.")
        return False
    return _post_with_backoff(MAIN_WEBHOOK, build_chat_card(metrics))

CSV_HEADERS = [
    "page_timestamp","period_range","store_line",
    "sales_total","sales_lfl","sales_vs_target",
    "supermarket_nps","colleague_happiness","home_delivery_nps","cafe_nps","click_collect_nps","customer_toilet_nps",
    "sco_utilisation","efficiency","scan_rate","scan_vs_target","interventions","interventions_vs_target",
    "mainbank_closed","mainbank_vs_target",
    "availability_pct","despatched_on_time","delivered_on_time","cc_avg_wait",
    "waste_total","markdowns_total","wm_total","wm_delta","wm_delta_pct",
    "moa","waste_validation","unrecorded_waste_pct","shrink_vs_budget_pct",
    "payroll_outturn","absence_outturn","productive_outturn","holiday_outturn","current_base_cost",
    "swipe_rate","swipes_wow_pct","new_customers","swipes_yoy_pct",
    "complaints_key","data_provided","trusted_data","my_reports","weekly_activity",
]

def write_csv_row(metrics: Dict[str,str]):
    write_header = not DAILY_LOG_CSV.exists() or DAILY_LOG_CSV.stat().st_size == 0
    with open(DAILY_LOG_CSV, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if write_header:
            w.writerow(CSV_HEADERS)
        w.writerow([metrics.get(h, "—") for h in CSV_HEADERS])
    log.info(f"Appended daily metrics row to {DAILY_LOG_CSV.name}")

# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────
def run_daily_scrape():
    if not AUTH_STATE_PATH.exists():
        alert(["⚠️ Daily dashboard scrape needs login. Run `python scrape.py now` once to save auth_state.json."])
        log.error("auth_state.json not found.")
        return

    with sync_playwright() as p:
        browser = context = page = None
        img = None
        words: List[dict] = []
        try:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                storage_state=str(AUTH_STATE_PATH),
                viewport=VIEWPORT,
                device_scale_factor=DEVICE_SCALE_FACTOR,
                is_mobile=False,
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
                screen=VIEWPORT,  # align "screen" and "viewport"
            )
            page = context.new_page()
            page.add_init_script("document.addEventListener('DOMContentLoaded', () => { document.body.style.zoom='1'; });")

            if not open_and_prepare(page):
                alert(["⚠️ Daily scrape blocked by login or load failure — please re-login."])
                return

            img = screenshot_viewport(page)
            img, words = ocr_fullpage(img)

            metrics = build_metrics_from_ocr_labels(words)

            # Long context fields via DOM text
            try:
                body = page.inner_text("body")
            except Exception:
                body = ""
            m = re.search(r"\b(\d{1,2}\s+[A-Za-z]{3}\s+\d{4},\s*\d{2}:\d{2}:\d{2})\b", body)
            if m: metrics["page_timestamp"] = m.group(1)
            m = re.search(r"The data on this report is from:\s*([^\n]+)", body)
            metrics["period_range"] = m.group(1).strip() if m else "—"
            m = re.search(r"([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}).*?\|\s*([^\|]+?)\s*\|\s*(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})",
                          body, flags=re.S)
            metrics["store_line"] = m.group(0).strip() if m else ""

            # ROI fallback (auto-scaled to screenshot)
            fill_from_roi(metrics, img, ROI_MAP_NORM)

        finally:
            try:
                if context: context.close()
            except Exception:
                pass
            try:
                if browser: browser.close()
            except Exception:
                pass

    ok = send_daily_card(metrics)
    log.info("Daily card send → %s", "OK" if ok else "FAIL")
    write_csv_row(metrics)

if __name__ == "__main__":
    run_daily_scrape()
