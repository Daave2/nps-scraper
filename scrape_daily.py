#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Retail Performance Dashboard → Daily Summary (layout-by-lines + ROI OCR) → Google Chat
v2 (layout tuned to 1761317314_daily_text.txt, ROI map from 1761330355 overlay)

Strategy:
  - Parse from text layout first (body text dumped line-by-line)
  - Use precise regex mappings for each metric
  - Warn when metrics missing
  - Fill NPS gauges only from ROI OCR (using roi_map.json)
"""

import os
import re
import csv
import json
import time
import logging
import configparser
from io import BytesIO
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

# Optional OCR deps
try:
    from PIL import Image, ImageDraw
    import pytesseract
    OCR_AVAILABLE = True
except Exception:
    OCR_AVAILABLE = False

# ───────────────────────────────────────────────
# Paths / constants
# ───────────────────────────────────────────────
BASE_DIR       = Path(__file__).resolve().parent
AUTH_STATE     = BASE_DIR / "auth_state.json"
LOG_FILE       = BASE_DIR / "scrape_daily.log"
DAILY_LOG_CSV  = BASE_DIR / "daily_report_log.csv"
SCREENS_DIR    = BASE_DIR / "screens"
ROI_MAP_FILE   = BASE_DIR / "roi_map.json"

DASHBOARD_URL = (
    "https://lookerstudio.google.com/embed/u/0/reporting/"
    "d93a03c7-25dc-439d-abaa-dd2f3780daa5/page/BLfDE"
    "?params=%7B%22f20f0n9kld%22:%22include%25EE%2580%25803%25EE%2580%2580T%22%7D"
)

VIEWPORT = {"width": 1366, "height": 768}

# ───────────────────────────────────────────────
# Logging setup
# ───────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler(LOG_FILE)],
)
log = logging.getLogger("daily")
log.addHandler(logging.StreamHandler())

# ───────────────────────────────────────────────
# Config
# ───────────────────────────────────────────────
config = configparser.ConfigParser()
config.read(BASE_DIR / "config.ini")
MAIN_WEBHOOK  = config["DEFAULT"].get("DAILY_WEBHOOK") or config["DEFAULT"].get("MAIN_WEBHOOK", os.getenv("MAIN_WEBHOOK", ""))
ALERT_WEBHOOK = config["DEFAULT"].get("ALERT_WEBHOOK",  os.getenv("ALERT_WEBHOOK", ""))
CI_RUN_URL    = os.getenv("CI_RUN_URL", "")

# ───────────────────────────────────────────────
# Helpers: Chat + files
# ───────────────────────────────────────────────
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
                time.sleep(delay)
                backoff = min(backoff * 1.7, max_backoff)
                continue
            log.error(f"Webhook error {r.status_code}: {r.text[:300]}")
            return False
        except Exception as e:
            log.error(f"Webhook exception: {e}")
            time.sleep(backoff)
            backoff = min(backoff * 1.7, max_backoff)

def alert(lines):
    if not ALERT_WEBHOOK or "chat.googleapis.com" not in ALERT_WEBHOOK:
        return
    if CI_RUN_URL:
        lines.append(f"• CI run: {CI_RUN_URL}")
    _post_with_backoff(ALERT_WEBHOOK, {"text": "\n".join(lines)})

def save_bytes(path: Path, data: bytes):
    SCREENS_DIR.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    log.info(f"Saved {path.name}")

def save_text(path: Path, text: str):
    SCREENS_DIR.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    log.info(f"Saved {path.name}")

# ───────────────────────────────────────────────
# Browser automation
# ───────────────────────────────────────────────
def click_this_week(page):
    for sel in [("role", "button", "This Week"), ("text", "This Week")]:
        try:
            if sel[0] == "role":
                el = page.get_by_role(sel[1], name=sel[2])
            else:
                el = page.get_by_text(sel[1])
            if el.count():
                el.first.click(timeout=2000)
                page.wait_for_timeout(800)
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
                    btn.nth(i).click(timeout=1200)
                    clicked += 1
                    fr.wait_for_timeout(300)
                except Exception:
                    continue
        except Exception:
            continue
    if clicked:
        log.info(f"Clicked {clicked} 'PROCEED' overlay(s). Waiting for render…")
        page.wait_for_timeout(1200)
    return clicked

def open_and_prepare(page):
    log.info("Opening Retail Performance Dashboard…")
    try:
        page.goto(DASHBOARD_URL, wait_until="domcontentloaded", timeout=120_000)
    except TimeoutError:
        log.error("Timeout loading dashboard.")
        return False

    if "accounts.google.com" in page.url:
        log.error("Login required — auth_state.json missing/expired.")
        return False

    page.wait_for_timeout(12_000)
    click_this_week(page)
    click_proceed_overlays(page)

    try:
        body = page.inner_text("body")
        if "You are about to interact with a community visualisation" in body:
            log.info("Community visualisation placeholders detected — retrying PROCEED and waiting longer.")
            click_proceed_overlays(page)
            page.wait_for_timeout(1500)
    except Exception:
        pass
    return True

# ───────────────────────────────────────────────
# Screenshot + OCR
# ───────────────────────────────────────────────
def screenshot_full(page):
    try:
        img_bytes = page.screenshot(full_page=True, type="png")
        ts = int(time.time())
        save_bytes(SCREENS_DIR / f"{ts}_fullpage.png", img_bytes)
        return Image.open(BytesIO(img_bytes))
    except Exception as e:
        log.error(f"Full-page screenshot failed: {e}")
        return None

def ocr_cell(img: Image.Image, want_time=False) -> str:
    if not OCR_AVAILABLE:
        return "—"
    try:
        w, h = img.size
        if max(w, h) < 240:
            img = img.resize((int(w*2), int(h*2)))
        txt = pytesseract.image_to_string(img, config="--psm 7")
        if want_time:
            m = re.search(r"\b\d{2}:\d{2}\b", txt)
            if m: return m.group(0)
        m = re.search(r"[£]?-?\d+(?:\.\d+)?(?:[KMB]|%)?", txt)
        if m: return m.group(0)
        return "—"
    except Exception:
        return "—"

# ───────────────────────────────────────────────
# Parse text layout (verified vs 1761317314_daily_text.txt)
# ───────────────────────────────────────────────
NUM = r"[£]?-?\d+(?:\.\d+)?(?:[KMB]|%)?"
def parse_from_lines(lines: List[str]) -> Dict[str, str]:
    m = {}
    txt = "\n".join(lines)

    def warn(k):
        log.warning(f"[WARN] Missing {k} in text — will stay blank or ROI fallback.")

    # context
    r = re.search(r"([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}).*?\|\s*([^\|]+?)\s*\|\s*(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})", txt, re.S)
    m["store_line"] = r.group(0).strip() if r else ""
    t = re.search(r"\b(\d{1,2}\s+[A-Za-z]{3}\s+\d{4},\s*\d{2}:\d{2}:\d{2})\b", txt)
    m["page_timestamp"] = t.group(1) if t else "—"
    p = re.search(r"Period\s*([^\n]+)", txt)
    m["period_range"] = p.group(1).strip() if p else "—"

    # Sales
    s = re.search(r"Total\s*\n\s*(" + NUM + r")\s*\n\s*(" + NUM + r")\s*\n\s*(" + NUM + r")", txt)
    if s:
        m["sales_total"], m["sales_lfl"], m["sales_vs_target"] = s.group(1), s.group(2), s.group(3)
    else:
        warn("Sales Total"); m["sales_total"]=m["sales_lfl"]=m["sales_vs_target"]="—"

    # Waste & Markdowns
    w = re.search(r"Waste\s*\n\s*(" + NUM + r")\s*\n\s*Markdowns\s*\n\s*(" + NUM + r")\s*\n\s*Total\s*\n\s*(" + NUM + r")\s*\n\s*([+\-]?\d+[KMB%\-\.]*)\s*\n\s*([+\-]?\d+(?:\.\d+)?%)", txt)
    if w:
        m["waste_total"], m["markdowns_total"], m["wm_total"], m["wm_delta"], m["wm_delta_pct"] = w.groups()
    else:
        warn("Waste & Markdowns"); m.update({k:"—" for k in ["waste_total","markdowns_total","wm_total","wm_delta","wm_delta_pct"]})

    # Payroll
    pay_labels = ["Payroll Outturn","Absence Outturn","Productive Outturn","Holiday Outturn","Current Base Cost"]
    for lbl in pay_labels:
        rg = re.search(lbl + r"\D+(" + NUM + ")", txt)
        m_key = lbl.lower().replace(" ", "_")
        if rg: m[m_key] = rg.group(1)
        else: m[m_key] = "—"; warn(lbl)

    # Online
    online_labels = ["Availability","Despatched on Time","Delivered on Time"]
    for lbl in online_labels:
        rg = re.search(lbl + r"\D+(" + NUM + ")", txt)
        m_key = lbl.lower().replace(" ", "_")
        if rg: m[m_key] = rg.group(1)
        else: m[m_key] = "—"; warn(lbl)
    m["cc_avg_wait"] = "—"

    # Front End Service
    fes_labels = ["SCO Utilisation","Efficiency","Scan Rate","Interventions","Mainbank Closed"]
    for lbl in fes_labels:
        rg = re.search(lbl + r"\D+(" + NUM + ")", txt)
        m_key = lbl.lower().replace(" ", "_").replace(" ", "")
        if rg: m[m_key] = rg.group(1)
        else: m[m_key] = "—"; warn(lbl)
    m["scan_vs_target"]=m["interventions_vs_target"]=m["mainbank_vs_target"]="—"

    # Card Engagement
    ce_labels = ["Swipe Rate","Swipes WOW","New Customers","Swipes YOY"]
    for lbl in ce_labels:
        rg = re.search(lbl + r"\D+(" + NUM + ")", txt)
        m_key = lbl.lower().replace(" ", "_").replace("%","pct")
        if rg: m[m_key] = rg.group(1)
        else: m[m_key] = "—"; warn(lbl)

    # Shrink
    shrink_labels = ["Order Adjustments","Waste Validation","Unrecorded Waste","Shrink vs Budget"]
    for lbl in shrink_labels:
        rg = re.search(lbl + r"\D+(" + NUM + ")", txt)
        m_key = lbl.lower().replace(" ", "_").replace("%","pct")
        if rg: m[m_key] = rg.group(1)
        else: m[m_key] = "—"; warn(lbl)

    # Misc
    m["complaints_key"]  = re.search(r"Key Complaints\s*(\d+)", txt).group(1) if re.search(r"Key Complaints\s*(\d+)", txt) else "—"
    m["data_provided"]   = re.search(r"Data Provided\s*(\d+%?)", txt).group(1) if re.search(r"Data Provided\s*(\d+%?)", txt) else "—"
    m["trusted_data"]    = re.search(r"Trusted Data\s*(\d+%?)", txt).group(1) if re.search(r"Trusted Data\s*(\d+%?)", txt) else "—"
    m["my_reports"]      = re.search(r"My Reports\s*(\d+)", txt).group(1) if re.search(r"My Reports\s*(\d+)", txt) else "—"
    m["weekly_activity"] = re.search(r"Weekly Activity\s*(\d+%?)", txt).group(1) if re.search(r"Weekly Activity\s*(\d+%?)", txt) else "—"

    # NPS gauges placeholders
    for k in ["supermarket_nps","colleague_happiness","home_delivery_nps","cafe_nps","click_collect_nps","customer_toilet_nps"]:
        m.setdefault(k,"—")
    return m

# ───────────────────────────────────────────────
# ROI OCR fallback (NPS gauges only)
# ───────────────────────────────────────────────
def load_roi_map():
    try:
        roi = json.loads(ROI_MAP_FILE.read_text())
        log.info(f"Loaded ROI overrides from roi_map.json: {len(roi)} entries.")
        return roi
    except Exception as e:
        log.error(f"Failed to load roi_map.json: {e}")
        return {}

def crop_norm(img, roi):
    W,H = img.size
    x,y,w,h = roi
    return img.crop((int(x*W),int(y*H),int((x+w)*W),int((y+h)*H)))

def draw_overlay(img, roi_map):
    try:
        dbg = img.copy()
        draw = ImageDraw.Draw(dbg)
        W,H = dbg.size
        for key,(x,y,w,h) in roi_map.items():
            box = (int(x*W),int(y*H),int((x+w)*W),int((y+h)*H))
            draw.rectangle(box, outline=(0,255,0), width=2)
            draw.text((box[0]+3, box[1]+3), key, fill=(0,255,0))
        out = SCREENS_DIR / f"{int(time.time())}_roi_overlay.png"
        dbg.save(out)
        log.info(f"ROI overlay saved → {out.name}")
    except Exception:
        pass

def fill_missing_with_roi(metrics, img):
    if img is None:
        return
    roi_map = load_roi_map()
    used=False
    for k in ["supermarket_nps","colleague_happiness","home_delivery_nps","cafe_nps","click_collect_nps","customer_toilet_nps"]:
        if metrics.get(k) and metrics[k]!="—":
            continue
        if k not in roi_map: continue
        val = ocr_cell(crop_norm(img, roi_map[k]))
        if val!="—":
            metrics[k]=val
            used=True
    if used: draw_overlay(img, roi_map)

# ───────────────────────────────────────────────
# Card + CSV
# ───────────────────────────────────────────────
def kv(label,val): return {"decoratedText":{"topLabel":label,"text":val or "—"}}
def title_widget(text): return {"textParagraph":{"text":f"<b>{text}</b>"}}

CSV_HEADERS = [
    "page_timestamp","period_range","store_line",
    "sales_total","sales_lfl","sales_vs_target",
    "supermarket_nps","colleague_happiness","home_delivery_nps","cafe_nps","click_collect_nps","customer_toilet_nps",
    "sco_utilisation","efficiency","scan_rate","scan_vs_target","interventions","interventions_vs_target",
    "mainbank_closed","mainbank_vs_target",
    "availability","despatched_on_time","delivered_on_time","cc_avg_wait",
    "waste_total","markdowns_total","wm_total","wm_delta","wm_delta_pct",
    "moa","waste_validation","unrecorded_waste_pct","shrink_vs_budget_pct",
    "payroll_outturn","absence_outturn","productive_outturn","holiday_outturn","current_base_cost",
    "swipe_rate","swipes_wow_pct","new_customers","swipes_yoy_pct",
    "complaints_key","data_provided","trusted_data","my_reports","weekly_activity",
]

def build_card(m):
    header={"title":"📊 Retail Daily Summary (Layout+ROI OCR)","subtitle":m.get("store_line","")}
    sec=[{"widgets":[kv("Report Time",m.get("page_timestamp")),kv("Period",m.get("period_range"))]}]
    return {"cardsV2":[{"cardId":f"daily_{int(time.time())}","card":{"header":header,"sections":sec}}]}

def send_card(m):
    if not MAIN_WEBHOOK or "chat.googleapis.com" not in MAIN_WEBHOOK:
        log.error("MAIN_WEBHOOK invalid")
        return False
    return _post_with_backoff(MAIN_WEBHOOK, build_card(m))

def write_csv(m):
    write_header = not DAILY_LOG_CSV.exists()
    with open(DAILY_LOG_CSV,"a",newline="",encoding="utf-8") as f:
        w=csv.writer(f)
        if write_header: w.writerow(CSV_HEADERS)
        w.writerow([m.get(h,"—") for h in CSV_HEADERS])
    log.info("Appended daily metrics row")

# ───────────────────────────────────────────────
# Run
# ───────────────────────────────────────────────
def run_daily_scrape():
    if not AUTH_STATE.exists():
        alert(["⚠️ Login required; missing auth_state.json"])
        return
    with sync_playwright() as p:
        browser=context=page=None
        metrics={}
        img=None
        try:
            browser=p.chromium.launch(headless=True)
            context=browser.new_context(storage_state=str(AUTH_STATE),viewport=VIEWPORT)
            page=context.new_page()
            if not open_and_prepare(page):
                alert(["⚠️ Daily scrape blocked — please re-login."])
                return
            img=screenshot_full(page)
            body=page.inner_text("body")
            lines=[ln.strip() for ln in body.splitlines() if ln.strip()]
            ts=int(time.time())
            save_text(SCREENS_DIR/f"{ts}_lines.txt","\n".join(f"{i:04d}|{ln}" for i,ln in enumerate(lines)))
            metrics=parse_from_lines(lines)
            fill_missing_with_roi(metrics,img)
        finally:
            if context: context.close()
            if browser: browser.close()
    send_card(metrics)
    write_csv(metrics)
    log.info("Daily card send → OK")

if __name__=="__main__":
    run_daily_scrape()
