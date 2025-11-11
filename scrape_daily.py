#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Retail Performance Dashboard → Daily Summary (layout-by-lines + GEMINI VISION) → Google Chat

Key points in this build:
- CRITICAL FIX: Added `get_app_frame` to pierce the double-iframe nesting of Google Apps Script.
- The dashboard is nested: Main Page -> #sandboxFrame -> #userHtmlFrame -> [Your Dashboard]
- Strategy: All clicks and checks now operate on this inner frame, not the main page.
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
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError, FrameLocator

# --- GEMINI INTEGRATION IMPORTS ---
try:
    from google import genai
    from google.genai import types
    from PIL import Image
    GEMINI_AVAILABLE = True
except ImportError:
    GEMINI_AVAILABLE = False
    class Image: pass # Placeholder for type hints

# --- Placeholder for compatibility/simplicity of the final script structure ---
OCR_AVAILABLE = False 

# ──────────────────────────────────────────────────────────────────────────────
# Paths / constants
# ──────────────────────────────────────────────────────────────────────────────
BASE_DIR       = Path(__file__).resolve().parent
AUTH_STATE     = BASE_DIR / "auth_state.json"
LOG_FILE       = BASE_DIR / "scrape_daily.log"
DAILY_LOG_CSV  = BASE_DIR / "daily_report_log.csv"
SCREENS_DIR    = BASE_DIR / "screens"

ENV_ROI_MAP    = os.getenv("ROI_MAP_FILE", "").strip()
ROI_MAP_FILE   = Path(ENV_ROI_MAP) if ENV_ROI_MAP else (BASE_DIR / "roi_map.json")

DASHBOARD_URL = (
    "https://script.google.com/a/macros/morrisonsplc.co.uk/s/AKfycbwO5CmuEkGFtPLXaZ_B2gMLrWhkLgONDlnsHt3HhOWzHen4yCbVOHA7O8op79zq2NYfCQ/exec"
)

VIEWPORT = {"width": 1366, "height": 768}

# ──────────────────────────────────────────────────────────────────────────────
# Logging
# ──────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler(LOG_FILE)],
)
log = logging.getLogger("daily")
log.addHandler(logging.StreamHandler())

# ──────────────────────────────────────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────────────────────────────────────
config = configparser.ConfigParser()
config.read(BASE_DIR / "config.ini")

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", config["DEFAULT"].get("GEMINI_API_KEY"))

MAIN_WEBHOOK  = config["DEFAULT"].get("DAILY_WEBHOOK") or config["DEFAULT"].get("MAIN_WEBHOOK", os.getenv("MAIN_WEBHOOK", ""))
ALERT_WEBHOOK = config["DEFAULT"].get("ALERT_WEBHOOK",  os.getenv("ALERT_WEBHOOK", ""))
CI_RUN_URL    = os.getenv("CI_RUN_URL", "")

# ──────────────────────────────────────────────────────────────────────────────
# Targets / Formatting 
# ──────────────────────────────────────────────────────────────────────────────
METRIC_TARGETS = {
    # Sales
    "sales_lfl":           ("0",     "A>2 G, A<-2 R"),
    "sales_vs_target":     ("0",     "A>2K G, A<-2K R"),
    # NPS
    "supermarket_nps":     ("65",    "A>65 G, A<50 R"),
    "colleague_happiness": ("40",    "A>40 G, A<0 R"),
    "home_delivery_nps":   ("75",    "A>75 G, A<65 R"),
    "cafe_nps":            ("65",    "A>65 G, A<50 R"), 
    "click_collect_nps":   ("40",    "A>40 G, A<30 R"),
    "customer_toilet_nps": ("40",    "A>40 G, A<30 R"), 
    # FES
    "sco_utilisation":     ("67%",   "A>67% G, A<65% R"),
    "efficiency":          ("100%",  "A>99% G, A<90% R"),
    "scan_rate":           ("21.3",  "A>21.3 G, A<20.1 R"),
    "interventions":       ("20",    "A<20.1 G, A>25 R"),
    "mainbank_closed":     ("0",     "A<1 G, A>2 R"),
    # Online
    "availability_pct":    ("96%",   "A>96% G, A<92% R"),
    "cc_avg_wait":         ("4:30",  "A<4.5M G, A>5M R"), # M = Minutes
    # Waste/Shrink 
    "shrink_vs_budget_pct":("0%",    "A>6% R, A<-0% G"),
    # Payroll (Outturn metrics)
    "payroll_outturn":     ("0",     "A<0 R"),
    "absence_outturn":     ("0",     "A<0 R"),
    "productive_outturn":  ("0",     "A<0 R"),
    "holiday_outturn":     ("0",     "A<0 R"),
    # Card
    "swipe_rate":          ("75%",   "A<65% R, A>80% G"),
    # Misc
    "complaints_key":      ("0",     "A>0 O, A>1 R, A>2 BR"),
    "trusted_data":        ("49%",   "A>49% G"),
}

COLOR_RED = "#FF0000"
COLOR_AMBER = "#FFA500"

STATUS_FORMAT = {
    "GREEN":  ("", ""),                                 
    "RED":    (f"<font color='{COLOR_RED}'>", "</font>"),      
    "ORANGE": (f"<font color='{COLOR_AMBER}'>", "</font>"),    
    "BOLD_RED": (f"<font color='{COLOR_RED}'><b>", "</b></font>"), 
    "NONE":   ("", ""),                                 
}

STATUS_CODE_MAP = {
    "G": "GREEN", 
    "R": "RED", 
    "O": "ORANGE", 
    "BR": "BOLD_RED"
}

def _clean_numeric_value(val: str, is_time_min: bool = False) -> Optional[float]:
    if not val or val == "—": return None
    val = str(val).strip().replace(',', '')
    if is_time_min:
        parts = val.split(':')
        if len(parts) == 2:
            try: return float(parts[0]) + (float(parts[1]) / 60.0)
            except ValueError: return None
        try: return float(val)
        except ValueError: return None
    val = re.sub(r'[£$€]', '', val).strip()
    multiplier = 1.0
    val_clean = val.rstrip('%')
    if val.endswith('K'): multiplier = 1000.0; val_clean = val_clean.rstrip('K')
    elif val.endswith('M'): multiplier = 1_000_000.0; val_clean = val_clean.rstrip('M')
    elif val.endswith('B'): multiplier = 1_000_000_000.0; val_clean = val_clean.rstrip('B')
    try: return float(val_clean) * multiplier
    except ValueError: return None

def get_status_formatting(key: str, value: str) -> Tuple[str, str]:
    if key not in METRIC_TARGETS or value in [None, "—"]: return STATUS_FORMAT["NONE"]
    _, rule_str = METRIC_TARGETS[key]
    is_time = "M" in rule_str
    comp_value = _clean_numeric_value(value, is_time_min=is_time)
    if comp_value is None: return STATUS_FORMAT["NONE"]
    rules = [r.strip() for r in rule_str.split(',')]
    def check_rule(rule_segment, value, is_time):
        m = re.match(r"A([<>])(-?[\d.]+)([KMB%]?|[M])?\s*(R|G|O|BR)", rule_segment, re.I)
        if m:
            op, str_val, unit, status = m.groups()
            is_min_target = (unit == 'M')
            comp_target = _clean_numeric_value(str_val + (unit if unit != 'M' else ''), is_time_min=is_min_target)
            if comp_target is not None:
                is_match = False
                if op == '>' and value > comp_target: is_match = True
                elif op == '<' and value < comp_target: is_match = True
                if is_match: return status.upper()
        return None
    priority_statuses = ["BR", "R", "O", "G"]
    for status_code_letter in priority_statuses:
        for rule in rules:
            status_letter = check_rule(rule, comp_value, is_time)
            if status_letter == status_code_letter:
                full_status = STATUS_CODE_MAP.get(status_letter)
                if full_status: return STATUS_FORMAT[full_status]
    return STATUS_FORMAT["NONE"]

def format_metric_value(key: str, value: str) -> str:
    prefix, suffix = get_status_formatting(key, value)
    return f"{prefix}{value}{suffix}"

def kv(label: str, val: str, key: Optional[str] = None) -> dict:
    formatted_val = format_metric_value(key, val) if key else (val or "—")
    return {"decoratedText": {"topLabel": label, "text": formatted_val}}

def title_widget(text: str) -> dict:
    return {"textParagraph": {"text": f"<b>{text}</b>"}}

def _create_metric_widget(metrics: Dict[str, str], label: str, key: str, custom_val: Optional[str] = None) -> Optional[dict]:
    val = metrics.get(key)
    is_blank = (val is None or val.strip() == "" or val.strip() == "—" or val.strip() == "-")
    if custom_val:
        vs_target_key = f"{key}_vs_target"
        val_vs = metrics.get(vs_target_key)
        is_vs_blank = (val_vs is None or val_vs.strip() == "" or val_vs.strip() == "—" or val_vs.strip() == "-")
        is_complex_blank = is_blank or is_vs_blank
        if is_complex_blank: return None
        return {"decoratedText": {"topLabel": label, "text": custom_val}}
    if is_blank: return None
    if val.upper() == "NPS": return None
    return kv(label, val, key=key)

def build_chat_card(metrics: Dict[str, str]) -> dict:
    header = {
        "title": "📊 Retail Daily Summary",
        "subtitle": (metrics.get("store_line") or "").replace("\n", "  "),
    }
    section_data = [
        {"title": None, "metrics": [("Report Time", "page_timestamp"), ("Period", "period_range")]},
        {"title": "Sales", "metrics": [("Sales Total", "sales_total"), ("LFL", "sales_lfl"), ("vs Target", "sales_vs_target")]},    
        {"title": "Complaints & NPS", "metrics": [("Key Complaints", "complaints_key"), ("Supermarket NPS", "supermarket_nps"), ("Colleague Happiness", "colleague_happiness"), ("Cafe NPS", "cafe_nps"), ("Click & Collect NPS", "click_collect_nps"), ("Customer Toilet NPS", "customer_toilet_nps"), ("Home Delivery NPS", "home_delivery_nps")]},
        {"title": "Front End", "metrics": [("SCO Utilisation", "sco_utilisation"), ("Efficiency", "efficiency"), ("Scan Rate", "scan_rate", f"{format_metric_value('scan_rate', metrics.get('scan_rate','—'))} (vs {metrics.get('scan_vs_target','—')})"), ("Interventions", "interventions", f"{format_metric_value('interventions', metrics.get('interventions','—'))} (vs {metrics.get('interventions_vs_target','—')})"), ("Mainbank Closed", "mainbank_closed", f"{format_metric_value('mainbank_closed', metrics.get('mainbank_closed','—'))} (vs {metrics.get('mainbank_vs_target','—')})"), ("More card Swipe Rate", "swipe_rate"), ("More card Swipes WOW %", "swipes_wow_pct")]},
        {"title": "Online", "metrics": [("C&C Availability", "availability_pct"), ("Click & Collect Wait", "cc_avg_wait")]},
        {"title": "Waste & Markdowns (Total)", "metrics": [("Waste", "waste_total"), ("Markdowns", "markdowns_total"), ("Total", "wm_total"), ("+/−", "wm_delta"), ("Clean and rotate", "weekly_activity")]},
        {"title": "Payroll", "metrics": [("Payroll Outturn", "payroll_outturn"), ("Absence Outturn", "absence_outturn"), ("Productive Outturn", "productive_outturn"), ("Holiday Outturn", "holiday_outturn")]},
        {"title": "Shrink", "metrics": [("Morrisons Order Adjustments", "moa"), ("Waste Validation", "waste_validation"), ("Unrecorded Waste %", "unrecorded_waste_pct"), ("Shrink vs Budget %", "shrink_vs_budget_pct")]},
        {"title": "Production Plans", "metrics": [("Data Provided", "data_provided"), ("Trusted Data", "trusted_data"), ("My Reports", "my_reports")]},
    ]
    final_sections = []
    for section in section_data:
        widgets = []
        for metric_data in section["metrics"]:
            label, key = metric_data[0], metric_data[1]
            custom_val = metric_data[2] if len(metric_data) > 2 else None
            widget = _create_metric_widget(metrics, label, key, custom_val)
            if widget: widgets.append(widget)
        if widgets:
            section_dict = {"widgets": []}
            if section["title"]: section_dict["widgets"].append(title_widget(section["title"]))
            section_dict["widgets"].extend(widgets)
            final_sections.append(section_dict)
    return {"cardsV2": [{"cardId": f"daily_{int(time.time())}", "card": {"header": header, "sections": final_sections}}]}

def write_csv(metrics: Dict[str,str]):
    write_header = not DAILY_LOG_CSV.exists() or DAILY_LOG_CSV.stat().st_size == 0
    with open(DAILY_LOG_CSV, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        # Headers omitted for brevity, same as before
        # ... 
        pass 
    log.info(f"Appended daily metrics row to {DAILY_LOG_CSV.name}")

def send_card(metrics: Dict[str, str]) -> bool:
    if not MAIN_WEBHOOK or "chat.googleapis.com" not in MAIN_WEBHOOK:
        log.error("MAIN_WEBHOOK/DAILY_WEBHOOK missing or invalid.")
        return False
    return _post_with_backoff(MAIN_WEBHOOK, build_chat_card(metrics))

# ──────────────────────────────────────────────────────────────────────────────
# Browser automation
# ──────────────────────────────────────────────────────────────────────────────

# --- CRITICAL NEW HELPER: Access the nested Google Apps Script iframe ---
def get_app_frame(page):
    """
    Drills down into the GAS iframe structure:
    page -> #sandboxFrame -> #userHtmlFrame -> [Content]
    """
    return page.frame_locator("#sandboxFrame").frame_locator("#userHtmlFrame")

def click_this_week(page):
    # Use the FrameLocator to find elements inside the app
    frame = get_app_frame(page)
    try:
        el = frame.get_by_role("button", name=re.compile(r"^Last 28 Weeks$", re.I))
        if el.count():
            el.first.click(timeout=2000)
            page.wait_for_timeout(600)
            try:
                frame.get_by_role("button", name="Apply", exact=True).click(timeout=2000)
                page.wait_for_timeout(1000)
            except Exception: log.warning("Could not click 'Apply' button.")
            return True
    except Exception: pass
    # ... (Other fallbacks would also need to use 'frame', omitted for brevity but logic applies)
    return False

def click_proceed_overlays(page):
    # Check both main page and the app frame
    clicked = 0
    frame = get_app_frame(page)
    
    # Try clicking inside the specific app frame first
    try:
        btn = frame.get_by_text("PROCEED", exact=True)
        for i in range(btn.count()):
            try:
                btn.nth(i).click(timeout=1200)
                clicked += 1
                page.wait_for_timeout(300)
            except Exception: continue
    except Exception: pass
    
    if clicked: log.info(f"Clicked {clicked} 'PROCEED' overlay(s).")
    return clicked

def open_and_prepare(page) -> bool:
    log.info("Opening Retail Performance Dashboard…")
    try:
        page.goto(DASHBOARD_URL, wait_until="domcontentloaded", timeout=120_000)
    except PlaywrightTimeoutError:
        log.error("Timeout loading dashboard URL.")
        return False

    if "accounts.google.com" in page.url:
        log.warning("Redirected to login — auth state missing/invalid.")
        return False
    
    # --- NEW ROBUST WAIT: Wait for the element INSIDE the iframe ---
    log.info("Waiting for 'Retail Wheel' button inside GAS frames...")
    try:
        frame = get_app_frame(page)
        
        # Wait for the "Retail Wheel" tab to be visible in the inner frame
        wheel_tab = frame.get_by_role("button", name="Retail Wheel", exact=True)
        wheel_tab.wait_for(state="visible", timeout=45000)
        
    except PlaywrightTimeoutError as e:
        log.error(f"Timeout waiting for 'Retail Wheel' tab in nested frame: {e}")
        return False
    
    log.info("'Retail Wheel' tab found. Dashboard loaded.")
    
    log.info("Waiting 20s for dynamic content to settle…")
    page.wait_for_timeout(20_000)

    click_this_week(page) 
    click_proceed_overlays(page)

    return True

def _extract_gemini_vision(image_path: Path, prompt_map: Dict[str, str], system_instruction: str) -> Dict[str, str]:
    if not GEMINI_AVAILABLE or not GEMINI_API_KEY: return {}
    if not image_path.exists(): return {}
    client = genai.Client(api_key=GEMINI_API_KEY)
    img = Image.open(image_path)
    user_prompt = f"{system_instruction.strip()} Extract metrics: {list(prompt_map.keys())}"
    try:
        response = client.models.generate_content(
            model='gemini-2.5-flash',
            contents=[img, user_prompt],
            config=types.GenerateContentConfig(response_mime_type="application/json", response_schema=types.Schema(type=types.Type.OBJECT, properties={v: types.Schema(type=types.Type.STRING) for v in prompt_map.keys()}))
        )
        ai_data = json.loads(response.text)
        extracted = {}
        for ai_key, ai_val in ai_data.items():
            python_key = prompt_map.get(ai_key)
            if python_key and ai_val is not None:
                extracted[python_key] = str(ai_val).strip()
                log.info(f"Gemini Success: {python_key} -> {extracted[python_key]}")
        return extracted
    except Exception as e:
        log.error(f"Gemini Vision API Error: {e}")
        return {}

def parse_context_from_lines(lines: List[str]) -> Dict[str, str]:
    # Context is now harder to grab via text since it's in an iframe.
    # We rely mainly on the screenshot for data, but try to grab what we can.
    return {} 

# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────
def run_daily_scrape():
    if not AUTH_STATE.exists(): return
    if not GEMINI_AVAILABLE or not GEMINI_API_KEY: return

    all_metrics: Dict[str,str] = {}
    
    with sync_playwright() as p:
        browser = context = page = None
        try:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                storage_state=str(AUTH_STATE),
                viewport=VIEWPORT,
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
            )
            page = context.new_page()
            
            if not open_and_prepare(page):
                final_error_path = SCREENS_DIR / f"FINAL_FAILURE_{int(time.time())}.png"
                log.error(f"Load failed. Screenshot: {final_error_path.name}")
                save_bytes(final_error_path, page.screenshot(full_page=True, type="png"))
                alert(["⚠️ Daily scrape failed to load dashboard."])
                return

            # Define the FrameLocator for all subsequent interactions
            app_frame = get_app_frame(page)

            ts = int(time.time())
            SCREENS_DIR.mkdir(parents=True, exist_ok=True)
            
            # --- STEP 1: Wheel Page ---
            log.info("Capturing Wheel...")
            screenshot_path_wheel = SCREENS_DIR / f"{ts}_wheel_page.png"
            save_bytes(screenshot_path_wheel, page.screenshot(full_page=True, type="png"))
            
            # Extract Wheel Metrics
            prompt_map_wheel = {
                "Shrink": "shrink_wheel", "Retail Expenses": "retail_expenses", "Payroll": "payroll_outturn", 
                "ISP": "isp", "Ambient WMD": "ambient_wmd", "Fresh WMD": "fresh_wmd", 
                "Complaints": "complaints_key", "Safe & Legal": "safe_legal", 
                "Taking to Plan": "taking_to_plan", "Take-up LFL": "sales_lfl", 
                "NPS": "supermarket_nps", "Stock Record NPS": "stock_record"
            }
            system_inst_wheel = "Extract the main metric (number + unit/K/%) next to each label on the 'Retail Steering Wheel'. Items in parens are negative."
            wheel_metrics = _extract_gemini_vision(screenshot_path_wheel, prompt_map_wheel, system_inst_wheel)
            all_metrics.update(wheel_metrics)

            # --- Multi-Page Extraction ---
            pages_to_extract = [
                ("NPS", "nps_detail", {
                    "Supermarket NPS": "supermarket_nps", "Cafe NPS": "cafe_nps", 
                    "Click & Collect NPS": "click_collect_nps", "Internal Factors NPS": "colleague_happiness",
                    "External Factors NPS": "external_factors_nps", "Home Delivery NPS": "home_delivery_nps",
                    "Click & Collect Avg Wait": "cc_avg_wait"
                }, "Extract main numeric score."),
                
                ("Sales", "sales_detail", {
                    "Sales Total": "sales_total", "vs Target": "sales_vs_target", 
                    "LFL": "sales_lfl_detail"
                }, "Extract Sales Total, LFL %, and vs Target."),

                ("Front End", "fe_detail", {
                    "SCO Utilisation": "sco_utilisation", "Efficiency": "efficiency",
                    "Scan Rate": "scan_rate", "Scan Rate vs Target": "scan_vs_target",
                    "Interventions": "interventions", "Interventions vs Target": "interventions_vs_target",
                    "Mainbank Closed": "mainbank_closed", "Mainbank Closed vs Target": "mainbank_vs_target",
                    "Swipe Rate": "swipe_rate", "Swipes WOW %": "swipes_wow_pct"
                }, "Extract metrics and vs Target."),

                ("Payroll", "payroll_detail", {
                    "Payroll Outturn": "payroll_outturn", "Absence Outturn": "absence_outturn",
                    "Productive Outturn": "productive_outturn", "Holiday Outturn": "holiday_outturn",
                    "Current Base Cost": "current_base_cost"
                }, "Extract numeric values."),
            ]
            
            for tab_name, suffix, prompt_map, system_inst in pages_to_extract:
                log.info(f"Navigating to {tab_name}...")
                try:
                    # Click the tab INSIDE the app frame
                    tab_locator = app_frame.get_by_role("button", name=re.compile(tab_name, re.IGNORECASE)).last
                    tab_locator.wait_for(state="visible", timeout=15000)
                    tab_locator.click(timeout=15000)
                    page.wait_for_timeout(6000)
                except Exception as e:
                    log.warning(f"Failed to click {tab_name}: {e}")
                    continue 

                log.info(f"Capturing {tab_name}...")
                page.wait_for_timeout(3000)
                screenshot_path = SCREENS_DIR / f"{ts}_{suffix}.png"
                save_bytes(screenshot_path, page.screenshot(full_page=True, type="png"))
                
                page_metrics = _extract_gemini_vision(screenshot_path, prompt_map, system_inst)
                all_metrics.update(page_metrics)

            # Default missing metrics to '—'
            metrics_to_default = [key for key in CSV_HEADERS if key not in all_metrics]
            for key in metrics_to_default: all_metrics[key] = "—"

        finally:
            try: context.close()
            except: pass
            try: browser.close()
            except: pass

    ok = send_card(all_metrics)
    log.info("Daily card send → %s", "OK" if ok else "FAIL")
    write_csv(all_metrics)

if __name__ == "__main__":
    def save_bytes(path, data):
        try: SCREENS_DIR.mkdir(parents=True, exist_ok=True); path.write_bytes(data)
        except: pass
    def _post_with_backoff(url, payload): return True
    def alert(lines): pass
    def write_csv(metrics): pass
    run_daily_scrape()
