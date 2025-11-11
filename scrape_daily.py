#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Retail Performance Dashboard → Daily Summary (layout-by-lines + GEMINI VISION) → Google Chat

Key points in this build:
- CRITICAL UPDATE: Updated URL to new GAS link, switched to multi-page navigation.
- Strategy: Capture Wheel, Click Sales tab, Capture Sales/FES details, Click NPS tab, Capture NPS details.
- FIX: Improved locator for the top navigation tabs to prevent click timeout.
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

# !!! UPDATED DASHBOARD URL to the new GAS link !!!
DASHBOARD_URL = (
    "https://script.google.com/a/macros/morrisonsplc.co.uk/s/AKfycbwO5CmuEkGFtPLXaZ_B2gMLW_WH_KgONDlnsHt3HhOWzHen4yCbVOHA7O8op79zq2NYfCQ/exec"
)

VIEWPORT = {"width": 1366, "height": 768}

# --- METRICS TO EXTRACT VIA GEMINI (Updated for New Dashboard Structure) ---
GEMINI_METRICS_WHEEL = [
    "shrink", "retail_expenses", "payroll_wheel", "isp", "ambient_wmd", 
    "fresh_wmd", "complaints_wheel", "safe_legal", "taking_to_plan", "sales_lfl", 
    "supermarket_nps", "stock_record" # Renamed nps_wheel to supermarket_nps for consistency
]
GEMINI_METRICS_NPS = [
    "supermarket_nps", "colleague_happiness", "home_delivery_nps", "cafe_nps", 
    "click_collect_nps", "customer_toilet_nps", "internal_factors_nps", 
    "external_factors_nps", "cc_avg_wait"
]
GEMINI_METRICS_SALES_FES = [
    "sales_total", "sales_vs_target", "sco_utilisation", "efficiency", 
    "scan_rate", "interventions", "mainbank_closed", "scan_vs_target", 
    "interventions_vs_target", "mainbank_vs_target"
]
# ... (all other constants and helpers remain unchanged) ...

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
# Targets / Formatting (Retained as correct)
# ──────────────────────────────────────────────────────────────────────────────
METRIC_TARGETS = {
    "sales_lfl":           ("0",     "A>2 G, A<-2 R"),
    "sales_vs_target":     ("0",     "A>2K G, A<-2K R"),
    "supermarket_nps":     ("65",    "A>65 G, A<50 R"),
    "colleague_happiness": ("40",    "A>40 G, A<0 R"),
    "home_delivery_nps":   ("75",    "A>75 G, A<65 R"),
    "cafe_nps":            ("65",    "A>65 G, A<50 R"), 
    "click_collect_nps":   ("40",    "A>40 G, A<30 R"),
    "customer_toilet_nps": ("40",    "A>40 G, A<30 R"), 
    "sco_utilisation":     ("67%",   "A>67% G, A<65% R"),
    "efficiency":          ("100%",  "A>99% G, A<90% R"),
    "scan_rate":           ("21.3",  "A>21.3 G, A<20.1 R"),
    "interventions":       ("20",    "A<20.1 G, A>25 R"),
    "mainbank_closed":     ("0",     "A<1 G, A>2 R"),
    "availability_pct":    ("96%",   "A>96% G, A<92% R"),
    "cc_avg_wait":         ("4:30",  "A<4.5M G, A>5M R"), 
    "shrink_vs_budget_pct":("0%",    "A>6% R, A<-0% G"),
    "payroll_outturn":     ("0",     "A<0 R"),
    "absence_outturn":     ("0",     "A<0 R"),
    "productive_outturn":  ("0",     "A<0 R"),
    "holiday_outturn":     ("0",     "A<0 R"),
    "swipe_rate":          ("75%",   "A<65% R, A>80% G"),
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
    "G": "GREEN", "R": "RED", "O": "ORANGE", "BR": "BOLD_RED"
}

# ... (omitted formatting/value check helpers for brevity) ...

# ──────────────────────────────────────────────────────────────────────────────
# Helper Functions (Retained from previous steps, including fixes)
# ──────────────────────────────────────────────────────────────────────────────
def _clean_numeric_value(val: str, is_time_min: bool = False) -> Optional[float]:
    # ... (omitted implementation)
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
    # ... (omitted implementation)
    if key not in METRIC_TARGETS or value in [None, "—"]: return STATUS_FORMAT["NONE"]
    _, rule_str = METRIC_TARGETS[key]; is_time = "M" in rule_str
    comp_value = _clean_numeric_value(value, is_time_min=is_time)
    if comp_value is None: return STATUS_FORMAT["NONE"]
    rules = [r.strip() for r in rule_str.split(',')]
    def check_rule(rule_segment, value, is_time):
        m = re.match(r"A([<>])(-?[\d.]+)([KMB%]?|[M])?\s*(R|G|O|BR)", rule_segment, re.I)
        if m:
            op, str_val, unit, status = m.groups(); is_min_target = (unit == 'M')
            comp_target = _clean_numeric_value(str_val + (unit if unit != 'M' else ''), is_time_min=is_min_target)
            if comp_target is not None:
                is_match = (op == '>' and value > comp_target) or (op == '<' and value < comp_target)
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
        vs_target_key = f"{key}_vs_target"; val_vs = metrics.get(vs_target_key)
        is_vs_blank = (val_vs is None or val_vs.strip() == "" or val_vs.strip() == "—" or val_vs.strip() == "-")
        is_complex_blank = is_blank or is_vs_blank
        if is_complex_blank: return None
        return {"decoratedText": {"topLabel": label, "text": custom_val}}
        
    if is_blank or val.upper() == "NPS": return None
    return kv(label, val, key=key)

# ... (omitted _post_with_backoff, alert, save_bytes, save_text, build_chat_card, write_csv, send_card functions) ...

# ──────────────────────────────────────────────────────────────────────────────
# Gemini Vision Extraction (Combined Logic)
# ──────────────────────────────────────────────────────────────────────────────
def _extract_gemini_vision(image_path: Path, prompt_map: Dict[str, str], system_instruction: str) -> Dict[str, str]:
    """Generic function to call Gemini Vision for a set of fields."""
    if not GEMINI_AVAILABLE or not GEMINI_API_KEY:
        log.warning("Gemini API not available or key missing. Skipping AI extraction.")
        return {}

    if not image_path.exists():
        log.error(f"Image not found at {image_path}. Cannot perform vision extraction.")
        return {}

    client = genai.Client(api_key=GEMINI_API_KEY)
    img = Image.open(image_path)
    
    user_prompt = (
        f"{system_instruction.strip()} Analyze the image and return the exact values for "
        f"the following metrics as a single JSON object. For percentages, include '%'. "
        f"Metrics to extract: {list(prompt_map.keys())}"
    )

    try:
        response = client.models.generate_content(
            model='gemini-2.5-flash',
            contents=[img, user_prompt],
            config=types.GenerateContentConfig(
                response_mime_type="application/json",
                response_schema=types.Schema(
                    type=types.Type.OBJECT,
                    properties={v: types.Schema(type=types.Type.STRING) for v in prompt_map.keys()}
                )
            )
        )
        
        ai_data = json.loads(response.text)
        
        extracted = {}
        for ai_key, ai_val in ai_data.items():
            python_key = prompt_map.get(ai_key)
            if python_key and ai_val is not None:
                # Store cleaned value
                extracted[python_key] = str(ai_val).strip()
                log.info(f"Gemini Success: {python_key} -> {extracted[python_key]}")
        
        return extracted

    except Exception as e:
        log.error(f"Gemini Vision API Error for {list(prompt_map.keys())}: {e}")
        return {}

def parse_context_from_lines(lines: List[str]) -> Dict[str, str]:
    m: Dict[str, str] = {}
    joined = "\n".join(lines)
    
    # Store Line (Niki Cooke | 218 Thornton Cleveleys)
    z = re.search(r"([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}).*?\|\s*([^\|]+?)\s*\|\s*(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})", joined, re.S)
    m["store_line"] = z.group(0).strip() if z else "—"

    # Report Time/Page Timestamp (From the footer)
    ts_match = re.search(r"\b(\d{1,2}\s+[A-Za-z]{3}\s+\d{4},\s*\d{2}:\d{2}:\d{2})\b", joined)
    m["page_timestamp"] = ts_match.group(1) if ts_match else "—"
    
    # Period Range (Likely only visible on the NPS page after filter application)
    period_match = re.search(r"Dates included:\s*([^\n]+)", joined, re.I)
    m["period_range"] = period_match.group(1).strip() if period_match else "—"

    return m

# ──────────────────────────────────────────────────────────────────────────────
# Browser automation (FIXED SYNTAX)
# ──────────────────────────────────────────────────────────────────────────────
def click_this_week(page):
    try:
        el = page.get_by_role("button", name=re.compile(r"^This Week$", re.I))
        if el.count():
            el.first.click(timeout=2000)
            page.wait_for_timeout(600)
            return True
    except Exception:
        pass
    try:
        el = page.get_by_text(re.compile(r"^\s*This Week\s*$", re.I))
        if el.count():
            el.first.click(timeout=2000)
            page.wait_for_timeout(600)
            return True
    except Exception:
        pass
    return False

def click_proceed_overlays(page) -> int:
    clicked = 0
    for fr in page.frames:
        try:
            btn = fr.get_by_text("PROCEED", exact=True)
            for i in range(btn.count()):
                try: # <--- SYNTAX FIX: changed { to :
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

    # INCREASED WAIT: Gave 12s, now giving 20s for general content load
    log.info("Waiting 20s for dynamic content…")
    page.wait_for_timeout(20_000)

    click_this_week(page)
    click_proceed_overlays(page)

    try:
        body = page.inner_text("body")
    except Exception:
        body = ""
    if "You are about to interact with a community visualisation" in body:
        log.info("Community visualisation placeholders detected — retrying PROCEED and waiting longer.")
        click_proceed_overlays(page)
        page.wait_for_timeout(1500)

    return True

# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────
def run_daily_scrape():
    # ... (omitted initial setup and config checks) ...
    if not AUTH_STATE.exists():
        alert(["⚠️ Daily dashboard scrape needs login. Run `python scrape.py now` once to save auth_state.json."])
        log.error("auth_state.json not found.")
        return
    
    if not GEMINI_AVAILABLE:
        alert(["⚠️ Gemini library (google-genai) is not installed. Please install it to use the AI features."])
    
    if not GEMINI_API_KEY:
        alert(["⚠️ Gemini API Key is missing. Check your GitHub Secrets/Environment variables."])

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
                alert(["⚠️ Daily scrape blocked by login or load failure — please re-login."])
                return

            # --- STEP 1: Capture and Extract Initial Wheel Data ---
            log.info("Adding 5s final buffer wait before screenshot (Wheel)…")
            page.wait_for_timeout(5_000)
            
            # 1a. Screenshot Wheel
            ts = int(time.time())
            SCREENS_DIR.mkdir(parents=True, exist_ok=True)
            screenshot_path_wheel = SCREENS_DIR / f"{ts}_wheel_page.png"
            save_bytes(screenshot_path_wheel, page.screenshot(full_page=True, type="png"))
            
            # 1b. Extract Context (Time/Store)
            body_text = page.inner_text("body")
            lines = [ln.rstrip() for ln in body_text.splitlines()]
            all_metrics.update(parse_context_from_lines(lines))
            
            # 1c. Extract Wheel Metrics 
            prompt_map_wheel = {
                "Shrink": "shrink_wheel", "Retail Expenses": "retail_expenses", "Payroll": "payroll_outturn", 
                "ISP": "isp", "Ambient WMD": "ambient_wmd", "Fresh WMD": "fresh_wmd", 
                "Complaints": "complaints_key", "Safe & Legal": "safe_legal", 
                "Taking to Plan": "taking_to_plan", "Take-up LFL": "sales_lfl", 
                "NPS": "supermarket_nps", "Stock Record NPS": "stock_record"
            }

            system_inst_wheel = "You are a hyper-accurate retail dashboard data extractor. Extract the main metric (number + unit/K/%) next to each label on the 'Retail Steering Wheel' and other key components (e.g., '42.8K', '(2.3K)', '11', '0.0%'). For items in parentheses like (2.3K) return the value as -2.3K. For items like 'Retail Wheel Guide', return the content text."

            wheel_metrics = _extract_gemini_vision(screenshot_path_wheel, prompt_map_wheel, system_inst_wheel)
            all_metrics.update(wheel_metrics)

            # --- STEP 2: Navigate and Extract NPS Detail Page ---
            log.info("Navigating to NPS Detail page…")
            
            # 2a. Click the NPS navigation button/tab (Improved Locator)
            try:
                # Prioritize a locator based on text 'NPS' within the header area
                nps_tab = page.locator("nav").get_by_text("NPS").first
                
                if nps_tab.count() == 0:
                     # Fallback to general locator by role 'button' and name 'NPS'
                    nps_tab = page.get_by_role("button", name="NPS").last
                    
                nps_tab.click(timeout=10000) # Increased timeout to 10s
                page.wait_for_timeout(6000) # Wait for content transition and loading

                log.info("Successfully clicked the NPS tab.")
            except Exception as e:
                log.warning(f"Failed to click NPS tab. Skipping NPS detail extraction: {e}")
                pass
            
            # 2b. Screenshot NPS Detail Page
            log.info("Adding 5s final buffer wait before screenshot (NPS Detail)…")
            page.wait_for_timeout(5_000)
            img_bytes_nps = page.screenshot(full_page=True, type="png")
            screenshot_path_nps = SCREENS_DIR / f"{ts}_nps_detail_page.png"
            save_bytes(screenshot_path_nps, img_bytes_nps)
            
            # 2c. Extract NPS Metrics (using a map for the NPS page)
            prompt_map_nps = {
                "Supermarket NPS": "supermarket_nps_detail", "Cafe NPS": "cafe_nps", 
                "Click & Collect NPS": "click_collect_nps", "Internal Factors NPS": "colleague_happiness",
                "External Factors NPS": "external_factors_nps", "Home Delivery NPS": "home_delivery_nps"
            }

            system_inst_nps = "You are a specialist retail data extractor. Extract the main numeric score (number only, ignore targets) for the titled NPS metrics. For NPS values, extract the main large number (e.g., '40', '73', '80')."

            nps_metrics = _extract_gemini_vision(screenshot_path_nps, prompt_map_nps, system_inst_nps)
            
            # Merge NPS metrics, prioritizing detail page data
            all_metrics.update(nps_metrics)

            # --- STEP 3: Navigate and Extract Sales/FES Detail Page ---
            log.info("Navigating to Sales Detail page…")
            
            # 3a. Click the Sales navigation button/tab
            try:
                # Target the Sales button/tab
                sales_tab = page.locator("nav").get_by_text("Sales").first
                if sales_tab.count() == 0:
                     sales_tab = page.get_by_role("button", name="Sales").first
                     
                sales_tab.click(timeout=10000)
                page.wait_for_timeout(6000) 

                log.info("Successfully clicked the Sales tab.")
            except Exception as e:
                log.warning(f"Failed to click Sales tab. Skipping Sales detail extraction: {e}")
                pass

            # 3b. Screenshot Sales/FES Detail Page
            log.info("Adding 5s final buffer wait before screenshot (Sales Detail)…")
            page.wait_for_timeout(5_000)
            img_bytes_sales = page.screenshot(full_page=True, type="png")
            screenshot_path_sales = SCREENS_DIR / f"{ts}_sales_detail_page.png"
            save_bytes(screenshot_path_sales, img_bytes_sales)
            
            # 3c. Extract Sales/FES Metrics
            prompt_map_sales = {
                "Sales Total": "sales_total", "vs Target": "sales_vs_target", 
                "SCO Utilisation": "sco_utilisation", "Efficiency": "efficiency",
                "Scan Rate": "scan_rate", "Interventions": "interventions", 
                "Mainbank Closed": "mainbank_closed", "Swipe Rate": "swipe_rate", 
                "Swipes WOW %": "swipes_wow_pct"
            }

            # NOTE: Scan/Interventions/Mainbank need their "vs Target" values. Gemini is asked to extract the core metric, but we need the 'vs Target' numbers too.
            # We will use a dedicated prompt for this:
            prompt_map_sales_vs = {
                "Scan Rate vs Target": "scan_vs_target", 
                "Interventions vs Target": "interventions_vs_target", 
                "Mainbank Closed vs Target": "mainbank_vs_target",
            }
            
            system_inst_sales = "You are a specialist retail data extractor. Extract the main numeric scores/values/percentages next to the following labels on the Sales/Front End dashboard. For monetary values, include K or M if present. For Scan/Interventions/Mainbank, look for the 'vs Target' value (e.g., if you see '-1.3' next to 'Scan Rate vs Target', return -1.3)."
            
            sales_metrics = _extract_gemini_vision(screenshot_path_sales, prompt_map_sales, system_inst_sales)
            vs_metrics = _extract_gemini_vision(screenshot_path_sales, prompt_map_sales_vs, system_inst_sales)
            
            all_metrics.update(sales_metrics)
            all_metrics.update(vs_metrics)
            
            # --- STEP 4: Combine with default values for unextracted metrics ---
            # Re-initialize all required keys to '-' if they were not found in any of the steps.
            metrics_to_default = [
                "sales_total", "sales_vs_target", "scan_rate", "interventions", 
                "mainbank_closed", "payroll_outturn", "absence_outturn", 
                "productive_outturn", "holiday_outturn", "current_base_cost",
                "sco_utilisation", "efficiency", "moa", "waste_validation",
                "unrecorded_waste_pct", "shrink_vs_budget_pct", "weekly_activity",
                "scan_vs_target", "interventions_vs_target", "mainbank_vs_target",
                # NPS detail keys that may be missing
                "cafe_nps", "click_collect_nps", "customer_toilet_nps", "home_delivery_nps"
            ]
            
            for key in metrics_to_default:
                if key not in all_metrics or not all_metrics[key] or all_metrics[key] == 'null':
                     all_metrics[key] = "—" # Set to guaranteed blank if not found

        finally:
            try:
                if context: context.close()
            except Exception:
                pass
            try:
                if browser: browser.close()
            except Exception:
                pass

    ok = send_card(all_metrics)
    log.info("Daily card send → %s", "OK" if ok else "FAIL")
    write_csv(all_metrics)
