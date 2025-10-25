#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Retail Performance Dashboard → Daily Summary (layout-by-lines + ROI OCR) → Google Chat

Key points in this build:
- Sales 'Total' row: FIRST 'Total' AFTER the 'Sales' header → capture next 3 numeric tokens.
- Front End Service: scoped values + correctly paired "vs Target" per KPI.
- Online/Complaints/Payroll/Shrink/Card Engagement: tuned line parsing search windows.
- Availability: prefer a % within 3 lines ABOVE the label.
- C&C average wait: expanded search window for time.
- Waste & Markdowns: robust block regex (Total row with (+/-) and (+/-)%).
- Updated and CORRECTED DEFAULT_ROI_MAP for accurate OCR fallback on gauges and key tiles.
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
    "https://lookerstudio.google.com/embed/u/0/reporting/"
    "d93a03c7-25dc-439d-abaa-dd2f3780daa5/page/BLfDE"
    "?params=%7B%22f20f0n9kld%22:%22include%25EE%2580%25803%25EE%2580%2580T%22%7D"
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

MAIN_WEBHOOK  = config["DEFAULT"].get("DAILY_WEBHOOK") or config["DEFAULT"].get("MAIN_WEBHOOK", os.getenv("MAIN_WEBHOOK", ""))
ALERT_WEBHOOK = config["DEFAULT"].get("ALERT_WEBHOOK",  os.getenv("ALERT_WEBHOOK", ""))
CI_RUN_URL    = os.getenv("CI_RUN_URL", "")

# ──────────────────────────────────────────────────────────────────────────────
# Helpers: Chat + file saves
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
                time.sleep(delay)
                backoff = min(backoff * 1.7, max_backoff)
                continue
            log.error(f"Webhook error {r.status_code}: {r.text[:300]}")
            return False
        except Exception as e:
            log.error(f"Webhook exception: {e}")
            time.sleep(backoff)
            backoff = min(backoff * 1.7, max_backoff)

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
        body = page.inner_text("body")
    except Exception:
        body = ""
    if "You are about to interact with a community visualisation" in body:
        log.info("Community visualisation placeholders detected — retrying PROCEED and waiting longer.")
        click_proceed_overlays(page)
        page.wait_for_timeout(1500)

    return True

# ──────────────────────────────────────────────────────────────────────────────
# Screenshot helper
# ──────────────────────────────────────────────────────────────────────────────
def screenshot_full(page) -> Optional["Image.Image"]:
    try:
        img_bytes = page.screenshot(full_page=True, type="png")
        ts = int(time.time())
        save_bytes(SCREENS_DIR / f"{ts}_fullpage.png", img_bytes)
        from PIL import Image  # lazy import
        return Image.open(BytesIO(img_bytes))
    except Exception as e:
        log.error(f"Full-page screenshot failed: {e}")
        return None

# ──────────────────────────────────────────────────────────────────────────────
# Text parsing (layout-by-lines) — Deterministic rules (SCOPED)
# ──────────────────────────────────────────────────────────────────────────────
NUM_ANY_RE   = re.compile(r"[£]?-?\d+(?:\.\d+)?(?:[KMB]|%)?", re.I)
NUM_INT_RE   = re.compile(r"\b-?\d+\b")
NUM_PCT_RE   = re.compile(r"-?\d+(?:\.\d+)?%")
# allow both "£-8K" and "-£8K"
NUM_MONEY_RE = re.compile(r"(?:-?\s*£|£\s*-?)\s*\d+(?:\.\d+)?[KMB]?", re.I)
TIME_RE      = re.compile(r"\b\d{1,2}:\d{2}\b")

EMAILLOC = re.compile(r"([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}).*?\|\s*([^\|]+?)\s*\|\s*(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})", re.S)
PERIOD_RE= re.compile(r"The data on this report is from:\s*([^\n]+)")
STAMP_RE = re.compile(r"\b(\d{1,2}\s+[A-Za-z]{3}\s+\d{4},\s*\d{2}:\d{2}:\d{2})\b")

def get_body_text(page) -> str:
    best, best_len = "", 0
    try:
        t = page.inner_text("body")
        if t and len(t) > best_len:
            best, best_len = t, len(t)
    except Exception:
        pass
    for fr in page.frames:
        try:
            fr.wait_for_selector("body", timeout=3000)
            t = fr.locator("body").inner_text(timeout=5000)
            if t and len(t) > best_len:
                best, best_len = t, len(t)
        except Exception:
            continue
    return best

def dump_numbered_lines(txt: str) -> List[str]:
    lines = [ln.rstrip() for ln in txt.splitlines()]
    ts = int(time.time())
    numbered = "\n".join(f"{i:04d} | {ln}" for i, ln in enumerate(lines))
    save_text(SCREENS_DIR / f"{ts}_lines.txt", numbered)
    return lines

def _contains_num_of_type(s: str, kind: str) -> Optional[str]:
    if kind == "time":
        m = TIME_RE.search(s); return m.group(0) if m else None
    if kind == "percent":
        m = NUM_PCT_RE.search(s); return m.group(0) if m else None
    if kind == "integer":
        m = NUM_INT_RE.search(s); return m.group(0) if m else None
    if kind == "money":
        m = NUM_MONEY_RE.search(s)
        if m: return re.sub(r"\s+", "", m.group(0))  # tidy spaces
        m2 = re.search(r"-?\d+(?:\.\d+)?[KMB]?", s, re.I); return m2.group(0) if m2 else None
    m = NUM_ANY_RE.search(s); return m.group(0) if m else None

def _idx(lines: List[str], needle: str, start=0, end=None) -> int:
    end = len(lines) if end is None else end
    nl = needle.lower()
    for i in range(start, end):
        if nl in lines[i].lower():
            return i
    return -1

def _scope_end(lines: List[str], starts: List[int], fallback_end: int) -> int:
    nxt = [i for i in starts if i >= 0]
    return min(nxt) if nxt else fallback_end

def section_bounds(lines: List[str], start_anchor: str, candidate_next: List[str]) -> Tuple[int,int]:
    s = _idx(lines, start_anchor)
    if s < 0: return -1, -1
    next_idxs = [ _idx(lines, a, s+1) for a in candidate_next ]
    e = _scope_end(lines, next_idxs, len(lines))
    return s, e

def value_near_scoped(lines: List[str], label: str, kind: str, scope: Tuple[int,int], *, near_before=6, near_after=6, prefer_before_first=0) -> str:
    s, e = scope
    if s < 0: return "—"
    li = _idx(lines, label, s, e)
    if li < 0: return "—"
    # bias: prefer hits just above the label (Availability 84%)
    if prefer_before_first > 0:
        for i in range(max(s, li - prefer_before_first), li):
            v = _contains_num_of_type(lines[i], kind)
            if v: return v
    # after the label
    for i in range(li+1, min(e, li+1+near_after)):
        v = _contains_num_of_type(lines[i], kind)
        if v: return v
    # before the label
    for i in range(max(s, li - near_before), li):
        v = _contains_num_of_type(lines[i], kind)
        if v: return v
    return "—"

def sales_three_after_total(lines: List[str]) -> Optional[Tuple[str,str,str]]:
    """‘Sales’ → first ‘Total’ after → next 3 numeric tokens across following lines."""
    i_sales = _idx(lines, "Sales", start=0)
    if i_sales < 0:
        return None
    for i in range(i_sales + 1, min(len(lines), i_sales + 200)):
        if lines[i].strip().lower() == "total":
            collected: List[str] = []
            for j in range(i + 1, min(len(lines), i + 40)):
                toks = NUM_ANY_RE.findall(lines[j])
                for t in toks:
                    collected.append(t)
                    if len(collected) == 3:
                        return collected[0], collected[1], collected[2]
            break
    return None

# NEW: safe "coalesce" to handle "—" being truthy
def coalesce(*vals: str) -> str:
    """Return the first value that isn't empty and isn't '—'."""
    for v in vals:
        if v and v != "—":
            return v
    return "—"

# FES helpers (scoped KPI value + correctly paired vs Target)
def _fes_value(lines: List[str], label: str, num_type: str, scope: Tuple[int,int]) -> str:
    s, e = scope
    if s < 0: return "—"
    li = _idx(lines, label, s, e)
    if li < 0: return "—"
    kpi_labels = ["Sco Utilisation","SCO Utilisation","Efficiency","Scan Rate","Interventions","Mainbank Closed"]
    next_labels = [ _idx(lines, l, li+1, e) for l in kpi_labels ]
    bound = _scope_end(lines, next_labels, e)
    vsi = _idx(lines, "vs Target", li+1, e)
    limit = min([x for x in [bound, vsi if vsi >= 0 else e] if x > li], default=e)
    # typed search right after label; if nothing, peek one line above (some tiles render above)
    # after
    for i in range(li+1, min(limit, li+1+8)):
        v = _contains_num_of_type(lines[i], num_type)
        if v: return v
    # one line above fallback
    if li-1 >= s:
        v = _contains_num_of_type(lines[li-1], num_type)
        if v: return v
    # outward bounded
    for i in range(li+1, limit):
        v = _contains_num_of_type(lines[i], num_type)
        if v: return v
    for i in range(max(s, li-3), li):
        v = _contains_num_of_type(lines[i], num_type)
        if v: return v
    return "—"

def _fes_vs(lines: List[str], label: str, scope: Tuple[int,int]) -> str:
    s, e = scope
    if s < 0: return "—"
    li = _idx(lines, label, s, e)
    if li < 0: return "—"
    vsi = _idx(lines, "vs Target", li+1, e)
    if vsi < 0: return "—"
    kpi_labels = ["Sco Utilisation","SCO Utilisation","Efficiency","Scan Rate","Interventions","Mainbank Closed"]
    next_labels = [ _idx(lines, l, li+1, min(e, li+40)) for l in kpi_labels ]
    bound = _scope_end(lines, next_labels, min(e, li+40))
    if vsi >= bound:
        return "—"
    # limited window after vs Target
    for i in range(vsi+1, min(vsi+3, bound)):
        v = _contains_num_of_type(lines[i], "any")
        if v: return v
    return "—"

def parse_from_lines(lines: List[str]) -> Dict[str, str]:
    m: Dict[str, str] = {}

    # Context
    joined = "\n".join(lines)
    z = EMAILLOC.search(joined); m["store_line"]    = z.group(0).strip() if z else ""
    y = PERIOD_RE.search(joined); m["period_range"] = y.group(1).strip() if y else "—"
    x = STAMP_RE.search(joined);  m["page_timestamp"]= x.group(1) if x else "—"

    # ── Section scopes ────────────────────────────────────────────────────────
    FES_SCOPE     = section_bounds(lines, "Front End Service",
                                   ["More Card Engagement","Card Engagement","Production Planning","Online","Waste & Markdowns","Shrink","Payroll","Privacy"])
    ONLINE_SCOPE  = section_bounds(lines, "Online",
                                   ["Front End Service","More Card Engagement","Card Engagement","Waste & Markdowns","Shrink","Payroll","Privacy"])
    PAYROLL_SCOPE = section_bounds(lines, "Payroll",
                                   ["Online","Front End Service","More Card Engagement","Card Engagement","Waste & Markdowns","Shrink","Privacy"])
    SHRINK_SCOPE  = section_bounds(lines, "Shrink",
                                   ["Waste & Markdowns","My Reports","Payroll","Online","Front End Service","More Card Engagement","Card Engagement","Privacy"])
    CARD_SCOPE    = section_bounds(lines, "More Card Engagement",
                                   ["Payroll","Online","Front End Service","Waste & Markdowns","Shrink","Privacy"])
    PP_SCOPE      = section_bounds(lines, "Production Planning",
                                   ["More Card Engagement","Card Engagement","Payroll","Shrink","Privacy"])
    COMPLAINTS_SCOPE = section_bounds(lines, "Customer Complaints",
                                   ["Production Planning","More Card Engagement","Card Engagement","Payroll","Shrink","Privacy"])
    CLEAN_ROTATE_SCOPE = section_bounds(lines, "Clean & Rotate",
                                   ["My Reports","More Card Engagement","Card Engagement","Payroll","Privacy"])

    # ── Sales (triple after 'Total') ──────────────────────────────────────────
    res = sales_three_after_total(lines)
    if res:
        m["sales_total"], m["sales_lfl"], m["sales_vs_target"] = res
    else:
        m["sales_total"] = m["sales_lfl"] = m["sales_vs_target"] = "—"

    # ── Waste & Markdowns (robust Total row regex) ───────────────────────────
    pivot = _idx(lines, "(+/-)%")
    if pivot < 0:
        pivot = _idx(lines, "Waste & Markdowns")
    if pivot >= 0:
        s = max(0, pivot - 60); e = min(len(lines), pivot + 80)
        window = "\n".join(lines[s:e])
        r = re.search(
            r"Total\s*\n\s*(" + NUM_ANY_RE.pattern + r")\s*\n\s*(" + NUM_ANY_RE.pattern + r")\s*\n\s*(" + NUM_ANY_RE.pattern + r")\s*\n\s*(" + NUM_ANY_RE.pattern + r")\s*\n\s*(" + NUM_ANY_RE.pattern + r")",
            window, flags=re.I
        )
        if r:
            m["waste_total"], m["markdowns_total"], m["wm_total"], m["wm_delta"], m["wm_delta_pct"] = \
                r.group(1), r.group(2), r.group(3), r.group(4), r.group(5)
        else:
            m.update({k: "—" for k in ["waste_total","markdowns_total","wm_total","wm_delta","wm_delta_pct"]})
    else:
        m.update({k: "—" for k in ["waste_total","markdowns_total","wm_total","wm_delta","wm_delta_pct"]})

    # ── Front End Service (scoped) ───────────────────────────────────────────
    # Use coalesce to handle "Sco" vs "SCO" variants (since "—" is truthy)
    m["sco_utilisation"]         = coalesce(
        _fes_value(lines, "Sco Utilisation", "percent", FES_SCOPE),
        _fes_value(lines, "SCO Utilisation", "percent", FES_SCOPE),
    )
    m["efficiency"]              = _fes_value(lines, "Efficiency",      "percent", FES_SCOPE)
    m["scan_rate"]               = _fes_value(lines, "Scan Rate",       "integer", FES_SCOPE)
    m["interventions"]           = _fes_value(lines, "Interventions",   "integer", FES_SCOPE)
    m["mainbank_closed"]         = _fes_value(lines, "Mainbank Closed", "integer", FES_SCOPE)
    m["scan_vs_target"]          = _fes_vs(lines, "Scan Rate",       FES_SCOPE)
    m["interventions_vs_target"] = _fes_vs(lines, "Interventions",   FES_SCOPE)
    m["mainbank_vs_target"]      = _fes_vs(lines, "Mainbank Closed", FES_SCOPE)

    # ── Online (scoped; Availability prefers 3 lines above) ──────────────────
    m["availability_pct"]   = value_near_scoped(lines, "Availability",       "percent", ONLINE_SCOPE, near_before=6,  near_after=10, prefer_before_first=3)
    m["despatched_on_time"] = value_near_scoped(lines, "Despatched on Time", "percent", ONLINE_SCOPE, near_before=8,  near_after=12)
    m["delivered_on_time"]  = value_near_scoped(lines, "Delivered on Time",  "percent", ONLINE_SCOPE, near_before=8,  near_after=12)
    # Increased search window slightly as '15:12' is far from the label
    m["cc_avg_wait"]        = value_near_scoped(lines, "average wait",       "time",    ONLINE_SCOPE, near_before=15, near_after=20)

    # ── Payroll (scoped) ─────────────────────────────────────────────────────
    # Increased 'near_after' window to catch values if the label is at the top of the scope
    m["payroll_outturn"]    = value_near_scoped(lines, "Payroll Outturn",    "any", PAYROLL_SCOPE, near_before=4, near_after=8)
    m["absence_outturn"]    = value_near_scoped(lines, "Absence Outturn",    "any", PAYROLL_SCOPE, near_before=4, near_after=8)
    m["productive_outturn"] = value_near_scoped(lines, "Productive Outturn", "any", PAYROLL_SCOPE, near_before=4, near_after=8)
    m["holiday_outturn"]    = value_near_scoped(lines, "Holiday Outturn",    "any", PAYROLL_SCOPE, near_before=4, near_after=8)
    m["current_base_cost"]  = value_near_scoped(lines, "Current Base Cost",  "any", PAYROLL_SCOPE, near_before=4, near_after=8)

    # ── Card Engagement (scoped) ─────────────────────────────────────────────
    # Increased 'near_after' window to catch values if the label is at the top of the scope
    m["swipe_rate"]      = value_near_scoped(lines, "Swipe Rate",    "percent", CARD_SCOPE, near_before=4, near_after=8)
    m["swipes_wow_pct"]  = value_near_scoped(lines, "Swipes WOW",    "percent", CARD_SCOPE, near_before=4, near_after=8)
    m["new_customers"]   = value_near_scoped(lines, "New Customers", "integer", CARD_SCOPE, near_before=6, near_after=10)
    m["swipes_yoy_pct"]  = value_near_scoped(lines, "Swipes YOY",    "percent", CARD_SCOPE, near_before=6, near_after=10)

    # ── Production Planning (scoped) ─────────────────────────────────────────
    m["data_provided"] = value_near_scoped(lines, "Data Provided", "percent", PP_SCOPE, near_before=6, near_after=8)
    m["trusted_data"]  = value_near_scoped(lines, "Trusted Data",  "percent", PP_SCOPE, near_before=6, near_after=8)

    # ── Shrink (scoped + strict types) ───────────────────────────────────────
    # Values appear near the bottom of the section, so increased 'near_after' slightly
    m["moa"]                  = value_near_scoped(lines, "Morrisons Order Adjustments", "money",   SHRINK_SCOPE, near_before=10, near_after=12)
    m["waste_validation"]     = value_near_scoped(lines, "Waste Validation",            "percent", SHRINK_SCOPE, near_before=10, near_after=12)
    m["unrecorded_waste_pct"] = value_near_scoped(lines, "Unrecorded Waste",            "percent", SHRINK_SCOPE, near_before=10, near_after=12)
    m["shrink_vs_budget_pct"] = value_near_scoped(lines, "Shrink vs Budget",            "percent", SHRINK_SCOPE, near_before=10, near_after=12)

    # ── Complaints / My Reports (scoped) ─────────────────────────────────────
    comp_scope = COMPLAINTS_SCOPE if COMPLAINTS_SCOPE[0] >= 0 else (0, len(lines))
    m["complaints_key"] = value_near_scoped(lines, "Key Customer Complaints", "integer", comp_scope, near_before=10, near_after=12)
    m["my_reports"]     = value_near_scoped(lines, "My Reports", "integer",
                         section_bounds(lines, "My Reports", ["Cafe NPS","Privacy","Payroll","Shrink","Waste & Markdowns"]),
                         near_before=6, near_after=10)

    # ── Weekly Activity — preserve literal “No data” in Clean & Rotate scope ─
    m["weekly_activity"] = "—"
    s,e = CLEAN_ROTATE_SCOPE
    if s >= 0:
        li = _idx(lines, "Weekly Activity", s, e)
        if li >= 0:
            window = lines[max(s, li-2):min(e, li+6)]
            if any("No data" in w for w in window):
                m["weekly_activity"] = "No data"
            else:
                m["weekly_activity"] = value_near_scoped(lines, "Weekly Activity", "any", (s,e), near_before=4, near_after=6)

    # Gauges via ROI OCR later
    for k in ["supermarket_nps","colleague_happiness","home_delivery_nps","cafe_nps","click_collect_nps","customer_toilet_nps"]:
        m.setdefault(k, "—")

    return m

# ──────────────────────────────────────────────────────────────────────────────
# ROI OCR fallback (UPDATED with CORRECTED coordinates)
# ──────────────────────────────────────────────────────────────────────────────
DEFAULT_ROI_MAP = {
    # Gauges row (CORRECTED)
    "colleague_happiness": (0.252, 0.230, 0.060, 0.040),
    "supermarket_nps":     (0.402, 0.230, 0.050, 0.040),
    "cafe_nps":            (0.552, 0.230, 0.050, 0.040),
    "click_collect_nps":   (0.702, 0.230, 0.050, 0.040),
    "home_delivery_nps":   (0.852, 0.230, 0.050, 0.040),
    "customer_toilet_nps": (0.950, 0.230, 0.050, 0.040),

    # Waste & Markdowns TOTAL row cells (Original, kept as backup)
    "waste_total":     (0.105, 0.415, 0.065, 0.035),
    "markdowns_total": (0.170, 0.415, 0.065, 0.035),
    "wm_total":        (0.235, 0.415, 0.065, 0.035),
    "wm_delta":        (0.300, 0.415, 0.065, 0.035),
    "wm_delta_pct":    (0.365, 0.415, 0.065, 0.035),

    # Online (CORRECTED)
    "availability_pct":   (0.480, 0.770, 0.050, 0.040),
    "despatched_on_time": (0.515, 0.585, 0.085, 0.055), 
    "delivered_on_time":  (0.585, 0.585, 0.085, 0.055),
    "cc_avg_wait":        (0.620, 0.770, 0.065, 0.040),
    
    # Payroll (ADDED for robust fallback)
    "payroll_outturn":    (0.457, 0.485, 0.065, 0.040),
    "absence_outturn":    (0.535, 0.485, 0.065, 0.040),
    "productive_outturn": (0.535, 0.540, 0.065, 0.040),
    "holiday_outturn":    (0.615, 0.485, 0.065, 0.040),
    "current_base_cost":  (0.615, 0.540, 0.065, 0.040),
    
    # Shrink (ADDED for robust fallback)
    "moa":                  (0.250, 0.785, 0.085, 0.040),
    "waste_validation":     (0.375, 0.785, 0.060, 0.040),
    "unrecorded_waste_pct": (0.435, 0.785, 0.060, 0.040),
    "shrink_vs_budget_pct": (0.495, 0.785, 0.060, 0.040),

    # Front End Service (Original, kept as backup)
    "sco_utilisation": (0.680, 0.590, 0.065, 0.060),
    "efficiency":      (0.940, 0.585, 0.090, 0.120),
    "scan_rate":       (0.680, 0.655, 0.065, 0.050),
    "interventions":   (0.810, 0.590, 0.065, 0.060),
    "mainbank_closed": (0.810, 0.655, 0.065, 0.050),

    # Card Engagement (ADDED for robust fallback)
    "new_customers": (0.742, 0.538, 0.060, 0.035),
}

def load_roi_map() -> Dict[str, Tuple[float,float,float,float]]:
    roi = DEFAULT_ROI_MAP.copy()
    try:
        # NOTE: If roi_map.json exists, it OVERRIDES the DEFAULT_ROI_MAP.
        # Ensure it is either EMPTY, deleted, or contains ONLY the required entries 
        # (like "sales_lfl") to avoid overriding the corrected coordinates above.
        if ROI_MAP_FILE and Path(ROI_MAP_FILE).exists():
            overrides = json.loads(Path(ROI_MAP_FILE).read_text(encoding="utf-8"))
            if overrides:
                roi.update(overrides)
                log.info(f"Loaded ROI overrides from roi_map.json: {len(overrides)} entrie(s).")
    except Exception as e:
        log.warning(f"Could not read roi_map.json: {e}")
    return roi

def crop_norm(img: "Image.Image", roi: Tuple[float,float,float,float]) -> "Image.Image":
    from PIL import Image  # type: ignore
    W, H = img.size
    x, y, w, h = roi
    box = (int(x*W), int(y*H), int((x+w)*W), int((y+h)*H))
    return img.crop(box)

def ocr_cell(img: "Image.Image", want_time=False, allow_percent=True) -> str:
    if not OCR_AVAILABLE:
        return "—"
    try:
        w, h = img.size
        if max(w, h) < 240:
            img = img.resize((int(w*2), int(h*2)))
        # Use simple PSM for single block of text/number
        txt = pytesseract.image_to_string(img, config="--psm 7") 
        if want_time:
            m = TIME_RE.search(txt)
            if m: return m.group(0)
        m = re.search(r"[£]?-?\d+(?:\.\d+)?[KMB]?", txt, flags=re.I)
        if m and m.group(0): return m.group(0)
        if allow_percent:
            m = re.search(r"-?\d+(?:\.\d+)?%", txt)
            if m and m.group(0): return m.group(0)
        m = re.search(r"\b-?\d{1,3}\b", txt)
        if m and m.group(0): return m.group(0)
        # Catch case where OCR reads '-' for missing NPS gauges
        if re.search(r"^\s*-\s*$", txt.strip()):
            return "—"
    except Exception:
        pass
    return "—"

def draw_overlay(img: "Image.Image", roi_map: Dict[str, Tuple[float,float,float,float]]):
    try:
        from PIL import ImageDraw  # type: ignore
        dbg = img.copy()
        draw = ImageDraw.Draw(dbg)
        W, H = dbg.size
        for key, (x,y,w,h) in roi_map.items():
            box = (int(x*W), int(y*H), int((x+w)*W), int((y+h)*H))
            draw.rectangle(box, outline=(0,255,0), width=2)
            draw.text((box[0]+3, box[1]+3), key, fill=(0,255,0))
        ts = int(time.time())
        outfile = SCREENS_DIR / f"{ts}_roi_overlay.png"
        dbg.save(outfile)
        log.info(f"ROI overlay saved → {outfile.name}")
    except Exception:
        pass

def fill_missing_with_roi(metrics: Dict[str, str], img: Optional["Image.Image"]):
    if img is None:
        return
    roi_map = load_roi_map()
    used = False
    for key, roi in roi_map.items():
        if metrics.get(key) and metrics[key] != "—":
            continue
        want_time = (key == "cc_avg_wait")
        allow_percent = not key.endswith("_nps")
        val = ocr_cell(crop_norm(img, roi), want_time=want_time, allow_percent=allow_percent)
        if val and val != "—":
            metrics[key] = val
            used = True
    if used:
        draw_overlay(img, roi_map)
        log.info("Filled some missing values from ROI OCR.")

# ──────────────────────────────────────────────────────────────────────────────
# Card + CSV
# ──────────────────────────────────────────────────────────────────────────────
def kv(label: str, val: str) -> dict:
    return {"decoratedText": {"topLabel": label, "text": (val or "—")}}

def title_widget(text: str) -> dict:
    return {"textParagraph": {"text": f"<b>{text}</b>"}}

def build_chat_card(metrics: Dict[str, str]) -> dict:
    header = {
        "title": "📊 Retail Daily Summary (Layout+ROI OCR)",
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

def write_csv(metrics: Dict[str,str]):
    write_header = not DAILY_LOG_CSV.exists() or DAILY_LOG_CSV.stat().st_size == 0
    with open(DAILY_LOG_CSV, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if write_header:
            w.writerow(CSV_HEADERS)
        w.writerow([metrics.get(h, "—") for h in CSV_HEADERS])
    log.info(f"Appended daily metrics row to {DAILY_LOG_CSV.name}")

def send_card(metrics: Dict[str, str]) -> bool:
    if not MAIN_WEBHOOK or "chat.googleapis.com" not in MAIN_WEBHOOK:
        log.error("MAIN_WEBHOOK/DAILY_WEBHOOK missing or invalid — cannot send daily report.")
        return False
    return _post_with_backoff(MAIN_WEBHOOK, build_chat_card(metrics))

# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────
def run_daily_scrape():
    if not AUTH_STATE.exists():
        alert(["⚠️ Daily dashboard scrape needs login. Run `python scrape.py now` once to save auth_state.json."])
        log.error("auth_state.json not found.")
        return

    from PIL import Image  # ensure PIL available for type refs
    with sync_playwright() as p:
        browser = context = page = None
        metrics: Dict[str,str] = {}
        screenshot: Optional[Image.Image] = None
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

            # Screenshot for ROI + debugging
            screenshot = screenshot_full(page)

            # BODY TEXT → numbered lines → layout parser
            body_text = get_body_text(page)
            lines = dump_numbered_lines(body_text)
            metrics = parse_from_lines(lines)

            # Fill stubborn tiles with ROI OCR (esp. gauges / online wait)
            fill_missing_with_roi(metrics, screenshot)

        finally:
            try:
                if context: context.close()
            except Exception:
                pass
            try:
                if browser: browser.close()
            except Exception:
                pass

    ok = send_card(metrics)
    log.info("Daily card send → %s", "OK" if ok else "FAIL")
    write_csv(metrics)

if __name__ == "__main__":
    run_daily_scrape()
