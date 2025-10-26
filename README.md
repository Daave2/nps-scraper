📖 Looker Studio Scraper Suite: Full Technical Manual
1. Overview and Architecture
This project is a multi-script, scheduled scraping solution designed to extract business-critical data from Google Looker Studio reports and deliver formatted, actionable alerts to Google Chat. It leverages headless browsing (Playwright) and AI-powered image analysis (Gemini Vision) to ensure robust data capture.
File Manifest
File	Role	Execution	Logic Focus
scrape_daily.py	Daily Summary Report Generator	Daily (8:30 AM UTC)	Gemini Vision for visual metrics, Conditional Logic, Custom HTML Card Formatting.
scrape.py	NPS Comment Scraper	Hourly (Multiple times per day)	2FA Handling, Comment Parsing, Batched Chat Sending, Deduplication.
scrape_complaints.py	Customer Complaints Scraper	Hourly (Multiple times per day)	State-Machine Parsing, Case Number Deduplication, Per-Complaint Card Alerting.
.github/workflows/...	GitHub Actions Workflow	Scheduled / Manual	Caching, Authentication Fallback, Conditional Run Types, Environment Setup.
auth_state.json	Shared Resource	Stores authenticated browser session cookie (critical for bypassing repeated login).	
comments_log.csv	Shared Resource	Logs unique NPS comments to prevent duplicates.	
complaints_log.csv	Shared Resource	Logs unique complaint case numbers to prevent duplicates.	
2. Configuration and Secrets
The system relies entirely on environment variables exposed via GitHub Secrets for security and operational settings.
Secret Name	Purpose & Script Usage
GOOGLE_EMAIL	The credential for accessing the Looker Studio reports. Used by scrape.py and scrape_complaints.py for login.
GOOGLE_PASSWORD	The password for the Google account. Used only during the initial authentication and when auth_state.json expires.
GEMINI_API_KEY	Critical for scrape_daily.py. Provides access to the Gemini Pro Vision model for reading visual metrics (dials, charts, custom indicators).
MAIN_WEBHOOK	Target URL for general alerts and batched NPS comments (scrape.py).
ALERT_WEBHOOK	Target URL for high-priority alerts: login failures, 2FA codes, and critical script errors.
DAILY_WEBHOOK	Target URL for the comprehensive daily summary card (scrape_daily.py).
COMPLAINTS_WEBHOOK	Target URL for new complaint notifications (scrape_complaints.py).
AUTH_STATE_B64	(Optional, Maintenance Fallback) Base64-encoded string of a successful auth_state.json file. Used by the workflow for initial setup or cache failure recovery.
3. Manual Operational Guide: Initial Login & 2FA
The first successful run is a manual process. It creates the auth_state.json file, which is then cached by the GitHub workflow for all subsequent runs. This process handles Google's 2FA challenge.
Procedure (First Run / After Authentication Expiry)
Prepare: Ensure all required GitHub Secrets (GOOGLE_EMAIL, GOOGLE_PASSWORD, ALERT_WEBHOOK) are set.
Trigger Headed Login: Navigate to the Actions tab on GitHub and run the main workflow manually (workflow_dispatch).
Wait for Alert: The scrape.py process will execute its authentication attempt. Since 2FA is likely enabled, it will pause.
Intervene (2FA Challenge):
The script runs the wait_for_2fa_and_alert function.
It uses screen-scraping logic and regex (_extract_number_from_body, RE_TWO_OR_THREE) to identify the number Google displays on the login screen.
It immediately sends an alert to the ALERT_WEBHOOK containing the number, e.g.: 🔐 Tap this number on your phone: **42**.
Approve: On your mobile device, you must approve the sign-in request and tap the matching number (e.g., '42').
Completion: Once approved, the Playwright session continues, saves the state to auth_state.json, and the workflow caches it. All subsequent automated runs will use this file until the session cookie expires.
4. Deep Dive: Python Script Logic
A. scrape_daily.py (Retail Daily Summary)
This is the most complex script, responsible for the definitive daily summary card.
4.1. Core Logic Flow
Setup & Navigation: Launches Playwright, restores auth_state.json, navigates to DASHBOARD_URL.
Stabilization (open_and_prepare): Executes an extended wait (20s) followed by attempts to dismiss "PROCEED" overlays (common in Looker Studio embeds) and a final 5-second buffer wait before capture.
Data Capture: Takes a full-page screenshot and extracts page text (body_text).
Initial Parsing (parse_from_lines): Uses regex and section bounding logic to extract all possible metrics from the raw page text, setting failures to "—".
AI Validation (extract_gemini_metrics):
Iterates over metrics that failed initial parsing (i.e., those set to "—") and all visual metrics (GEMINI_METRICS).
Sends the full screenshot and a targeted prompt (listing required metrics) to Gemini Pro Vision.
The AI returns a JSON object, which overwrites the metrics dictionary with the visually extracted data.
Card Generation (build_chat_card): Dynamically constructs the Chat Card payload.
4.2. Formatting and Filtering Logic (Crucial)
Function	Logic Mechanism	Explanation
METRIC_TARGETS	Dictionary (key: (target, rule))	The central data store for all formatting rules (e.g., "sales_lfl": ("0", "A>2 G, A<-2 R")).
get_status_formatting	Rule Parsing & Prioritization (G > R > O > BR)	Takes metric and value, checks against rules, and returns the highest priority formatting tuple (prefix, suffix).
STATUS_FORMAT	Custom HTML Tags	Uses the observed, unofficial <font color='...'> for visual distinction: Red (#FF0000) for R/BR and Amber (#FFA500) for O. Green is plain text ("").
_create_metric_widget	Conditional Filtering	Checks: val is None, val.strip() == "", val.strip() == "—", val.strip() == "-", or val.upper() == "NPS". If any are true, it returns None, skipping the widget.
build_chat_card	Section Filtering	final_sections.append(section_dict) only occurs if widgets is not empty, ensuring blank sections are completely removed from the final card.
B. scrape_complaints.py (Customer Complaints)
This script manages case-by-case alerting for new complaints.
5.1. Core Logic Flow
Read Existing: Reads complaints_log.csv to load existing case_numbers into a Python set for fast lookup.
Navigation & Extraction: Performs a headless scrape of the complaints report using copy_looker_studio_text.
State Machine Parsing (parse_complaints_from_lines):
Looker Studio dumps data sequentially without clear delimiters. This function maintains a state variable (LOOKING_FOR_START, FOUND_DATE, READING_DESC, READING_RESPONSE).
It progresses based on recognized markers (e.g., date, case number, or end markers like "Respond" / "under review"). This logic is brittle but necessary for this report type.
Deduplication: Filters parsed complaints by checking if case_number exists in the existing set.
Alerting & Logging: For every new complaint:
Sends a richly formatted Google Chat Card containing Case ID, Reason, truncated Description (700 chars), and Response (500 chars).
Appends the case_number to complaints_log.csv.
C. scrape.py (NPS Comments)
This script is dedicated to handling the high-volume stream of NPS feedback.
6.1. Core Logic Flow
Internal Scrape: _scrape_internal attempts to fetch the Looker Studio page, returning RELOGIN_REQUIRED if the session is bad.
Auth Retry: The run_scrape function manages a 2-attempt loop: if the first attempt fails due to login, it triggers the headless login flow and retries once.
Parsing (parse_comments_from_lines): Uses the unique signature of the comments data (DATE -> STORE -> [COMMENT LINES] -> SCORE) to stitch together discrete records. It relies heavily on line sequence and content patterns (DATE_PATTERN, SCORE_PATTERN, STORE_PATTERN) and noise filtering (SKIP_PATTERN).
Deduplication: Uses the combination of (store, timestamp, comment) as the unique key to prevent reposting.
Rate Limiting & Batching:
Limits total comments sent per run (MAX_COMMENTS_PER_RUN).
Sends comments in batches of BATCH_SIZE (10) to prevent webhook rate-limit violations.
Uses a base/exponential backoff (_post_with_backoff) to handle temporary 429 errors from Google Chat.
5. Deep Dive: GitHub Actions Workflow
The workflow (.github/workflows/main.yml) orchestrates the execution and environment.
5.1. Triggers and Concurrency
Schedule: Triggers are set by cron expressions, enabling:
Full Run: 30 8 * * * (8:30 AM UTC) for all three scripts, including scrape_daily.py.
Partial Runs: 0 11,13,15,17,19,21 * * * (Hourly, every two hours) for only scrape.py and scrape_complaints.py.
workflow_dispatch: Allows manual runs (always triggers a "Full Run").
concurrency: Ensures only one job runs at a time (cancel-in-progress: false means a queued run will wait, preventing race conditions on the cache and lock files).
5.2. Setup and Environment
Dependencies: Installs Python 3.12, playwright, google-genai, and system dependencies for Chromium (python -m playwright install --with-deps chromium).
Environment Variables: Exports GitHub-native variables (CI_RUN_URL, TODAY) and the critical secrets (e.g., GEMINI_API_KEY) to the shell environment.
Authentication and Logging Cache (actions/cache/restore):
Saves/restores shared state files: auth_state.json, comments_log.csv, complaints_log.csv, daily_report_log.csv.
Uses a key structure that defaults to the latest successful run if the daily key is missed, ensuring the login state is almost always available.
config.ini Creation: Populates a local config.ini with all secrets before script execution, centralizing the configuration for the Python files.
5.3. Conditional Execution
determine_run_type: This step uses the GitHub event context (github.event.schedule) to set the IS_DAILY_REPORT flag.
Conditional Run: The step for scrape_daily.py is guarded by:
code
Yaml
if: steps.determine_run_type.outputs.IS_DAILY_REPORT == 'true'
run: xvfb-run -a python scrape_daily.py || true
This ensures the resource-intensive Daily Report script only runs during the configured 8:30 AM UTC window or on manual dispatch.
xvfb-run -a python...: All scripts are run using xvfb-run -a, which provides a virtual display server necessary for Playwright's browser to run successfully in the headless Linux environment.
