Looker Studio Scraper Suite: Technical Manual
This repository contains a robust, multi-script Python and GitHub Actions solution for extracting business-critical data from Google Looker Studio reports and delivering formatted, actionable alerts to Google Chat.
🚀 Features at a Glance
Script	Purpose	Execution	Key Technology
scrape_daily.py	Retail Daily Summary (Comprehensive dashboard card)	Once Daily (8:30 AM UTC)	Gemini Pro Vision (for visual metrics), Conditional Card Filtering, Custom Color/Target Formatting.
scrape.py	NPS Comment Scraper (Batched customer feedback)	Multiple Times Daily	2FA Handling, Rate Limiting, Deduplication (using comment/timestamp/store).
scrape_complaints.py	Customer Complaints Scraper (New case alerts)	Multiple Times Daily	State-Machine Parsing, Deduplication (using case number), Per-Complaint Card Alerting.
⚙️ 1. Configuration: GitHub Secrets
The system is configured entirely via GitHub Secrets. These secrets are injected into the runtime environment and stored in a temporary config.ini for the scripts.
Secret Name	Purpose & Script Usage
GOOGLE_EMAIL	Google account email used to access the Looker Studio reports.
GOOGLE_PASSWORD	The password for the Google account (used only for re-login).
GEMINI_API_KEY	Critical for scrape_daily.py. Provides access to the Gemini API for visual metric extraction.
MAIN_WEBHOOK	Target URL for general alerts and batched NPS comments (scrape.py).
ALERT_WEBHOOK	Target URL for high-priority alerts: login failures, 2FA codes, and critical script errors.
DAILY_WEBHOOK	Target URL for the comprehensive daily summary card (scrape_daily.py).
COMPLAINTS_WEBHOOK	Target URL for new complaint notifications (scrape_complaints.py).
AUTH_STATE_B64	(Optional) Base64-encoded content of a successful auth_state.json file. Used for immediate setup or cache failure recovery.
🔑 2. Manual Operational Guide: Initial Login & 2FA
The first successful run is a manual, non-headless process. It is required to create and cache the shared authenticated session file (auth_state.json).
Initial Authentication Procedure
Set Secrets: Verify all required credentials (GOOGLE_EMAIL, GOOGLE_PASSWORD, and all *WEBHOOKs) are configured in GitHub Secrets.
Trigger Login: Navigate to the Actions tab, select the primary workflow, and click Run workflow (using workflow_dispatch).
Wait for Alert: The scrape.py script will run and detect the necessary Google sign-in flow.
2FA Intervention (Critical Step):
The script pauses and uses screen-scraping logic to identify the number displayed on the Google "Match the number" screen.
It immediately sends an alert to your ALERT_WEBHOOK containing the verification code (e.g., 🔐 Tap this number on your phone: **42**).
Action Required: You must quickly approve the sign-in request on your mobile device and tap the matching number shown in the alert.
Completion: Once approved, the Playwright session saves the state to auth_state.json, which the workflow then caches and uses for all subsequent automated runs.
3. Detailed Script Logic and Mechanisms
A. scrape_daily.py (Daily Report)
The primary goal is robustness and clarity of the final Chat Card.
Feature	Logic Explanation
Gemini Vision Extraction	Metrics listed in GEMINI_METRICS (e.g., NPS scores, payroll/shrink dials) that fail initial text scraping are sent to Gemini Pro Vision for re-extraction from a full-page screenshot. This bypasses common visual parsing failures.
Data Stabilization	The script uses a combined waiting time of 20 seconds after initial navigation, plus a 5-second buffer just before screenshot and text extraction, ensuring all dynamic dashboard components are fully loaded.
Conditional Widget Filtering (_create_metric_widget)	Logic: A metric widget is only created if its value is not None, empty (""), the placeholder ("—"), or the literal hyphen ("-"). NPS metrics returning "NPS" (meaning data is missing) are also filtered out.
Dynamic Section Filtering	The build_chat_card function iterates over the pre-defined sections and only includes a section in the final card if it contains at least one non-filtered metric widget.
Custom Color Formatting	Performance status rules (G, R, O, BR) are translated into <font color='...'> HTML tags (Red: #FF0000, Amber: #FFA500) for visual flagging on the Chat Card text.
B. scrape_complaints.py (Complaints)
Feature	Logic Explanation
State-Machine Parsing	The parse_complaints_from_lines function uses sequential logic based on expected line patterns (FOUND_DATE, FOUND_CASE, READING_DESC, READING_RESPONSE) to correctly group unstructured raw text lines into cohesive complaint records.
Case Deduplication	New complaints are identified by comparing the extracted case_number against all entries in the persistent complaints_log.csv. Only unique, new cases are processed and sent.
Alert Formatting	Each new complaint triggers an individual, richly formatted Google Chat Card, including truncated description and store response text for immediate review.
C. scrape.py (NPS Comments)
Feature	Logic Explanation
Robust Parsing	The parse_comments_from_lines function uses a multi-step process that looks for DATE, STORE, and SCORE markers relative to the "Submission via:" line to ensure comments are correctly associated with their metadata, while filtering out common dashboard noise lines.
Rate Limiting / Batching	New comments are capped at MAX_COMMENTS_PER_RUN and sent in batches of BATCH_SIZE (10 widgets per card) using the send_comments_batched_to_chat function to prevent API overload.
Lock Management	The _acquire_lock function uses a physical scrape.lock file to prevent concurrent runs. It includes logic to clean up stale locks older than 20 minutes, ensuring a failed run does not permanently block the scheduler.
4. GitHub Actions Workflow Logic
The workflow is set up for high availability and efficient resource usage.
Workflow Step	Logic Explanation
Scheduling	Uses cron expressions to trigger the workflow multiple times a day. The 8:30 AM UTC run is specially flagged to execute the resource-intensive scrape_daily.py.
Caching	Uses actions/cache to persist auth_state.json and the CSV logs (comments_log.csv, complaints_log.csv, etc.). Cache key structure ensures the most recent authentication state is always restored.
Authentication Fallback	If the cache miss is complete, the workflow checks the AUTH_STATE_B64 secret to decode a base64 string directly to auth_state.json for a quick recovery.
Conditional Runs	The script scrape_daily.py is executed only if the run originated from the daily schedule (if: steps.determine_run_type.outputs.IS_DAILY_REPORT == 'true'). All other runs are partial (NPS and Complaints only).
Virtual Display	All Python executions use xvfb-run -a python.... This provides a necessary virtual X server environment for Playwright's Chromium browser, allowing it to run in a seemingly headed mode within a headless CI environment.
Artifacts	Logs, screenshots (screens/), and state files are uploaded on completion (even on failure) for detailed post-mortem debugging.
