# Juniper Square Automated Data Refresh System
### Implementation Plan — v1.0 | Confidential

---

## Executive Summary

This document outlines the end-to-end implementation plan for automating the download of CSV exports from Juniper Square and delivering them to the department's report generation UI. Because Juniper Square does not offer scheduled exports for the required CSV types, the solution uses browser automation (Playwright) combined with session persistence to handle SMS-based MFA without requiring intervention on every run.

> **Goal:** Enable any authorized team member to refresh Juniper Square data on-demand with a single double-click — with no manual login required on routine runs — while maintaining strong security controls over credentials, session tokens, and file access.

### Solution at a Glance

| Component | Choice |
|---|---|
| Automation tool | Python + Playwright (headless Chromium) |
| MFA strategy | Session cookie persistence (SMS OTP bypassed after first login) |
| Credential storage | Windows Credential Manager via `keyring` library |
| Session storage | User-local `LOCALAPPDATA` (never on shared drive) |
| Trigger method | Double-click `.bat` file on shared drive |
| Output destination | Shared drive data folder read by the UI |
| Access control | Script folder restricted; data folder team-readable |

---

## Phase 1 — Environment Setup & Security Baseline

*Install dependencies and establish all security controls before writing a single line of automation code.*

### 1.1 Prerequisites

- Python 3.10 or higher installed on the machine that will run the script
- `pip` and a virtual environment (`venv`) to isolate dependencies
- Playwright and its Chromium browser downloaded
- `keyring`, `python-dotenv` packages installed

```bash
pip install playwright keyring python-dotenv
playwright install chromium
```

### 1.2 Folder Structure

Create the following structure on the shared drive and apply permissions as noted:

| Path | Who Can Access | Permission Level |
|---|---|---|
| `shared_drive/reports_ui/` | All team members | Read + Execute |
| `shared_drive/reports_ui/data/` | All team members | Read only |
| `shared_drive/admin/juniper_refresh/` | Script runner only | Read + Write + Execute |
| `shared_drive/admin/juniper_refresh/.env` | Script runner only | Read only (600) |
| `%LOCALAPPDATA%/JuniperRefresh/` | Script runner only | Read + Write (OS-enforced) |

### 1.3 Credential Setup (Windows Credential Manager)

> **Why keyring?** The `keyring` library stores credentials in the Windows encrypted vault (DPAPI). Unlike a `.env` file, the vault is tied to the Windows user account and cannot be read by other users or processes. No plain text password ever touches the shared drive.

Run the following one-time setup script on the machine that will execute the automation:

```python
import keyring

keyring.set_password("juniper_square", "email", "your@email.com")
keyring.set_password("juniper_square", "password", "yourpassword")
print("Credentials stored securely.")
```

> ⚠️ **Important:** Run this setup script once, then delete it immediately. Credentials are bound to the Windows user — another user on the same machine cannot read them.

---

## Phase 2 — Script Development

*Build the Playwright automation with session persistence, selector identification, and multi-file download support.*

### 2.1 Identify Selectors (Do This First)

Before writing the download logic, run Playwright in non-headless mode and manually inspect Juniper Square's export UI to identify the correct CSS selectors. This must be done for every export page.

- Launch the browser with `headless=False`
- Navigate to each export page
- Right-click the export button → Inspect Element → copy the selector
- Record the post-login URL pattern for session validation
- Note whether downloads trigger immediately or require a modal/confirmation step

### 2.2 Core Script Architecture

The script is organized into four clearly separated concerns:

| Module | Responsibility |
|---|---|
| Credential loader | Retrieves email/password from Windows Credential Manager at runtime |
| Session manager | Loads, validates, and saves session cookies to `LOCALAPPDATA` |
| Login handler | Opens visible browser, pre-fills credentials, waits for user to complete SMS OTP |
| Download engine | Iterates the `EXPORTS` list, navigates each URL, triggers and saves each file |

### 2.3 Session File Security

> ⚠️ **Risk:** The session JSON file is equivalent to a live authenticated token. If an attacker obtains it, they can access Juniper Square as you until expiry. It must never be stored on the shared drive.

- Store session file at: `%LOCALAPPDATA%\JuniperRefresh\session.json`
- This path is OS-enforced to the current Windows user — other users on the machine cannot read it
- Add `session.json` to `.gitignore` if the project is ever version-controlled
- The script auto-deletes and recreates the session file on re-authentication

### 2.4 EXPORTS Configuration

Define all required CSV exports in a single configuration list at the top of the script. This is the only section that needs updating when Juniper Square adds new reports or changes URLs.

```python
EXPORTS = [
    { "name": "investors",     "url": "https://...", "button": "button#export-csv" },
    { "name": "transactions",  "url": "https://...", "button": "button#export-csv" },
    { "name": "distributions", "url": "https://...", "button": "button#export-csv" },
]
```

### 2.5 Full Script

```python
import asyncio, json, os, hashlib, logging
import keyring
from playwright.async_api import async_playwright

# ── Configuration ────────────────────────────────────────────────────────────

DOWNLOAD_DIR  = r"\\shared_drive\reports_ui\data"
SESSION_FILE  = os.path.join(os.environ["LOCALAPPDATA"], "JuniperRefresh", "session.json")
LOG_FILE      = os.path.join(os.environ["LOCALAPPDATA"], "JuniperRefresh", "refresh.log")
EXPECTED_HASH = "paste_sha256_of_this_script_here"   # Update after every legitimate change

EXPORTS = [
    { "name": "investors",     "url": "https://app.juniperssquare.com/path/investors",    "button": "button#export-csv" },
    { "name": "transactions",  "url": "https://app.juniperssquare.com/path/transactions", "button": "button#export-csv" },
]

# ── Integrity check ──────────────────────────────────────────────────────────

def verify_integrity():
    with open(__file__, "rb") as f:
        actual = hashlib.sha256(f.read()).hexdigest()
    if actual != EXPECTED_HASH:
        raise RuntimeError("Script integrity check failed. File may have been tampered with.")

# ── Logging (safe — never logs credentials or tokens) ────────────────────────

os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
logging.basicConfig(filename=LOG_FILE, level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")

# ── Session helpers ──────────────────────────────────────────────────────────

async def save_session(context):
    os.makedirs(os.path.dirname(SESSION_FILE), exist_ok=True)
    cookies = await context.cookies()
    with open(SESSION_FILE, "w") as f:
        json.dump(cookies, f)
    logging.info("Session saved.")

async def load_session(context):
    if not os.path.exists(SESSION_FILE):
        return False
    with open(SESSION_FILE) as f:
        await context.add_cookies(json.load(f))
    return True

async def is_session_valid(page):
    await page.goto("https://app.juniperssquare.com/dashboard", wait_until="networkidle")
    return "login" not in page.url

# ── Login ────────────────────────────────────────────────────────────────────

async def manual_login(page, context):
    email    = keyring.get_password("juniper_square", "email")
    password = keyring.get_password("juniper_square", "password")
    if not email or not password:
        raise RuntimeError("Credentials not found in Windows Credential Manager. Run setup first.")

    print("\n  Browser opened. Please complete the SMS MFA when prompted.")
    print("  The script will continue automatically once you are logged in.\n")
    logging.info("Manual login initiated.")

    await page.goto("https://app.juniperssquare.com/login")
    await page.fill("#email", email)
    await page.fill("#password", password)
    await page.click("button[type='submit']")

    try:
        await page.wait_for_url("**/dashboard**", timeout=180000)  # 3 min for SMS OTP
        logging.info("Login successful.")
        await save_session(context)
    except Exception:
        logging.error("Login timed out.")
        raise

# ── Download ─────────────────────────────────────────────────────────────────

async def download_export(page, export):
    print(f"  Downloading: {export['name']}...")
    logging.info(f"Starting download: {export['name']}")
    await page.goto(export["url"], wait_until="networkidle")
    try:
        async with page.expect_download(timeout=30000) as dl:
            await page.click(export["button"])
        download = await dl.value
        save_path = os.path.join(DOWNLOAD_DIR, f"{export['name']}.csv")
        await download.save_as(save_path)
        print(f"  ✔  Saved: {export['name']}.csv")
        logging.info(f"Download complete: {export['name']}.csv")
    except Exception as e:
        print(f"  ✘  Failed: {export['name']} — {e}")
        logging.error(f"Download failed: {export['name']}")

# ── Main ─────────────────────────────────────────────────────────────────────

async def main():
    verify_integrity()
    print("\n=== Juniper Square Data Refresh ===\n")
    logging.info("Refresh started.")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(accept_downloads=True)
        page    = await context.new_page()

        session_loaded = await load_session(context)

        if session_loaded:
            print("  Checking saved session...")
            if await is_session_valid(page):
                print("  ✔  Session valid. Skipping login.\n")
                logging.info("Existing session valid.")
            else:
                print("  ✘  Session expired. Re-authenticating...\n")
                logging.info("Session expired. Re-authenticating.")
                await manual_login(page, context)
        else:
            print("  No session found. Logging in for the first time...\n")
            logging.info("No session found. First-time login.")
            await manual_login(page, context)

        for export in EXPORTS:
            await download_export(page, export)

        await browser.close()

    print("\n=== Done! All files updated. ===\n")
    logging.info("Refresh complete.")

asyncio.run(main())
```

---

## Phase 3 — Security Hardening

*Apply all security controls to the script, files, and folder structure before any live testing.*

### 3.1 Logging — Safe Practices

The log file is written to `LOCALAPPDATA` (not the shared drive).

| Safe to Log | Never Log |
|---|---|
| Timestamp of each run | Email address or password |
| Which export was downloaded | Cookie values or session token contents |
| Success / failure per file | Full URL if it contains auth tokens |
| Session valid / expired | Any data from the downloaded CSV content |
| Script version / hash | Error stack traces containing auth headers |

### 3.2 Script Integrity Check

To guard against tampering on the shared drive, the script verifies its own SHA-256 hash at startup. If the hash does not match, execution halts immediately.

```bash
# Generate the hash after finalizing the script
python -c "import hashlib; print(hashlib.sha256(open('refresh_data.py','rb').read()).hexdigest())"
```

Paste the output as `EXPECTED_HASH` in the script. Update it intentionally whenever the script is legitimately changed.

### 3.3 .env File Hardening

If a `.env` file is used for non-credential config (e.g., `DOWNLOAD_DIR`, `LOG_PATH`):

- Store in the admin-only folder, never on the open shared drive
- Set Windows file permissions so only the script runner's account can read it
- Never store passwords in `.env` — use `keyring` exclusively for credentials
- Add `.env` to `.gitignore`

### 3.4 Service Account Recommendation

> **Best Practice:** Create a dedicated Juniper Square account with read-only/export-only permissions specifically for this automation. If the account is ever compromised, an attacker can only export data — they cannot modify investments, update records, or access billing. Your primary account remains safe.

### 3.5 Incident Response Checklist

If credentials or the session file are ever suspected to be compromised:

- [ ] Immediately change the Juniper Square password for the automation account
- [ ] Delete `%LOCALAPPDATA%\JuniperRefresh\session.json` to invalidate the session token
- [ ] Re-run `keyring` credential setup with the new password
- [ ] Review Juniper Square activity/audit logs for unauthorized access *(check for unexpected logins, exports, or data access)*
- [ ] Rotate credentials on any other systems that share the same password
- [ ] Notify your IT/security team if PII or financial data may have been exposed

---

## Phase 4 — Testing

*Validate the full flow end-to-end before handing off to the team.*

### 4.1 Test Cases

| Test | Expected Result |
|---|---|
| First-ever run (no session file) | Browser opens, credentials pre-filled, user completes SMS OTP, session saved, all CSVs downloaded |
| Subsequent run (valid session) | No browser interaction required, all CSVs downloaded silently |
| Run with expired session | Browser opens, prompts for SMS OTP, refreshes session, continues download |
| Wrong/missing credentials in keyring | Script exits with clear error message, no crash, no traceback exposed to log |
| Export button selector has changed | Graceful failure for that export, other exports continue, error logged |
| Download directory does not exist | Script creates directory or exits with clear error — no silent failure |
| Script file has been tampered with | Hash check fails at startup, script refuses to execute |
| `.bat` double-click by team member | Console window shows progress, closes cleanly on success |

---

## Phase 5 — Deployment & Handoff

*Deploy to the shared drive and brief the team on how to use the refresh tool.*

### 5.1 Deployment Checklist

- [ ] Finalize script and record `EXPECTED_HASH` *(update the hash whenever the script is legitimately changed)*
- [ ] Store credentials in Windows Credential Manager on the runner's machine *(run `setup_credentials.py` once, then delete the setup script)*
- [ ] Copy `refresh_data.py` and `refresh_data.bat` to admin-only shared drive folder
- [ ] Set folder permissions: admin-only write on script folder, read-only for team on data folder
- [ ] Place a shortcut to `refresh_data.bat` in the reports UI folder for team access *(shortcut only — not the script itself)*
- [ ] Run a full end-to-end test with a fresh session *(delete `session.json` first)*
- [ ] Verify downloaded CSVs appear correctly in the UI
- [ ] Document the session expiry behavior in the team runbook

### 5.2 `.bat` Trigger File

```bat
@echo off
echo ===================================
echo   Juniper Square Data Refresh
echo ===================================
echo.
cd /d %~dp0
python refresh_data.py
echo.
pause
```

### 5.3 Team Runbook (Summary)

Provide the team with a simple guide covering:

- **How to trigger a refresh:** Double-click the shortcut in the reports UI folder
- **What to expect:** A console window showing progress; closes automatically when done
- **If the browser opens:** Enter the SMS OTP when prompted — this is normal when the session expires
- **What NOT to do:** Do not move, copy, or forward the `.bat` file or any files from the admin folder
- **Who to contact:** If the script fails or behaves unexpectedly, contact [admin name]

---

## Phase 6 — Ongoing Maintenance

*Keep the automation healthy over time.*

### 6.1 Maintenance Schedule

| Frequency | Task |
|---|---|
| After each Juniper Square UI update | Re-verify CSS selectors for export buttons — most likely thing to break |
| When session expires unexpectedly early | Check if Juniper Square changed its session TTL policy |
| Quarterly | Rotate the automation account password and update `keyring` credentials |
| When adding a new CSV export | Add entry to `EXPORTS` list, test, and update `EXPECTED_HASH` |
| If the script runner changes | Remove old keyring entry; re-run credential setup on the new runner's machine |

### 6.2 Version Control Recommendation

Even for a small internal script, a private Git repository provides:

- Full history of every change to the script
- Ability to roll back if an update breaks the automation
- Clear audit trail of who changed what and when

> ⚠️ **Reminder:** Never commit credentials, `.env` files, or `session.json` to version control — even in a private repository. Ensure `.gitignore` is configured before the first commit.

```gitignore
# .gitignore
.env
session.json
*.json
__pycache__/
*.pyc
```

---

## Security Controls Summary

| Risk | Control | Phase |
|---|---|---|
| Plain text password exposure | Windows Credential Manager (`keyring`) | Phase 1 |
| Session token on shared drive | Store in `LOCALAPPDATA` (OS-enforced, user-only) | Phase 2 |
| Credentials in source code | Runtime retrieval from `keyring` only | Phase 1 |
| Overly broad account permissions | Dedicated read-only service account | Phase 3 |
| Script tampering on shared drive | SHA-256 integrity check at startup | Phase 3 |
| Team over-access to scripts | Admin-only folder permissions | Phase 1 & 5 |
| Sensitive data in log files | Log actions only, never tokens or credentials | Phase 3 |
| No incident response plan | Documented checklist in Phase 3.5 | Phase 3 |
| Stale selectors breaking silently | Per-export error handling + maintenance schedule | Phase 4 & 6 |
| `.env` or `session.json` in Git | `.gitignore` configured before first commit | Phase 2 |

---

*Internal Use Only — Juniper Square Automation Implementation Plan v1.0*
