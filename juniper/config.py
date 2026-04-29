import os
import platform

# ── Juniper Square URLs ─────────────────────────────────────────────────────

BASE_URL = "https://denholtz.junipersquare.com"
LOGIN_URL = f"{BASE_URL}/login"
# Used to detect whether a session is still valid (redirects to login if expired)
SESSION_CHECK_URL = f"{BASE_URL}/contacts"

# ── Keyring ─────────────────────────────────────────────────────────────────

KEYRING_SERVICE = "juniper_square"
KEYRING_EMAIL_KEY = "email"
KEYRING_PASSWORD_KEY = "password"

# ── Login selectors ─────────────────────────────────────────────────────────

LOGIN_SELECTORS = {
    "email": "#email",
    "password": "#password",
    "submit": "button[type='submit']",
}

# ── Export definitions ──────────────────────────────────────────────────────
# Each export maps to one file in the data/ directory.
# "steps" is a list of click actions; the LAST step triggers the download.
# Between steps the downloader waits for the next selector to appear.

EXPORTS = [
    {
        "name": "Contacts",
        "url": f"{BASE_URL}/contacts",
        "steps": [
            # Ensure we're on the Contacts view before opening Export panel.
            {
                "action": "click",
                "selector": "a[data-cy='contacts-link']:visible",
                "timeout": 30_000,
            },
            # Step 1: click the "Export" side button (ag-grid panel toggle)
            {
                "action": "click",
                "selector": "button.ag-side-button-button[role='tab']:has(span.ag-side-button-label:has-text('Export')):visible",
                # Avoid closing the panel if Export is already open.
                "skip_click_if_visible": "button[data-testid='form-submit-button']:has-text('Run export'):visible",
                "timeout": 30_000,
                "post_click_wait_for": "button[data-testid='form-submit-button']:has-text('Run export'):visible",
                "post_click_timeout": 45_000,
                "retries": 2,
            },
            # Step 2: click "Run export" submit button
            {
                "action": "click",
                "selector": "button[data-testid='form-submit-button']:has-text('Run export'):visible",
                "timeout": 60_000,
            },
        ],
        "save_as": "js_contacts.csv",
    },
    {
        "name": "Accounts",
        "url": f"{BASE_URL}/accounts",
        "steps": [
            # Step 1: open the Downloads dropdown
            {"action": "click", "selector": "button.btn.btn-link.dropdown-toggle:has-text('Downloads'):visible"},
            # Step 2: click Accounts option from the Downloads menu
            {"action": "click", "selector": "a#menu-item-export_accounts.menu-item:has-text('Accounts'):visible"},
            # Step 3: click Download in the Accounts CSV modal
            {"action": "click", "selector": "button.btn.btn-primary[data-cy='modal-button-save'][data-save-url='/accounts/download_csv']:has-text('Download'):visible"},
        ],
        "save_as": "Accounts.csv",
    },
    # Entity Overview and Contact Export — to be added in Phase 2
]

# ── Session storage (platform-aware) ───────────────────────────────────────

def _get_session_dir():
    system = platform.system()
    if system == "Darwin":
        base = os.path.expanduser("~/Library/Application Support")
    elif system == "Windows":
        base = os.environ.get("LOCALAPPDATA", os.path.expanduser("~"))
    else:  # Linux and others
        base = os.path.join(os.path.expanduser("~"), ".local", "share")
    return os.path.join(base, "JuniperRefresh")

SESSION_DIR = _get_session_dir()
SESSION_FILE = os.path.join(SESSION_DIR, "session.json")

# ── Timeouts (milliseconds) ────────────────────────────────────────────────

MFA_TIMEOUT = 300_000       # 5 minutes for user to complete SMS OTP
DOWNLOAD_TIMEOUT = 60_000   # 1 minute per file download
NAV_TIMEOUT = 15_000        # 15 seconds for page navigation and selector waits
AUTH_NAV_TIMEOUT = 60_000   # 60 seconds for login/session-check navigation
