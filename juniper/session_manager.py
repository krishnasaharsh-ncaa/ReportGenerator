import json
import os
import logging

from playwright.async_api import TimeoutError as PlaywrightTimeoutError

from .config import SESSION_DIR, SESSION_FILE, SESSION_CHECK_URL, AUTH_NAV_TIMEOUT

logger = logging.getLogger(__name__)


async def save_session(context):
    """Persist browser cookies to disk for session reuse."""
    os.makedirs(SESSION_DIR, exist_ok=True)
    cookies = await context.cookies()
    with open(SESSION_FILE, "w") as f:
        json.dump(cookies, f)
    logger.info("Session saved to %s", SESSION_FILE)


async def load_session(context):
    """Load saved cookies into the browser context. Returns True if file existed."""
    if not os.path.exists(SESSION_FILE):
        return False
    try:
        with open(SESSION_FILE) as f:
            cookies = json.load(f)
        await context.add_cookies(cookies)
        logger.info("Session loaded from %s", SESSION_FILE)
        return True
    except (json.JSONDecodeError, Exception) as e:
        logger.warning("Failed to load session file: %s", e)
        clear_session()
        return False


async def is_session_valid(page):
    """Navigate to a known authenticated page and check if we're redirected to login."""
    try:
        await page.goto(SESSION_CHECK_URL, wait_until="domcontentloaded", timeout=AUTH_NAV_TIMEOUT)
        try:
            # Best-effort stabilization only; some SPA pages keep background traffic alive.
            await page.wait_for_load_state("networkidle", timeout=8_000)
        except PlaywrightTimeoutError:
            pass
        current_url = page.url
        # If the URL contains "signin" or "login", the session is expired
        is_valid = "signin" not in current_url and "login" not in current_url
        logger.info("Session valid: %s (url: %s)", is_valid, current_url)
        return is_valid
    except Exception as e:
        logger.warning("Session validation failed: %s", e)
        return False


def clear_session():
    """Delete the session file."""
    if os.path.exists(SESSION_FILE):
        os.remove(SESSION_FILE)
        logger.info("Session file deleted.")
