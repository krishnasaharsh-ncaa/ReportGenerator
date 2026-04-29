import asyncio
import mimetypes
import os
import logging
import tempfile

import keyring
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

from .config import (
    EXPORTS, LOGIN_URL, LOGIN_SELECTORS, KEYRING_SERVICE,
    KEYRING_EMAIL_KEY, KEYRING_PASSWORD_KEY,
    MFA_TIMEOUT, DOWNLOAD_TIMEOUT, NAV_TIMEOUT, AUTH_NAV_TIMEOUT,
)
from .session_manager import save_session, load_session, is_session_valid, clear_session
from storage import cache_source_file

logger = logging.getLogger(__name__)


def _get_credentials():
    """Retrieve credentials from the system keychain."""
    email = keyring.get_password(KEYRING_SERVICE, KEYRING_EMAIL_KEY)
    password = keyring.get_password(KEYRING_SERVICE, KEYRING_PASSWORD_KEY)
    if not email or not password:
        raise RuntimeError(
            "Juniper Square credentials not found in system keychain. "
            "Please set them up via the Settings button in the UI."
        )
    return email, password


async def _login(page, context, progress_callback=None):
    """Pre-fill credentials and wait for the user to complete SMS MFA."""
    email, password = _get_credentials()

    if progress_callback:
        progress_callback("login", "mfa_required")

    logger.info("Navigating to login page.")
    await page.goto(LOGIN_URL, wait_until="domcontentloaded", timeout=AUTH_NAV_TIMEOUT)
    try:
        # Best-effort stabilization only; don't fail auth if background requests remain active.
        await page.wait_for_load_state("networkidle", timeout=8_000)
    except PlaywrightTimeoutError:
        pass

    await page.fill(LOGIN_SELECTORS["email"], email)
    await page.fill(LOGIN_SELECTORS["password"], password)
    await page.click(LOGIN_SELECTORS["submit"])

    logger.info("Credentials submitted. Waiting for MFA completion (up to 5 min)...")

    # Wait for navigation away from the login/signin page
    try:
        await page.wait_for_function(
            """() => !window.location.href.includes('signin') && !window.location.href.includes('login')""",
            timeout=MFA_TIMEOUT,
        )
        logger.info("Login successful. Current URL: %s", page.url)
        await save_session(context)
        if progress_callback:
            progress_callback("login", "complete")
    except Exception as e:
        logger.error("Login timed out or failed: %s", e)
        raise RuntimeError("Login failed — MFA was not completed within the timeout window.") from e


async def _dismiss_overlay(page):
    """Dismiss any overlay/popup that intercepts clicks (Qualtrics survey, feedback modals, etc.)."""
    try:
        await page.evaluate("""
            () => {
                // Remove Qualtrics survey iframes
                document.querySelectorAll('iframe[title*="Qualtrics"]').forEach(el => el.remove());
                // Remove any overlay containers that might block clicks
                document.querySelectorAll('[class*="qualtrics"], [id*="qualtrics"]').forEach(el => el.remove());
                // Remove any fixed/absolute positioned overlays covering the page
                // The Juniper Square feedback widget renders inside jsq-react-root
                // and can overlay the entire page with a survey popup
                for (const el of document.querySelectorAll('div')) {
                    const style = window.getComputedStyle(el);
                    if ((style.position === 'fixed' || style.position === 'absolute') &&
                        style.zIndex > 100 &&
                        el.querySelector('iframe[title*="Qualtrics"], iframe[title*="Survey"], iframe[title*="Feedback"]')) {
                        el.remove();
                    }
                }
            }
        """)
        logger.info("Dismissed overlay elements.")
    except Exception:
        pass
    await asyncio.sleep(0.5)


async def _download_export(page, export, download_dir):
    """Navigate to an export page, execute steps, and save the downloaded file to a temp staging directory."""
    name = export["name"]
    save_as = export["save_as"]
    steps = export["steps"]

    logger.info("Starting download: %s", name)
    await page.goto(export["url"], wait_until="networkidle", timeout=NAV_TIMEOUT)

    # Dismiss any Qualtrics feedback survey overlay that intercepts clicks
    await _dismiss_overlay(page)

    # Execute all steps. The LAST step triggers the download.
    for i, step in enumerate(steps):
        is_last = (i == len(steps) - 1)
        selector = step["selector"]
        skip_if_visible = step.get("skip_click_if_visible")
        step_timeout = step.get("timeout", NAV_TIMEOUT)
        retries = max(0, int(step.get("retries", 0)))
        post_click_wait_for = step.get("post_click_wait_for")
        post_click_timeout = step.get("post_click_timeout", step_timeout)

        if skip_if_visible:
            try:
                if await page.locator(skip_if_visible).first.is_visible():
                    logger.info("Skipping step %s for %s because '%s' is already visible.", i + 1, name, skip_if_visible)
                    continue
            except Exception:
                # If visibility probing fails, proceed with normal step execution.
                pass

        # Wait for the element to be visible before clicking
        await page.wait_for_selector(selector, state="visible", timeout=step_timeout)

        if is_last:
            # Wrap the final click in expect_download to capture the file
            async with page.expect_download(timeout=DOWNLOAD_TIMEOUT) as dl_info:
                await page.click(selector, force=True)
            download = await dl_info.value

            # Save to a temp file first, then atomically move to final path
            final_path = os.path.join(download_dir, save_as)
            tmp_fd, tmp_path = tempfile.mkstemp(
                dir=download_dir, prefix=f".{save_as}.", suffix=".tmp"
            )
            os.close(tmp_fd)
            try:
                await download.save_as(tmp_path)
                os.replace(tmp_path, final_path)
                logger.info("Saved: %s", final_path)
            except Exception:
                # Clean up temp file on failure
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
                raise
        else:
            success = False
            last_error = None
            for attempt in range(retries + 1):
                try:
                    try:
                        await page.click(selector, timeout=step_timeout)
                    except Exception:
                        # Fallback to force click for flaky overlays/interception.
                        await page.click(selector, force=True, timeout=step_timeout)

                    if post_click_wait_for:
                        await page.wait_for_selector(post_click_wait_for, state="visible", timeout=post_click_timeout)
                    # Brief pause for modals/dropdowns to appear
                    await asyncio.sleep(1)
                    success = True
                    break
                except Exception as e:
                    last_error = e
                    if attempt < retries:
                        logger.info(
                            "Step %s for %s did not settle (attempt %s/%s). Retrying.",
                            i + 1, name, attempt + 1, retries + 1
                        )
                        await asyncio.sleep(0.75)
            if not success and last_error:
                raise last_error
    return os.path.join(download_dir, save_as)


async def run_refresh(artifact_store, local_cache_dir, progress_callback=None, export_name=None):
    """
    Main entry point for the Juniper Square data refresh.

    Args:
        artifact_store: Storage backend used as the source of truth for source files.
        local_cache_dir: Local cache directory used by the current process and report scripts.
        progress_callback: Optional callable(file_name, status) for progress updates.
            status is one of: "mfa_required", "complete", "downloading", "uploading", "done", "error"
        export_name: Optional name of a single export (e.g., "Contacts"). If None, all exports run.

    Returns:
        dict with keys: status, files_downloaded, errors
    """
    # Verify credentials exist before launching browser
    _get_credentials()

    files_downloaded = []
    errors = []

    async with async_playwright() as p:
        # First try headless with saved session
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(accept_downloads=True)
        page = await context.new_page()

        session_loaded = await load_session(context)
        needs_login = True

        if session_loaded:
            if progress_callback:
                progress_callback("session", "checking")
            if await is_session_valid(page):
                needs_login = False
                logger.info("Existing session is valid. Proceeding headless.")
                if progress_callback:
                    progress_callback("session", "valid")
            else:
                logger.info("Session expired.")
                clear_session()

        if needs_login:
            # Close headless browser and relaunch in headed mode for MFA
            await browser.close()
            browser = await p.chromium.launch(headless=False)
            context = await browser.new_context(accept_downloads=True)
            page = await context.new_page()
            await _login(page, context, progress_callback)

        # Filter exports if a specific one was requested
        exports_to_run = EXPORTS
        if export_name:
            exports_to_run = [e for e in EXPORTS if e["name"] == export_name]
            if not exports_to_run:
                await browser.close()
                return {"status": "error", "files_downloaded": [], "errors": [f"Unknown export: {export_name}"]}

        # Download each configured export
        with tempfile.TemporaryDirectory(prefix="juniper-refresh-") as download_dir:
            for export in exports_to_run:
                name = export["name"]
                save_as = export["save_as"]
                try:
                    if progress_callback:
                        progress_callback(name, "downloading")
                    staged_path = await _download_export(page, export, download_dir)

                    if not os.path.exists(staged_path):
                        raise RuntimeError(f"{save_as} was not downloaded.")

                    with open(staged_path, "rb") as f:
                        content = f.read()
                    if not content:
                        raise RuntimeError(f"{save_as} was downloaded but is empty.")

                    if progress_callback:
                        progress_callback(name, "uploading")

                    artifact_store.put_source_file(
                        save_as,
                        content,
                        content_type=mimetypes.guess_type(save_as)[0] or "application/octet-stream",
                        metadata={"origin": "juniper", "export_name": name},
                    )

                    if local_cache_dir:
                        cache_source_file(save_as, content, local_cache_dir)

                    files_downloaded.append(save_as)
                    if progress_callback:
                        progress_callback(name, "done")
                except Exception as e:
                    error_msg = f"{name}: {e}"
                    errors.append(error_msg)
                    logger.error("Download failed for %s: %s", name, e)
                    if progress_callback:
                        progress_callback(name, "error")

        await browser.close()

    status = "success" if not errors else ("partial" if files_downloaded else "error")
    return {
        "status": status,
        "files_downloaded": files_downloaded,
        "errors": errors,
    }
