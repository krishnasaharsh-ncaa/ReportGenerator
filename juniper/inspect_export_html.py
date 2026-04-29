#!/usr/bin/env python3
import argparse
import asyncio
from datetime import datetime
import json
from pathlib import Path
import re
import sys

from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

if __package__ in (None, ""):
    repo_root = Path(__file__).resolve().parent.parent
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))
    from juniper.config import EXPORTS, NAV_TIMEOUT, DOWNLOAD_TIMEOUT
    from juniper.downloader import _get_credentials, _login
    from juniper.session_manager import load_session, is_session_valid, clear_session
else:
    from .config import EXPORTS, NAV_TIMEOUT, DOWNLOAD_TIMEOUT
    from .downloader import _get_credentials, _login
    from .session_manager import load_session, is_session_valid, clear_session


def _slug(value):
    cleaned = re.sub(r"[^a-zA-Z0-9]+", "_", value.strip().lower()).strip("_")
    return cleaned or "export"


def _safe_filename(value):
    cleaned = re.sub(r"[^a-zA-Z0-9._-]+", "_", (value or "").strip())
    return cleaned or "download.bin"


def _filter_exports(requested_names):
    if not requested_names:
        return EXPORTS

    selected = []
    unknown = []
    by_name = {e["name"].lower(): e for e in EXPORTS}
    for name in requested_names:
        match = by_name.get(name.lower())
        if match:
            selected.append(match)
        else:
            unknown.append(name)

    if unknown:
        raise ValueError(f"Unknown export name(s): {', '.join(unknown)}")
    return selected


async def _probe_selector(page, selector):
    result = {"selector": selector, "count": 0, "samples": []}
    try:
        locator = page.locator(selector)
        count = await locator.count()
        result["count"] = count
        for i in range(min(count, 3)):
            try:
                outer = await locator.nth(i).evaluate("el => el.outerHTML")
                result["samples"].append((outer or "")[:1500])
            except Exception as sample_err:
                result["samples"].append(f"<failed to capture sample: {sample_err}>")
    except Exception as err:
        result["error"] = str(err)
    return result


async def _collect_candidates(page, limit):
    return await page.evaluate(
        """(maxItems) => {
            const nodes = Array.from(
              document.querySelectorAll(
                "button, a[role='button'], input[type='button'], input[type='submit']"
              )
            );

            function normalizeText(el) {
              return (el.innerText || el.textContent || el.value || "").replace(/\\s+/g, " ").trim();
            }

            function selectorHint(el) {
              const tag = el.tagName.toLowerCase();
              if (el.id) return `${tag}#${el.id}`;
              if (el.getAttribute("data-testid")) {
                return `${tag}[data-testid="${el.getAttribute("data-testid")}"]`;
              }
              if (el.classList && el.classList.length > 0) {
                return `${tag}.${Array.from(el.classList).slice(0, 3).join(".")}`;
              }
              return tag;
            }

            const out = [];
            for (const el of nodes) {
              if (out.length >= maxItems) break;
              out.push({
                tag: el.tagName.toLowerCase(),
                text: normalizeText(el),
                id: el.id || null,
                classes: (el.className || "").toString(),
                role: el.getAttribute("role"),
                type: el.getAttribute("type"),
                data_testid: el.getAttribute("data-testid"),
                aria_label: el.getAttribute("aria-label"),
                selector_hint: selectorHint(el),
                outer_html: (el.outerHTML || "").slice(0, 1000),
              });
            }
            return out;
        }""",
        limit,
    )


def _is_interesting_url(url):
    value = (url or "").lower()
    keywords = ("export", "download", "csv", "report", "file", "attachment")
    return any(k in value for k in keywords)


async def _collect_download_like_links(page, limit):
    return await page.evaluate(
        """(maxItems) => {
            const out = [];
            const anchors = Array.from(document.querySelectorAll("a[href]"));
            for (const a of anchors) {
              if (out.length >= maxItems) break;
              const href = a.getAttribute("href") || "";
              const text = (a.innerText || a.textContent || "").replace(/\\s+/g, " ").trim();
              const target = (href + " " + text).toLowerCase();
              if (
                target.includes("download") ||
                target.includes("export") ||
                target.includes(".csv")
              ) {
                out.push({
                  text,
                  href,
                  outer_html: (a.outerHTML || "").slice(0, 1000),
                });
              }
            }
            return out;
        }""",
        limit,
    )


async def _navigate_for_inspection(page, url, timeout_ms):
    """
    Navigate with a strategy suitable for SPAs that may never reach strict network idle.
    """
    await page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
    try:
        # Best-effort stabilization without failing the run.
        await page.wait_for_load_state("networkidle", timeout=min(8_000, timeout_ms))
    except PlaywrightTimeoutError:
        pass


async def _execute_steps_for_debug(
    page,
    steps,
    nav_timeout_ms,
    expect_download_timeout_ms,
    per_step_pause_seconds,
):
    step_results = []
    expected_download = {
        "attempted_on_last_step": False,
        "received": False,
        "error": None,
        "suggested_filename": None,
        "url": None,
    }

    for i, step in enumerate(steps):
        selector = step.get("selector")
        action = step.get("action")
        skip_if_visible = step.get("skip_click_if_visible")
        step_timeout = step.get("timeout", nav_timeout_ms)
        retries = max(0, int(step.get("retries", 0)))
        post_click_wait_for = step.get("post_click_wait_for")
        post_click_timeout = step.get("post_click_timeout", step_timeout)
        index = i + 1
        is_last = i == len(steps) - 1
        result = {
            "step_index": index,
            "action": action,
            "selector": selector,
            "ok": False,
            "count": 0,
            "url_after": None,
            "error": None,
        }

        try:
            await page.wait_for_selector(selector, state="visible", timeout=step_timeout)
            locator = page.locator(selector)
            result["count"] = await locator.count()

            if skip_if_visible:
                try:
                    if await page.locator(skip_if_visible).first.is_visible():
                        result["ok"] = True
                        result["url_after"] = page.url
                        result["skipped"] = True
                        result["skip_reason"] = f"'{skip_if_visible}' already visible"
                        step_results.append(result)
                        continue
                except Exception:
                    pass

            if is_last:
                expected_download["attempted_on_last_step"] = True
                try:
                    async with page.expect_download(timeout=expect_download_timeout_ms) as dl_info:
                        await page.click(selector, force=True)
                    download = await dl_info.value
                    expected_download["received"] = True
                    expected_download["suggested_filename"] = download.suggested_filename
                    expected_download["url"] = download.url
                except PlaywrightTimeoutError as e:
                    expected_download["error"] = f"No download event within timeout: {e}"
            else:
                success = False
                last_error = None
                for _attempt in range(retries + 1):
                    try:
                        try:
                            await page.click(selector, timeout=step_timeout)
                        except Exception:
                            await page.click(selector, force=True, timeout=step_timeout)
                        if post_click_wait_for:
                            await page.wait_for_selector(
                                post_click_wait_for,
                                state="visible",
                                timeout=post_click_timeout,
                            )
                        success = True
                        break
                    except Exception as e:
                        last_error = e
                        await asyncio.sleep(0.5)
                if not success and last_error:
                    raise last_error

            await asyncio.sleep(per_step_pause_seconds)
            result["ok"] = True
            result["url_after"] = page.url
        except Exception as e:
            result["error"] = str(e)
            result["url_after"] = page.url
            step_results.append(result)
            break

        step_results.append(result)

    return step_results, expected_download


async def _inspect_export(
    page,
    export,
    output_dir,
    settle_seconds,
    candidate_limit,
    screenshot,
    nav_timeout_ms,
    execute_steps,
    post_click_wait_seconds,
    expect_download_timeout_ms,
):
    name = export["name"]
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    slug = _slug(name)

    await _navigate_for_inspection(page, export["url"], nav_timeout_ms)
    await asyncio.sleep(settle_seconds)

    event_state = {
        "download_events": [],
        "saved_download_files": [],
        "download_save_errors": [],
        "request_failures": [],
        "console_errors": [],
        "page_errors": [],
        "interesting_responses": [],
        "response_capture_errors": [],
    }
    response_tasks = []
    download_tasks = []

    async def _save_download(download):
        suggested = _safe_filename(download.suggested_filename)
        target = output_dir / f"{slug}_{ts}_{suggested}"
        if target.exists():
            stem = target.stem
            suffix = target.suffix
            for i in range(1, 1000):
                candidate = target.with_name(f"{stem}_{i}{suffix}")
                if not candidate.exists():
                    target = candidate
                    break
        try:
            await download.save_as(str(target))
            event_state["saved_download_files"].append(str(target))
        except Exception as e:
            event_state["download_save_errors"].append(
                {
                    "suggested_filename": download.suggested_filename,
                    "url": download.url,
                    "error": str(e),
                }
            )

    def _on_download(download):
        event_state["download_events"].append({
            "suggested_filename": download.suggested_filename,
            "url": download.url,
        })
        download_tasks.append(asyncio.create_task(_save_download(download)))

    def _on_request_failed(request):
        event_state["request_failures"].append({
            "url": request.url,
            "method": request.method,
            "resource_type": request.resource_type,
            "failure": request.failure,
        })

    def _on_console(msg):
        if msg.type == "error":
            event_state["console_errors"].append(msg.text)

    def _on_page_error(exc):
        event_state["page_errors"].append(str(exc))

    async def _capture_response(resp):
        try:
            url = resp.url
            headers = resp.headers
            content_type = headers.get("content-type", "")
            content_disposition = headers.get("content-disposition", "")
            if _is_interesting_url(url) or "attachment" in content_disposition.lower():
                item = {
                    "status": resp.status,
                    "url": url,
                    "content_type": content_type,
                    "content_disposition": content_disposition,
                }
                if "json" in content_type.lower() or "text" in content_type.lower():
                    try:
                        body = await resp.text()
                        item["body_excerpt"] = body[:3000]
                    except Exception as e:
                        item["body_excerpt_error"] = str(e)
                event_state["interesting_responses"].append(item)
        except Exception as e:
            event_state["response_capture_errors"].append(str(e))

    def _on_response(resp):
        response_tasks.append(asyncio.create_task(_capture_response(resp)))

    page.on("download", _on_download)
    page.on("requestfailed", _on_request_failed)
    page.on("console", _on_console)
    page.on("pageerror", _on_page_error)
    page.on("response", _on_response)

    initial_html = await page.content()
    title = await page.title()

    html_file = output_dir / f"{slug}_{ts}.html"
    html_file.write_text(initial_html, encoding="utf-8")

    screenshot_file = None
    if screenshot:
        screenshot_file = output_dir / f"{slug}_{ts}.png"
        await page.screenshot(path=str(screenshot_file), full_page=True)

    selector_probes = []
    for step in export.get("steps", []):
        selector = step.get("selector")
        if selector:
            selector_probes.append(await _probe_selector(page, selector))

    candidates = await _collect_candidates(page, candidate_limit)
    candidates_file = output_dir / f"{slug}_{ts}_candidates.json"
    candidates_file.write_text(json.dumps(candidates, indent=2), encoding="utf-8")

    post_html_file = None
    post_screenshot_file = None
    post_candidates_file = None
    step_results = []
    expected_download = None
    download_like_links = []
    if execute_steps:
        step_results, expected_download = await _execute_steps_for_debug(
            page=page,
            steps=export.get("steps", []),
            nav_timeout_ms=nav_timeout_ms,
            expect_download_timeout_ms=expect_download_timeout_ms,
            per_step_pause_seconds=max(0.2, settle_seconds),
        )

        await asyncio.sleep(max(0.0, post_click_wait_seconds))
        post_html = await page.content()
        post_html_file = output_dir / f"{slug}_{ts}_post_steps.html"
        post_html_file.write_text(post_html, encoding="utf-8")

        if screenshot:
            post_screenshot_file = output_dir / f"{slug}_{ts}_post_steps.png"
            await page.screenshot(path=str(post_screenshot_file), full_page=True)

        post_candidates = await _collect_candidates(page, candidate_limit)
        post_candidates_file = output_dir / f"{slug}_{ts}_post_steps_candidates.json"
        post_candidates_file.write_text(json.dumps(post_candidates, indent=2), encoding="utf-8")
        download_like_links = await _collect_download_like_links(page, limit=80)

    if response_tasks:
        await asyncio.gather(*response_tasks, return_exceptions=True)
    if download_tasks:
        await asyncio.gather(*download_tasks, return_exceptions=True)

    # Prevent duplicate handlers if multiple exports are inspected in one run.
    page.remove_listener("download", _on_download)
    page.remove_listener("requestfailed", _on_request_failed)
    page.remove_listener("console", _on_console)
    page.remove_listener("pageerror", _on_page_error)
    page.remove_listener("response", _on_response)

    summary = {
        "timestamp": datetime.now().isoformat(),
        "export_name": name,
        "source_url": export["url"],
        "final_url": page.url,
        "title": title,
        "execute_steps": execute_steps,
        "step_results": step_results,
        "expected_download_from_last_step": expected_download,
        "observed_download_events": event_state["download_events"],
        "saved_download_files": event_state["saved_download_files"],
        "download_save_errors": event_state["download_save_errors"],
        "request_failures": event_state["request_failures"],
        "console_errors": event_state["console_errors"],
        "page_errors": event_state["page_errors"],
        "interesting_responses": event_state["interesting_responses"],
        "response_capture_errors": event_state["response_capture_errors"],
        "download_like_links_post_steps": download_like_links,
        "configured_steps": export.get("steps", []),
        "selector_probes": selector_probes,
        "html_file": str(html_file),
        "post_html_file": str(post_html_file) if post_html_file else None,
        "screenshot_file": str(screenshot_file) if screenshot_file else None,
        "post_screenshot_file": str(post_screenshot_file) if post_screenshot_file else None,
        "candidates_file": str(candidates_file),
        "post_candidates_file": str(post_candidates_file) if post_candidates_file else None,
    }
    summary_file = output_dir / f"{slug}_{ts}_summary.json"
    summary_file.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    return summary_file


async def _run(args):
    # Fail fast if credentials are missing.
    _get_credentials()

    output_dir = Path(args.out_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    exports = _filter_exports(args.export)
    print(f"Inspecting {len(exports)} export page(s). Output directory: {output_dir}")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=args.headless)
        context = await browser.new_context(accept_downloads=True)
        page = await context.new_page()

        session_loaded = await load_session(context)
        needs_login = True

        if session_loaded and await is_session_valid(page):
            needs_login = False
            print("Using existing saved Juniper session.")
        elif session_loaded:
            clear_session()

        if needs_login:
            if args.headless:
                print("Headless login is not practical for MFA. Relaunching headed browser for login.")
                await browser.close()
                browser = await p.chromium.launch(headless=False)
                context = await browser.new_context(accept_downloads=True)
                page = await context.new_page()
            print("No valid session found. Complete MFA in the browser window when prompted.")
            await _login(page, context, progress_callback=None)

        generated = []
        failed = []
        for export in exports:
            print(f"Capturing HTML for: {export['name']}")
            try:
                summary_file = await _inspect_export(
                    page=page,
                    export=export,
                    output_dir=output_dir,
                    settle_seconds=args.settle_seconds,
                    candidate_limit=args.candidate_limit,
                    screenshot=not args.no_screenshot,
                    nav_timeout_ms=args.nav_timeout_ms,
                    execute_steps=args.execute_steps,
                    post_click_wait_seconds=args.post_click_wait_seconds,
                    expect_download_timeout_ms=args.expect_download_timeout_ms,
                )
                generated.append(summary_file)
                print(f"  wrote summary: {summary_file}")
            except PlaywrightTimeoutError as e:
                failed.append((export["name"], str(e)))
                print(f"  timeout while loading {export['url']}")
            except Exception as e:
                failed.append((export["name"], str(e)))
                print(f"  failed: {e}")

        await browser.close()

    print("\nDone. Generated files:")
    for path in generated:
        print(f"  - {path}")
    if failed:
        print("\nFailed exports:")
        for name, err in failed:
            print(f"  - {name}: {err}")


def _build_parser():
    parser = argparse.ArgumentParser(
        description="Standalone Juniper export-page inspector. Captures HTML and selector diagnostics without clicking export buttons."
    )
    parser.add_argument(
        "--export",
        action="append",
        help="Export name from juniper/config.py (repeatable). Default: inspect all configured exports.",
    )
    parser.add_argument(
        "--out-dir",
        default="juniper_debug_html",
        help="Directory to write HTML/screenshots/JSON output.",
    )
    parser.add_argument(
        "--headless",
        action="store_true",
        help="Run browser headless when possible (if login is required, script relaunches headed for MFA).",
    )
    parser.add_argument(
        "--settle-seconds",
        type=float,
        default=1.5,
        help="Extra wait after navigation before capturing page content.",
    )
    parser.add_argument(
        "--nav-timeout-ms",
        type=int,
        default=max(NAV_TIMEOUT, 60_000),
        help="Timeout for page navigation during inspection.",
    )
    parser.add_argument(
        "--candidate-limit",
        type=int,
        default=120,
        help="Maximum number of clickable candidates to capture per page.",
    )
    parser.add_argument(
        "--execute-steps",
        action="store_true",
        help="Execute configured click steps for debug and capture post-click state.",
    )
    parser.add_argument(
        "--post-click-wait-seconds",
        type=float,
        default=15.0,
        help="How long to wait after step execution before collecting post-click diagnostics.",
    )
    parser.add_argument(
        "--expect-download-timeout-ms",
        type=int,
        default=DOWNLOAD_TIMEOUT,
        help="Timeout to wait for a download event on the final configured step.",
    )
    parser.add_argument(
        "--no-screenshot",
        action="store_true",
        help="Skip screenshot capture.",
    )
    return parser


def main():
    parser = _build_parser()
    args = parser.parse_args()
    try:
        asyncio.run(_run(args))
    except ValueError as e:
        parser.error(str(e))


if __name__ == "__main__":
    main()
