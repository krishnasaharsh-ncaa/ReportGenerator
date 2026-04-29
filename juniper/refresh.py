import asyncio
import threading
import logging
from datetime import datetime

from .downloader import run_refresh

logger = logging.getLogger(__name__)

# ── Module-level state shared between the background thread and Flask ───────

_lock = threading.Lock()

_state = {
    "status": "idle",        # idle | running | success | error | partial
    "mfa_required": False,
    "progress": [],          # [{"name": "Contacts", "status": "downloading"}, ...]
    "files_downloaded": [],
    "errors": [],
    "started_at": None,
    "finished_at": None,
}


def _reset_state():
    _state.update({
        "status": "running",
        "mfa_required": False,
        "progress": [],
        "files_downloaded": [],
        "errors": [],
        "started_at": datetime.now().isoformat(),
        "finished_at": None,
    })


def _progress_callback(name, status):
    """Called by the downloader to report progress. Thread-safe via the GIL for dict updates."""
    if name == "login" and status == "mfa_required":
        _state["mfa_required"] = True
        return
    if name == "login" and status == "complete":
        _state["mfa_required"] = False
        return
    if name == "session":
        # Session check status — not a file, just update progress
        return

    # Update or append file progress
    for entry in _state["progress"]:
        if entry["name"] == name:
            entry["status"] = status
            return
    _state["progress"].append({"name": name, "status": status})


def _run_in_thread(artifact_store, local_cache_dir, export_name=None):
    """Background thread target that runs the async refresh."""
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        result = loop.run_until_complete(
            run_refresh(
                artifact_store,
                local_cache_dir,
                _progress_callback,
                export_name=export_name,
            )
        )
        loop.close()

        _state["status"] = result["status"]
        _state["files_downloaded"] = result["files_downloaded"]
        _state["errors"] = result["errors"]
    except Exception as e:
        logger.error("Refresh thread failed: %s", e)
        _state["status"] = "error"
        _state["errors"] = [str(e)]
    finally:
        _state["finished_at"] = datetime.now().isoformat()
        _state["mfa_required"] = False


def start_refresh(artifact_store, local_cache_dir, export_name=None):
    """
    Start a Juniper Square data refresh in a background thread.
    Args:
        artifact_store: Storage backend used as the source of truth for downloaded exports.
        local_cache_dir: Path to the local cache directory used by the current process.
        export_name: Optional name of a single export to refresh (e.g., "Contacts").
                     If None, refreshes all configured exports.
    Returns True if started, False if already running.
    """
    with _lock:
        if _state["status"] == "running":
            return False
        _reset_state()

    thread = threading.Thread(
        target=_run_in_thread,
        args=(artifact_store, local_cache_dir, export_name),
        daemon=True,
    )
    thread.start()
    logger.info("Refresh thread started (export_name=%s).", export_name)
    return True


def get_refresh_status():
    """Return the current refresh state dict (safe to call from Flask route)."""
    return dict(_state)
