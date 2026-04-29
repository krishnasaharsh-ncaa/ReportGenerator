import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


TEST_DIR = os.path.dirname(__file__)
REPORT_GENERATOR_DIR = os.path.dirname(TEST_DIR)
if REPORT_GENERATOR_DIR not in sys.path:
    sys.path.insert(0, REPORT_GENERATOR_DIR)

import app as app_module
from storage import StoredArtifact


class FakeStore:
    backend_name = "sharepoint"

    def __init__(self, files=None):
        self.files = files or {}

    def put_source_file(self, filename, content, content_type=None, metadata=None):
        stored_metadata = {
            "filename": filename,
            "exists": True,
            "backend": self.backend_name,
            "modified": "2026-04-24T12:00:00Z",
            "size": len(content),
            "origin": metadata.get("origin") if metadata else "manual_upload",
            "resolved_name": filename,
        }
        if metadata:
            stored_metadata.update(metadata)
        self.files[filename] = (content, stored_metadata)
        return stored_metadata

    def get_source_file(self, filename):
        if filename not in self.files:
            raise FileNotFoundError(filename)
        content, metadata = self.files[filename]
        return StoredArtifact(content=content, metadata=metadata, content_type="application/octet-stream")

    def list_source_files(self):
        return {name: metadata for name, (_, metadata) in self.files.items()}

    def source_file_exists(self, filename):
        return filename in self.files


class AppIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.original_data_dir = app_module.DATA_DIR
        self.original_local_cache_dir = app_module.LOCAL_CACHE_DIR
        self.original_output_dir = app_module.OUTPUT_DIR
        self.original_local_work_dir = app_module.LOCAL_WORK_DIR
        self.original_store = app_module.ARTIFACT_STORE
        self.original_refresh_enabled = app_module.ENABLE_JUNIPER_REFRESH
        self.original_upload_enabled = app_module.ENABLE_SOURCE_UPLOADS
        self.original_credential_setup_enabled = app_module.ENABLE_CREDENTIAL_SETUP

        app_module.DATA_DIR = self.tmpdir.name
        app_module.LOCAL_CACHE_DIR = self.tmpdir.name
        app_module.OUTPUT_DIR = self.tmpdir.name
        app_module.LOCAL_WORK_DIR = self.tmpdir.name
        app_module.ENABLE_JUNIPER_REFRESH = False
        app_module.ENABLE_SOURCE_UPLOADS = True
        app_module.ENABLE_CREDENTIAL_SETUP = True
        os.makedirs(app_module.DATA_DIR, exist_ok=True)

        files = {
            "Accounts.csv": (
                b"Account ID,Legal name\n1,Example LLC\n",
                {
                    "filename": "Accounts.csv",
                    "exists": True,
                    "backend": "sharepoint",
                    "modified": "2026-04-24T12:00:00Z",
                    "size": 36,
                    "origin": "juniper",
                    "resolved_name": "Accounts.csv",
                },
            ),
            "Entity_overview.xlsx": (
                b"entity-bytes",
                {
                    "filename": "Entity_overview.xlsx",
                    "exists": True,
                    "backend": "sharepoint",
                    "modified": "2026-04-24T12:00:00Z",
                    "size": 12,
                    "origin": "outlook",
                    "resolved_name": "Investment_Entity_Export_-_Evening_20260424.xlsx",
                },
            ),
            "js_contacts.csv": (
                b"First name,Last name\nAda,Lovelace\n",
                {
                    "filename": "js_contacts.csv",
                    "exists": True,
                    "backend": "sharepoint",
                    "modified": "2026-04-24T12:00:00Z",
                    "size": 35,
                    "origin": "juniper",
                    "resolved_name": "js_contacts.csv",
                },
            ),
            "Contact Export.csv": (
                b"Name\nAda Lovelace\n",
                {
                    "filename": "Contact Export.csv",
                    "exists": True,
                    "backend": "sharepoint",
                    "modified": "2026-04-24T12:00:00Z",
                    "size": 18,
                    "origin": "outlook",
                    "resolved_name": "Contact_Export_-_Evening_20260424.csv",
                },
            ),
        }
        app_module.ARTIFACT_STORE = FakeStore(files)
        self.client = app_module.app.test_client()

    def tearDown(self):
        app_module.DATA_DIR = self.original_data_dir
        app_module.LOCAL_CACHE_DIR = self.original_local_cache_dir
        app_module.OUTPUT_DIR = self.original_output_dir
        app_module.LOCAL_WORK_DIR = self.original_local_work_dir
        app_module.ARTIFACT_STORE = self.original_store
        app_module.ENABLE_JUNIPER_REFRESH = self.original_refresh_enabled
        app_module.ENABLE_SOURCE_UPLOADS = self.original_upload_enabled
        app_module.ENABLE_CREDENTIAL_SETUP = self.original_credential_setup_enabled
        self.tmpdir.cleanup()

    def test_status_uses_store_metadata(self):
        response = self.client.get("/api/status")
        payload = response.get_json()

        self.assertEqual(response.status_code, 200)
        self.assertEqual(payload["storage_backend"], "sharepoint")
        self.assertTrue(payload["credential_setup_enabled"])
        accounts = next(item for item in payload["files"] if item["filename"] == "Accounts.csv")
        self.assertTrue(accounts["exists"])
        self.assertEqual(accounts["backend"], "sharepoint")
        self.assertEqual(accounts["origin"], "juniper")
        self.assertEqual(accounts["resolved_name"], "Accounts.csv")
        self.assertEqual(accounts["modified"], "Apr 24, 2026 08:00")
        entity_overview = next(item for item in payload["files"] if item["filename"] == "Entity_overview.xlsx")
        self.assertEqual(entity_overview["origin"], "outlook")
        self.assertEqual(
            entity_overview["resolved_name"],
            "Investment_Entity_Export_-_Evening_20260424.xlsx",
        )

    def test_generate_hydrates_store_files_before_running_report(self):
        def fake_run(cmd, **kwargs):
            output_path = cmd[cmd.index("--output-path") + 1]
            Path(output_path).parent.mkdir(parents=True, exist_ok=True)
            Path(output_path).write_bytes(b"%PDF-1.4\n")
            return mock.Mock(returncode=0, stderr="", stdout="")

        with mock.patch("app.subprocess.run") as mocked_run:
            mocked_run.side_effect = fake_run
            response = self.client.post("/api/generate/report2")

        payload = response.get_json()
        self.assertEqual(response.status_code, 200)
        self.assertTrue(payload["success"])
        self.assertIn("Accounts.csv", payload["hydrated_files"])
        self.assertTrue(os.path.exists(os.path.join(app_module.OUTPUT_DIR, "Entity_Metrics_Report.pdf")))

    def test_refresh_route_is_disabled_when_worker_mode_is_off(self):
        response = self.client.post("/api/refresh", json={})
        payload = response.get_json()

        self.assertEqual(response.status_code, 503)
        self.assertEqual(payload["refresh_enabled"], False)

    def test_credential_routes_are_guarded_when_disabled(self):
        app_module.ENABLE_CREDENTIAL_SETUP = False

        check_response = self.client.get("/api/credentials/check")
        setup_response = self.client.post(
            "/api/credentials/setup",
            json={"email": "user@example.com", "password": "secret"},
        )

        self.assertEqual(check_response.status_code, 403)
        self.assertEqual(setup_response.status_code, 403)
        self.assertFalse(check_response.get_json()["credential_setup_enabled"])


if __name__ == "__main__":
    unittest.main()
