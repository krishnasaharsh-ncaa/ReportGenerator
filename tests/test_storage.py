import os
import sys
import tempfile
import unittest


TEST_DIR = os.path.dirname(__file__)
REPORT_GENERATOR_DIR = os.path.dirname(TEST_DIR)
if REPORT_GENERATOR_DIR not in sys.path:
    sys.path.insert(0, REPORT_GENERATOR_DIR)

from storage import LocalArtifactStore, SharePointArtifactStore


class FakeResponse:
    def __init__(self, status_code=200, json_data=None, content=b"", headers=None, text=""):
        self.status_code = status_code
        self._json_data = json_data or {}
        self.content = content
        self.headers = headers or {}
        self.text = text or str(self._json_data)

    def json(self):
        return self._json_data


class FakeTransport:
    def __init__(self, handler):
        self.handler = handler
        self.calls = []

    def request(self, method, url, **kwargs):
        self.calls.append((method, url, kwargs))
        return self.handler(method, url, **kwargs)


class LocalArtifactStoreTests(unittest.TestCase):
    def test_put_get_and_list_round_trip(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            store = LocalArtifactStore(tmpdir)
            stored = store.put_source_file("Accounts.csv", b"abc,123\n", metadata={"source": "test"})

            self.assertEqual(stored["filename"], "Accounts.csv")
            self.assertTrue(store.source_file_exists("Accounts.csv"))

            artifact = store.get_source_file("Accounts.csv")
            self.assertEqual(artifact.content, b"abc,123\n")
            self.assertEqual(artifact.metadata["filename"], "Accounts.csv")

            listed = store.list_source_files()
            self.assertIn("Accounts.csv", listed)
            self.assertEqual(listed["Accounts.csv"]["backend"], "local")
            self.assertEqual(listed["Accounts.csv"]["resolved_name"], "Accounts.csv")
            self.assertEqual(listed["Accounts.csv"]["origin"], "juniper")


class SharePointArtifactStoreTests(unittest.TestCase):
    def test_put_get_and_list_use_expected_graph_paths(self):
        def handler(method, url, **kwargs):
            if url.endswith("/root:/ReportGenerator/source-files/Accounts.csv:/content") and method == "PUT":
                return FakeResponse(
                    json_data={
                        "id": "item-1",
                        "name": "Accounts.csv",
                        "size": 9,
                        "eTag": "etag-1",
                        "lastModifiedDateTime": "2026-04-24T12:00:00Z",
                        "webUrl": "https://sharepoint.example/file",
                    }
                )
            if url.endswith("/root:/ReportGenerator/source-files/Accounts.csv") and method == "GET":
                return FakeResponse(
                    json_data={
                        "id": "item-1",
                        "name": "Accounts.csv",
                        "size": 9,
                        "eTag": "etag-1",
                        "lastModifiedDateTime": "2026-04-24T12:00:00Z",
                        "webUrl": "https://sharepoint.example/file",
                    }
                )
            if url.endswith("/items/item-1/content") and method == "GET":
                return FakeResponse(content=b"abc,123\n", headers={"Content-Type": "text/csv"})
            if url.endswith("/root:/ReportGenerator/source-files:/children") and method == "GET":
                return FakeResponse(
                    json_data={
                        "value": [
                            {
                                "id": "item-1",
                                "name": "Accounts.csv",
                                "size": 9,
                                "eTag": "etag-1",
                                "lastModifiedDateTime": "2026-04-24T12:00:00Z",
                                "webUrl": "https://sharepoint.example/file",
                            }
                        ]
                    }
                )
            raise AssertionError(f"Unexpected Graph call: {method} {url}")

        transport = FakeTransport(handler)
        store = SharePointArtifactStore(
            tenant_id="tenant-id",
            client_id="client-id",
            drive_id="drive-id",
            source_folder="/ReportGenerator/source-files",
            access_token_provider=lambda: "token",
            transport=transport,
        )
        store._ensure_folder_exists = lambda: None

        stored = store.put_source_file("Accounts.csv", b"abc,123\n", metadata={"source": "test"})
        self.assertEqual(stored["backend"], "sharepoint")
        self.assertEqual(stored["etag"], "etag-1")

        artifact = store.get_source_file("Accounts.csv")
        self.assertEqual(artifact.content, b"abc,123\n")
        self.assertEqual(artifact.metadata["web_url"], "https://sharepoint.example/file")

        listed = store.list_source_files()
        self.assertIn("Accounts.csv", listed)
        self.assertEqual(listed["Accounts.csv"]["size"], 9)
        self.assertEqual(listed["Accounts.csv"]["origin"], "juniper")

    def test_prefix_fallback_resolves_newest_outlook_file(self):
        def handler(method, url, **kwargs):
            if url.endswith("/root:/ReportGenerator/source-files/Contact%20Export.csv") and method == "GET":
                return FakeResponse(status_code=404, text="not found")
            if url.endswith("/root:/ReportGenerator/source-files/Entity_overview.xlsx") and method == "GET":
                return FakeResponse(status_code=404, text="not found")
            if url.endswith("/root:/ReportGenerator/source-files:/children") and method == "GET":
                return FakeResponse(
                    json_data={
                        "value": [
                            {
                                "id": "old-item",
                                "name": "Contact_Export_-_Morning_20260424.csv",
                                "size": 12,
                                "eTag": "old-etag",
                                "lastModifiedDateTime": "2026-04-24T08:00:00Z",
                                "webUrl": "https://sharepoint.example/old",
                            },
                            {
                                "id": "new-item",
                                "name": "Contact_Export_-_Evening_20260424.csv",
                                "size": 18,
                                "eTag": "new-etag",
                                "lastModifiedDateTime": "2026-04-24T20:00:00Z",
                                "webUrl": "https://sharepoint.example/new",
                            },
                        ]
                    }
                )
            if url.endswith("/items/new-item/content") and method == "GET":
                return FakeResponse(content=b"name\nlatest\n", headers={"Content-Type": "text/csv"})
            raise AssertionError(f"Unexpected Graph call: {method} {url}")

        store = SharePointArtifactStore(
            tenant_id="tenant-id",
            client_id="client-id",
            drive_id="drive-id",
            source_folder="/ReportGenerator/source-files",
            access_token_provider=lambda: "token",
            transport=FakeTransport(handler),
        )

        artifact = store.get_source_file("Contact Export.csv")
        self.assertEqual(artifact.content, b"name\nlatest\n")
        self.assertEqual(artifact.metadata["origin"], "outlook")
        self.assertEqual(
            artifact.metadata["resolved_name"],
            "Contact_Export_-_Evening_20260424.csv",
        )

        listed = store.list_source_files()
        self.assertEqual(
            listed["Contact Export.csv"]["resolved_name"],
            "Contact_Export_-_Evening_20260424.csv",
        )

    def test_prefix_fallback_prefers_filename_timestamp_over_modified_time(self):
        def handler(method, url, **kwargs):
            if url.endswith("/root:/ReportGenerator/source-files/Contact%20Export.csv") and method == "GET":
                return FakeResponse(status_code=404, text="not found")
            if url.endswith("/root:/ReportGenerator/source-files/Entity_overview.xlsx") and method == "GET":
                return FakeResponse(status_code=404, text="not found")
            if url.endswith("/root:/ReportGenerator/source-files:/children") and method == "GET":
                return FakeResponse(
                    json_data={
                        "value": [
                            {
                                "id": "evening-item",
                                "name": "Contact_Export_-_Evening_2026-04-28T1800",
                                "size": 12,
                                "eTag": "evening-etag",
                                "lastModifiedDateTime": "2026-04-29T08:00:00Z",
                                "webUrl": "https://sharepoint.example/evening",
                            },
                            {
                                "id": "morning-item",
                                "name": "Contact_Export_-_Morning_2026-04-29T0800",
                                "size": 18,
                                "eTag": "morning-etag",
                                "lastModifiedDateTime": "2026-04-28T20:00:00Z",
                                "webUrl": "https://sharepoint.example/morning",
                            },
                        ]
                    }
                )
            if url.endswith("/items/morning-item/content") and method == "GET":
                return FakeResponse(content=b"name\nlatest\n", headers={"Content-Type": "text/csv"})
            raise AssertionError(f"Unexpected Graph call: {method} {url}")

        store = SharePointArtifactStore(
            tenant_id="tenant-id",
            client_id="client-id",
            drive_id="drive-id",
            source_folder="/ReportGenerator/source-files",
            access_token_provider=lambda: "token",
            transport=FakeTransport(handler),
        )

        artifact = store.get_source_file("Contact Export.csv")
        self.assertEqual(
            artifact.metadata["resolved_name"],
            "Contact_Export_-_Morning_2026-04-29T0800",
        )


if __name__ == "__main__":
    unittest.main()
