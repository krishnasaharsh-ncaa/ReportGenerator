import json
import mimetypes
import os
import re
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timezone
from tempfile import NamedTemporaryFile
from typing import Any, Callable, Dict, Optional
from urllib.parse import quote


DEFAULT_SOURCE_FILE_RULES = {
    "Accounts.csv": {
        "exact_name": "Accounts.csv",
        "prefixes": [],
        "origin": "juniper",
    },
    "js_contacts.csv": {
        "exact_name": "js_contacts.csv",
        "prefixes": [],
        "origin": "juniper",
    },
    "Contact Export.csv": {
        "exact_name": "Contact Export.csv",
        "prefixes": ["Contact_Export_-_"],
        "origin": "outlook",
    },
    "Entity_overview.xlsx": {
        "exact_name": "Entity_overview.xlsx",
        "prefixes": ["Investment_Entity_Export_-_"],
        "origin": "outlook",
    },
}


class StorageConfigurationError(RuntimeError):
    """Raised when a storage backend is selected but not fully configured."""


class ArtifactNotFoundError(FileNotFoundError):
    """Raised when a source file does not exist in the configured store."""


@dataclass
class StoredArtifact:
    content: bytes
    metadata: Dict[str, Any]
    content_type: Optional[str] = None


class ArtifactStore(ABC):
    backend_name = "unknown"

    @abstractmethod
    def put_source_file(
        self,
        filename: str,
        content: bytes,
        content_type: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def get_source_file(self, filename: str) -> StoredArtifact:
        raise NotImplementedError

    @abstractmethod
    def list_source_files(self) -> Dict[str, Dict[str, Any]]:
        raise NotImplementedError

    @abstractmethod
    def source_file_exists(self, filename: str) -> bool:
        raise NotImplementedError


def _guess_content_type(filename: str) -> str:
    return mimetypes.guess_type(filename)[0] or "application/octet-stream"


def _write_bytes_atomically(path: str, content: bytes) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with NamedTemporaryFile(dir=os.path.dirname(path), delete=False) as tmp:
        tmp.write(content)
        tmp_path = tmp.name
    os.replace(tmp_path, path)


def cache_source_file(filename: str, content: bytes, cache_dir: str) -> str:
    path = os.path.join(cache_dir, filename)
    _write_bytes_atomically(path, content)
    return path


def hydrate_source_files(store: ArtifactStore, filenames: list[str], cache_dir: str) -> Dict[str, Dict[str, Any]]:
    hydrated: Dict[str, Dict[str, Any]] = {}
    for filename in filenames:
        artifact = store.get_source_file(filename)
        path = cache_source_file(filename, artifact.content, cache_dir)
        hydrated[filename] = {**artifact.metadata, "cache_path": path}
    return hydrated


def get_local_cache_dir(default_dir: str) -> str:
    return os.environ.get("LOCAL_CACHE_DIR", default_dir)


def _normalize_rule(filename: str, rule: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    base_rule = {
        "exact_name": filename,
        "prefixes": [],
        "origin": "unknown",
    }
    if rule:
        base_rule.update(rule)
    prefixes = base_rule.get("prefixes") or []
    base_rule["prefixes"] = [prefix for prefix in prefixes if prefix]
    if not base_rule.get("exact_name"):
        base_rule["exact_name"] = filename
    return base_rule


def load_source_file_rules() -> Dict[str, Dict[str, Any]]:
    rules = {
        filename: _normalize_rule(filename, rule)
        for filename, rule in DEFAULT_SOURCE_FILE_RULES.items()
    }

    raw_rules = os.environ.get("SOURCE_FILE_RULES")
    if not raw_rules:
        return rules

    try:
        custom_rules = json.loads(raw_rules)
    except json.JSONDecodeError as e:
        raise StorageConfigurationError(f"Invalid SOURCE_FILE_RULES JSON: {e}") from e

    if not isinstance(custom_rules, dict):
        raise StorageConfigurationError("SOURCE_FILE_RULES must be a JSON object keyed by logical filename.")

    for filename, rule in custom_rules.items():
        rules[filename] = _normalize_rule(filename, rule if isinstance(rule, dict) else None)
    return rules


def _parse_datetime(value: Optional[str]) -> datetime:
    if not value:
        return datetime.fromtimestamp(0, tz=timezone.utc)
    normalized = value.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


FILENAME_TIMESTAMP_RE = re.compile(r"(\d{4}-\d{2}-\d{2})T(\d{4}|\d{6})")


def _extract_filename_timestamp(name: Optional[str]) -> Optional[datetime]:
    if not name:
        return None
    match = FILENAME_TIMESTAMP_RE.search(name)
    if not match:
        return None

    date_part, time_part = match.groups()
    if len(time_part) == 4:
        fmt = "%Y-%m-%dT%H%M"
    else:
        fmt = "%Y-%m-%dT%H%M%S"

    try:
        parsed = datetime.strptime(f"{date_part}T{time_part}", fmt)
    except ValueError:
        return None
    return parsed.replace(tzinfo=timezone.utc)


class LocalArtifactStore(ArtifactStore):
    backend_name = "local"

    def __init__(self, root_dir: str, source_file_rules: Optional[Dict[str, Dict[str, Any]]] = None):
        self.root_dir = root_dir
        self.source_file_rules = source_file_rules or load_source_file_rules()
        os.makedirs(self.root_dir, exist_ok=True)

    def _path_for(self, filename: str) -> str:
        return os.path.join(self.root_dir, filename)

    def _rule_for(self, filename: str) -> Dict[str, Any]:
        return _normalize_rule(filename, self.source_file_rules.get(filename))

    def _build_metadata(self, filename: str, path: str, exists: bool) -> Dict[str, Any]:
        rule = self._rule_for(filename)
        metadata: Dict[str, Any] = {
            "filename": filename,
            "exists": exists,
            "backend": self.backend_name,
            "path": path,
            "resolved_name": rule["exact_name"],
            "origin": rule["origin"],
        }
        if exists:
            stat = os.stat(path)
            modified = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc)
            metadata.update(
                {
                    "size": stat.st_size,
                    "modified": modified.isoformat(),
                }
            )
        return metadata

    def put_source_file(
        self,
        filename: str,
        content: bytes,
        content_type: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        if not content:
            raise ValueError(f"{filename} is empty.")
        path = self._path_for(filename)
        _write_bytes_atomically(path, content)
        stored_metadata = self._build_metadata(filename, path, exists=True)
        if metadata:
            stored_metadata.update(metadata)
        stored_metadata.setdefault("resolved_name", filename)
        stored_metadata.setdefault("origin", self._rule_for(filename)["origin"])
        stored_metadata["content_type"] = content_type or _guess_content_type(filename)
        return stored_metadata

    def get_source_file(self, filename: str) -> StoredArtifact:
        path = self._path_for(filename)
        if not os.path.exists(path):
            raise ArtifactNotFoundError(filename)
        with open(path, "rb") as f:
            content = f.read()
        return StoredArtifact(
            content=content,
            metadata=self._build_metadata(filename, path, exists=True),
            content_type=_guess_content_type(filename),
        )

    def list_source_files(self) -> Dict[str, Dict[str, Any]]:
        files: Dict[str, Dict[str, Any]] = {}
        for filename in os.listdir(self.root_dir):
            path = self._path_for(filename)
            if os.path.isfile(path):
                files[filename] = self._build_metadata(filename, path, exists=True)
        return files

    def source_file_exists(self, filename: str) -> bool:
        return os.path.exists(self._path_for(filename))


class SharePointArtifactStore(ArtifactStore):
    backend_name = "sharepoint"

    def __init__(
        self,
        tenant_id: str,
        client_id: str,
        drive_id: Optional[str] = None,
        site_id: Optional[str] = None,
        source_folder: str = "/ReportGenerator/source-files",
        client_secret: Optional[str] = None,
        certificate_path: Optional[str] = None,
        certificate_thumbprint: Optional[str] = None,
        certificate_passphrase: Optional[str] = None,
        access_token_provider: Optional[Callable[[], str]] = None,
        transport: Optional[Any] = None,
        timeout: Optional[int] = None,
        source_file_rules: Optional[Dict[str, Dict[str, Any]]] = None,
    ):
        if not tenant_id or not client_id:
            raise StorageConfigurationError("SharePoint storage requires tenant and client IDs.")
        if not drive_id and not site_id:
            raise StorageConfigurationError("SharePoint storage requires either SHAREPOINT_DRIVE_ID or SHAREPOINT_SITE_ID.")

        self.tenant_id = tenant_id
        self.client_id = client_id
        self.drive_id = drive_id
        self.site_id = site_id
        self.source_folder = (source_folder or "/").strip()
        self.client_secret = client_secret
        self.certificate_path = certificate_path
        self.certificate_thumbprint = certificate_thumbprint
        self.certificate_passphrase = certificate_passphrase
        self.timeout = int(timeout or os.environ.get("SHAREPOINT_TIMEOUT_SECONDS", "30"))
        self.source_file_rules = source_file_rules or load_source_file_rules()
        self._transport = transport
        self._access_token_provider = access_token_provider
        self._resolved_drive_id: Optional[str] = None

    @classmethod
    def from_env(cls) -> "SharePointArtifactStore":
        return cls(
            tenant_id=os.environ.get("SHAREPOINT_TENANT_ID", ""),
            client_id=os.environ.get("SHAREPOINT_CLIENT_ID", ""),
            drive_id=os.environ.get("SHAREPOINT_DRIVE_ID"),
            site_id=os.environ.get("SHAREPOINT_SITE_ID"),
            source_folder=os.environ.get("SHAREPOINT_SOURCE_FOLDER", "/ReportGenerator/source-files"),
            client_secret=os.environ.get("SHAREPOINT_CLIENT_SECRET"),
            certificate_path=os.environ.get("SHAREPOINT_CERT_PATH"),
            certificate_thumbprint=os.environ.get("SHAREPOINT_CERT_THUMBPRINT"),
            certificate_passphrase=os.environ.get("SHAREPOINT_CERT_PASSPHRASE"),
            timeout=os.environ.get("SHAREPOINT_TIMEOUT_SECONDS"),
            source_file_rules=load_source_file_rules(),
        )

    def _rule_for(self, filename: str) -> Dict[str, Any]:
        return _normalize_rule(filename, self.source_file_rules.get(filename))

    def _session(self) -> Any:
        if self._transport is None:
            import requests

            self._transport = requests.Session()
        return self._transport

    def _build_token_provider(self) -> Callable[[], str]:
        if self.client_secret:
            credential: Any = self.client_secret
        elif self.certificate_path and self.certificate_thumbprint:
            with open(self.certificate_path, "r", encoding="utf-8") as f:
                private_key = f.read()
            credential = {
                "private_key": private_key,
                "thumbprint": self.certificate_thumbprint,
            }
            if self.certificate_passphrase:
                credential["passphrase"] = self.certificate_passphrase
        else:
            raise StorageConfigurationError(
                "SharePoint storage requires either SHAREPOINT_CLIENT_SECRET or a certificate configuration."
            )

        import msal

        app = msal.ConfidentialClientApplication(
            client_id=self.client_id,
            authority=f"https://login.microsoftonline.com/{self.tenant_id}",
            client_credential=credential,
        )

        def _provider() -> str:
            result = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])
            token = result.get("access_token")
            if not token:
                raise StorageConfigurationError(f"Failed to acquire Microsoft Graph token: {result}")
            return token

        return _provider

    def _get_access_token(self) -> str:
        if self._access_token_provider is None:
            self._access_token_provider = self._build_token_provider()
        return self._access_token_provider()

    def _graph_request(self, method: str, path: str, **kwargs: Any) -> Any:
        headers = dict(kwargs.pop("headers", {}) or {})
        headers["Authorization"] = f"Bearer {self._get_access_token()}"
        response = self._session().request(
            method,
            f"https://graph.microsoft.com/v1.0{path}",
            headers=headers,
            timeout=self.timeout,
            **kwargs,
        )
        return response

    def _path_fragment(self, filename: Optional[str] = None) -> str:
        parts = [self.source_folder.strip("/")] if self.source_folder.strip("/") else []
        if filename:
            parts.append(filename)
        joined = "/".join(parts)
        return f"/{quote(joined, safe='/')}" if joined else ""

    def _ensure_drive_id(self) -> str:
        if self._resolved_drive_id:
            return self._resolved_drive_id
        if self.drive_id:
            self._resolved_drive_id = self.drive_id
            return self._resolved_drive_id

        response = self._graph_request("GET", f"/sites/{self.site_id}/drive")
        if response.status_code >= 400:
            raise StorageConfigurationError(
                f"Unable to resolve SharePoint drive from site {self.site_id}: {response.text}"
            )
        self._resolved_drive_id = response.json()["id"]
        return self._resolved_drive_id

    def _response_json(self, response: Any) -> Dict[str, Any]:
        if response.status_code >= 400:
            raise RuntimeError(f"SharePoint request failed ({response.status_code}): {getattr(response, 'text', '')}")
        return response.json()

    def _item_metadata_to_dict(
        self,
        logical_filename: str,
        item: Dict[str, Any],
        resolved_name: Optional[str] = None,
        origin: Optional[str] = None,
    ) -> Dict[str, Any]:
        return {
            "filename": logical_filename,
            "exists": True,
            "backend": self.backend_name,
            "size": item.get("size"),
            "modified": item.get("lastModifiedDateTime"),
            "etag": item.get("eTag"),
            "id": item.get("id"),
            "web_url": item.get("webUrl"),
            "resolved_name": resolved_name or item.get("name") or logical_filename,
            "origin": origin or self._rule_for(logical_filename)["origin"],
        }

    def _children_path(self) -> str:
        folder = self._path_fragment()
        if folder:
            return f"/drives/{self._ensure_drive_id()}/root:{folder}:/children"
        return f"/drives/{self._ensure_drive_id()}/root/children"

    def _ensure_folder_exists(self) -> None:
        drive_id = self._ensure_drive_id()
        folder = self.source_folder.strip("/")
        if not folder:
            return

        parent_id = "root"
        current_path = ""
        for segment in folder.split("/"):
            current_path = f"{current_path}/{segment}" if current_path else segment
            lookup = self._graph_request("GET", f"/drives/{drive_id}/root:/{quote(current_path, safe='/')}")
            if lookup.status_code == 200:
                parent_id = lookup.json()["id"]
                continue
            if lookup.status_code != 404:
                raise RuntimeError(
                    f"Failed to inspect SharePoint folder '{current_path}' ({lookup.status_code}): {lookup.text}"
                )

            create = self._graph_request(
                "POST",
                f"/drives/{drive_id}/items/{parent_id}/children",
                headers={"Content-Type": "application/json"},
                json={
                    "name": segment,
                    "folder": {},
                    "@microsoft.graph.conflictBehavior": "replace",
                },
            )
            if create.status_code >= 400:
                raise RuntimeError(
                    f"Failed to create SharePoint folder '{current_path}' ({create.status_code}): {create.text}"
                )
            parent_id = create.json()["id"]

    def _list_folder_items(self) -> list[Dict[str, Any]]:
        response = self._graph_request("GET", self._children_path())
        if response.status_code == 404:
            return []
        payload = self._response_json(response)
        return payload.get("value", [])

    def _lookup_exact_item(self, remote_name: str) -> Optional[Dict[str, Any]]:
        response = self._graph_request(
            "GET",
            f"/drives/{self._ensure_drive_id()}/root:{self._path_fragment(remote_name)}",
        )
        if response.status_code == 404:
            return None
        return self._response_json(response)

    def _pick_latest_prefix_match(self, prefixes: list[str], items: list[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        matches = [
            item
            for item in items
            if any((item.get("name") or "").startswith(prefix) for prefix in prefixes)
        ]
        if not matches:
            return None
        return max(
            matches,
            key=lambda item: (
                _extract_filename_timestamp(item.get("name")) is not None,
                _extract_filename_timestamp(item.get("name")) or datetime.fromtimestamp(0, tz=timezone.utc),
                _parse_datetime(item.get("lastModifiedDateTime")),
            ),
        )

    def _resolve_item_for_filename(
        self,
        filename: str,
        folder_items: Optional[list[Dict[str, Any]]] = None,
    ) -> tuple[Dict[str, Any], Dict[str, Any]]:
        rule = self._rule_for(filename)
        exact_name = rule["exact_name"]

        if folder_items is None:
            exact_item = self._lookup_exact_item(exact_name)
            if exact_item is not None:
                return exact_item, rule
            if not rule["prefixes"]:
                raise ArtifactNotFoundError(filename)
            folder_items = self._list_folder_items()
        else:
            exact_item = next((item for item in folder_items if item.get("name") == exact_name), None)
            if exact_item is not None:
                return exact_item, rule

        latest_prefix_item = self._pick_latest_prefix_match(rule["prefixes"], folder_items or [])
        if latest_prefix_item is None:
            raise ArtifactNotFoundError(filename)
        return latest_prefix_item, rule

    def put_source_file(
        self,
        filename: str,
        content: bytes,
        content_type: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        if not content:
            raise ValueError(f"{filename} is empty.")
        self._ensure_folder_exists()
        rule = self._rule_for(filename)
        remote_name = rule["exact_name"]
        drive_id = self._ensure_drive_id()
        content_type = content_type or _guess_content_type(remote_name)
        response = self._graph_request(
            "PUT",
            f"/drives/{drive_id}/root:{self._path_fragment(remote_name)}:/content",
            headers={"Content-Type": content_type},
            data=content,
        )
        item = self._response_json(response)
        stored_metadata = self._item_metadata_to_dict(
            filename,
            item,
            resolved_name=remote_name,
            origin=rule["origin"],
        )
        if metadata:
            stored_metadata.update(metadata)
        stored_metadata.setdefault("resolved_name", remote_name)
        stored_metadata.setdefault("origin", rule["origin"])
        stored_metadata["content_type"] = content_type
        return stored_metadata

    def get_source_file(self, filename: str) -> StoredArtifact:
        item, rule = self._resolve_item_for_filename(filename)
        item_id = item.get("id")
        if not item_id:
            raise RuntimeError(f"SharePoint file '{filename}' did not include an item ID.")

        content_response = self._graph_request(
            "GET",
            f"/drives/{self._ensure_drive_id()}/items/{item_id}/content",
        )
        if content_response.status_code == 404:
            raise ArtifactNotFoundError(filename)
        if content_response.status_code >= 400:
            raise RuntimeError(
                f"Failed to download SharePoint file '{filename}' ({content_response.status_code}): {content_response.text}"
            )

        return StoredArtifact(
            content=content_response.content,
            metadata=self._item_metadata_to_dict(
                filename,
                item,
                resolved_name=item.get("name"),
                origin=rule["origin"],
            ),
            content_type=content_response.headers.get("Content-Type") or _guess_content_type(item.get("name") or filename),
        )

    def list_source_files(self) -> Dict[str, Dict[str, Any]]:
        folder_items = self._list_folder_items()
        files: Dict[str, Dict[str, Any]] = {}
        for logical_filename in self.source_file_rules:
            try:
                item, rule = self._resolve_item_for_filename(logical_filename, folder_items=folder_items)
            except ArtifactNotFoundError:
                continue
            files[logical_filename] = self._item_metadata_to_dict(
                logical_filename,
                item,
                resolved_name=item.get("name"),
                origin=rule["origin"],
            )
        return files

    def source_file_exists(self, filename: str) -> bool:
        try:
            self._resolve_item_for_filename(filename)
        except ArtifactNotFoundError:
            return False
        return True


def build_artifact_store(default_local_root: str) -> ArtifactStore:
    backend = os.environ.get("STORAGE_BACKEND", "local").strip().lower()
    source_file_rules = load_source_file_rules()
    if backend == "sharepoint":
        return SharePointArtifactStore.from_env()
    if backend != "local":
        raise StorageConfigurationError(f"Unsupported STORAGE_BACKEND: {backend}")
    return LocalArtifactStore(get_local_cache_dir(default_local_root), source_file_rules=source_file_rules)
