SharePoint-Backed Juniper Refresh Plan
Summary
Use SharePoint as the system of record for Juniper source files and keep local disk as temporary staging/cache only. Do not make a OneDrive-synced folder the production architecture. A OneDrive folder is acceptable only as a short-term desktop bridge; it is brittle for hosted Azure/AWS deployments and gives poor observability and failure handling.

Adopt a split architecture:

A Juniper refresh worker stays on a controlled Windows machine or VM that can support Playwright, keyring, and periodic human MFA when the Juniper session expires.
The Report Generator app can later be hosted on Azure or AWS and should read source files from SharePoint, not from a machine-local data/ folder.
This avoids tying the hosted app to desktop sync software while preserving the current Juniper login model.

Key Changes
1. Storage model
Introduce an internal storage abstraction, e.g. ArtifactStore, with these operations:

put_source_file(filename, bytes, content_type, metadata)
get_source_file(filename) -> bytes + metadata
list_source_files() -> metadata
source_file_exists(filename)
Provide two implementations:

LocalArtifactStore for current/local development
SharePointArtifactStore using Microsoft Graph upload/download APIs
Decision:

SharePointArtifactStore becomes the default production backend.
LocalArtifactStore remains for local dev and fallback.
2. Juniper refresh flow
Refactor the current Playwright path in juniper/downloader.py so it:

Downloads to a temp directory on the worker machine.
Validates the file exists and is non-empty.
Uploads the file to SharePoint under a fixed folder such as /ReportGenerator/source-files/.
Optionally updates a local cache copy for the current process, but does not treat local disk as authoritative.
Do not write final outputs directly into UI/ReportGenerator/data/ as the source of truth in production.

3. Report app behavior
Keep the existing Flask endpoints, but change their backing behavior:

/api/status
Read source-file existence and modified timestamps from SharePoint metadata, not local mtime.
/api/refresh
Trigger the worker flow and report SharePoint upload success/failure per export.
/api/generate/<report_id> and /api/generate-all
Before running report scripts, hydrate required inputs from SharePoint into a per-run working directory or refresh the local cache.
Keep report scripts unchanged if possible by still presenting files at the same local paths during execution.
Decision:

Report generation will continue to use local files at runtime, but those files are treated as an ephemeral working copy pulled from SharePoint.
4. Secrets and auth
Use two separate auth paths:

Juniper auth
Remains on the worker machine with keyring and persisted session cookies.
Manual MFA is allowed when the Juniper session expires.
SharePoint auth
Use Microsoft Graph with an application identity and least-privilege site access (Sites.Selected).
Store credentials outside code:
Azure: Key Vault
AWS: Secrets Manager
Prefer certificate-based auth over a long-lived client secret.
5. Deployment shape
Azure-first production shape
Host Flask app on Azure App Service or Azure Container Apps.
Keep the Juniper refresh worker on:
a Windows VM, or
a dedicated user-managed Windows machine
Worker uploads directly to SharePoint via Graph.
Hosted app reads from SharePoint and generates reports from a local ephemeral working directory.
AWS variant
Same application design; only the infrastructure wrappers change:

Host Flask app on ECS/Fargate or EC2.
Store Microsoft credentials in Secrets Manager.
Keep the Juniper refresh worker separate on a Windows runner.
Continue to use Graph for SharePoint access.
Decision:

Do not move the Playwright+MFA worker into generic cloud hosting as the primary design. The MFA constraint makes that operationally weak.
6. Configuration additions
Add environment/config entries for:

STORAGE_BACKEND=local|sharepoint
LOCAL_CACHE_DIR
SHAREPOINT_TENANT_ID
SHAREPOINT_CLIENT_ID
SHAREPOINT_SITE_ID
SHAREPOINT_DRIVE_ID or document library ID
SHAREPOINT_SOURCE_FOLDER
certificate/secret reference
optional upload timeout / retry settings
Public Interfaces / Behavior Changes
No user-facing endpoint removals.
/api/status response should include SharePoint-backed metadata for each source file.
/api/refresh/status should include upload stage information, e.g. downloading, uploading, done, error.
Internal contract change: the app no longer assumes data/ is the canonical storage location in production.
Test Plan
Unit tests for ArtifactStore contract using local and mocked SharePoint implementations.
Unit tests for upload naming, overwrite behavior, and metadata mapping.
Integration test: refresh one export, confirm it lands in the expected SharePoint folder and appears in /api/status.
Integration test: report generation downloads required files from SharePoint into the working directory before running scripts.
Failure tests:
SharePoint upload fails after download
SharePoint file missing during report generation
expired Juniper session requiring manual MFA
partial refresh where one file succeeds and one fails
Smoke test in the chosen hosted environment with ephemeral disk only.
Assumptions and Defaults
SharePoint is the authoritative store for source files.
Manual Juniper MFA on session expiry is acceptable.
The current report scripts should remain mostly unchanged and continue to read local file paths during execution.
OneDrive folder sync is not the production integration path; it is only acceptable as a temporary local pilot if Graph upload is deferred.
Report outputs remain local/app-served for now; uploading generated PDFs to SharePoint is a separate enhancement unless explicitly added to scope.