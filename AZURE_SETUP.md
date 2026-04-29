# Azure App Service Setup

This app is designed to run in Azure App Service with SharePoint as the source of truth for source files and the Juniper refresh worker kept off-platform on a Windows machine.

## Deployment shape

- Deploy this folder to Azure App Service for Linux.
- Use Microsoft Entra authentication at the App Service layer.
- Use a read-only SharePoint app registration for the web app.
- Keep `ENABLE_JUNIPER_REFRESH=false`, `ENABLE_SOURCE_UPLOADS=false`, and `ENABLE_CREDENTIAL_SETUP=false`.

## Required Azure resources

- Resource group
- App Service plan
- Linux Web App
- Key Vault
- Optional: Application Insights

## Recommended startup command

```bash
gunicorn --bind=0.0.0.0 --timeout 600 --access-logfile '-' --error-logfile '-' app:app
```

## Required app settings

Copy the values from [azure.env.example](./azure.env.example) into Azure App Service configuration and replace the placeholders.

At minimum you must set:

- `STORAGE_BACKEND=sharepoint`
- `ENABLE_JUNIPER_REFRESH=false`
- `ENABLE_SOURCE_UPLOADS=false`
- `ENABLE_CREDENTIAL_SETUP=false`
- `LOCAL_CACHE_DIR=/home/reportgenerator/cache`
- `LOCAL_WORK_DIR=/home/reportgenerator/work`
- `OUTPUT_DIR=/home/reportgenerator/outputs`
- `SHAREPOINT_TENANT_ID`
- `SHAREPOINT_CLIENT_ID`
- `SHAREPOINT_SITE_ID`
- `SHAREPOINT_DRIVE_ID`
- `SHAREPOINT_SOURCE_FOLDER`
- `SOURCE_FILE_RULES`
- `SHAREPOINT_CLIENT_SECRET` via Key Vault reference

## Authentication and authorization

- Turn on Microsoft Entra authentication in App Service.
- Require authentication for all requests.
- Use current-tenant users only.
- Set enterprise app assignment required to `Yes`.
- Assign only the approved user/group allowlist.

## SharePoint access

- Grant the Azure read-only app registration `Sites.Selected`.
- Explicitly grant that app `read` access to the dedicated SharePoint site.
- Do not give the Azure web app write access to the site.

## Runtime folders

The app should write generated files only under `/home`:

- cache: `/home/reportgenerator/cache`
- work: `/home/reportgenerator/work`
- outputs: `/home/reportgenerator/outputs`

## Post-deploy verification

1. Browse to the site and confirm Entra sign-in is required.
2. Confirm `/api/status` returns SharePoint-backed metadata.
3. Confirm refresh buttons and upload controls are hidden.
4. Generate each report and confirm PDFs are created successfully.
5. Confirm a user outside the allowed Entra group cannot access the site.
