# Publishing

## NuGet

- Workflow: `.github/workflows/nuget-publish.yml`
- Secret: `NUGET_API_KEY`
- Optional environment secret support: job runs in Environment `nuget-publish` by default (or `vars.NUGET_PUBLISH_ENVIRONMENT` when set)
- Tag: `v*`

## VSIX (Visual Studio)

- Workflow: `.github/workflows/vsix-publish.yml`
- Secret: `VS_MARKETPLACE_TOKEN`
- Tag: `vsix-v*`

## VS Code Extension

- Workflow: `.github/workflows/vscode-extension-publish.yml`
- Secret: `VSCE_PAT`
- Tag: `vscode-v*`

---

## Português

### NuGet

- Workflow: `.github/workflows/nuget-publish.yml`
- Secret: `NUGET_API_KEY`
- Suporte opcional a segredo de Environment: o job roda no Environment `nuget-publish` por padrão (ou `vars.NUGET_PUBLISH_ENVIRONMENT` quando definido)
- Tag: `v*`

### VSIX (Visual Studio)

- Workflow: `.github/workflows/vsix-publish.yml`
- Secret: `VS_MARKETPLACE_TOKEN`
- Tag: `vsix-v*`

### Extensão VS Code

- Workflow: `.github/workflows/vscode-extension-publish.yml`
- Secret: `VSCE_PAT`
- Tag: `vscode-v*`
