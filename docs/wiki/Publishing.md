# Publishing

## NuGet

- Workflow: `.github/workflows/nuget-publish.yml`
- Secret: `NUGET_API_KEY`
- Optional environment secret support: job runs in Environment `nuget-publish` by default (or `vars.NUGET_PUBLISH_ENVIRONMENT` when set)
- Tag: `v*`
- Version source: `src/Directory.Build.props`
- Pre-pack audit: `python3 scripts/check_release_readiness.py`
- Post-pack `.nupkg` audit: `python3 scripts/check_nuget_package_metadata.py --artifacts-dir ./artifacts`
- Version governance: `check_release_readiness.py` validates SemVer format for the core and extension artifacts.
- NuGet artifact governance: `check_nuget_package_metadata.py` also validates the published `.nuspec` version against `src/Directory.Build.props` and the `.nupkg` filename suffix.
- Release notes source: `CHANGELOG.md`
- Keep `Known limitations still open` explicit in `CHANGELOG.md` before tagging `v*`.
- Extension governance: `check_release_readiness.py` also validates essential VS Code assets/scripts and basic VSIX publish-manifest fields.
- VSIX compatibility governance: the same audit checks `MinimumVisualStudioVersion` against the supported Visual Studio range in the manifest.
- VS Code localization governance: the same audit validates `%...%` placeholders from `package.json` against `package.nls*.json` and checks the `l10n` folder.

## VSIX (Visual Studio)

- Workflow: `.github/workflows/vsix-publish.yml`
- Secret: `VS_MARKETPLACE_TOKEN`
- Tag: `vsix-v*`
- Version source: `src/DbSqlLikeMem.VisualStudioExtension/source.extension.vsixmanifest`
- Baseline audit before build: `python scripts/check_release_readiness.py`
- Strict publish audit: `python scripts/check_release_readiness.py --strict-marketplace-placeholders`
- Review release notes and known limitations in `CHANGELOG.md` before pushing `vsix-v*`.

## VS Code Extension

- Workflow: `.github/workflows/vscode-extension-publish.yml`
- Secret: `VSCE_PAT`
- Tag: `vscode-v*`
- Version source: `src/DbSqlLikeMem.VsCodeExtension/package.json`
- Baseline audit before packaging: `python3 scripts/check_release_readiness.py`
- Review release notes and known limitations in `CHANGELOG.md` before pushing `vscode-v*`.

---

## Português

### NuGet

- Workflow: `.github/workflows/nuget-publish.yml`
- Secret: `NUGET_API_KEY`
- Suporte opcional a segredo de Environment: o job roda no Environment `nuget-publish` por padrão (ou `vars.NUGET_PUBLISH_ENVIRONMENT` quando definido)
- Tag: `v*`
- Fonte da versao: `src/Directory.Build.props`
- Auditoria antes do pack: `python3 scripts/check_release_readiness.py`
- Auditoria dos `.nupkg` apos o pack: `python3 scripts/check_nuget_package_metadata.py --artifacts-dir ./artifacts`
- Governanca de versao: `check_release_readiness.py` valida o formato SemVer do core e dos artefatos de extensao.
- Governanca do artefato NuGet: `check_nuget_package_metadata.py` tambem valida a versao publicada no `.nuspec` contra `src/Directory.Build.props` e o sufixo do arquivo `.nupkg`.
- Fonte das release notes: `CHANGELOG.md`
- Mantenha `Known limitations still open` explicito em `CHANGELOG.md` antes de criar a tag `v*`.
- Governanca de extensao: `check_release_readiness.py` tambem valida assets/scripts essenciais do VS Code e campos basicos do manifesto de publicacao VSIX.
- Governanca de compatibilidade VSIX: a mesma auditoria confere `MinimumVisualStudioVersion` contra o range suportado no manifesto.
- Governanca de localizacao VS Code: a mesma auditoria valida placeholders `%...%` do `package.json` contra `package.nls*.json` e confere a pasta `l10n`.

### VSIX (Visual Studio)

- Workflow: `.github/workflows/vsix-publish.yml`
- Secret: `VS_MARKETPLACE_TOKEN`
- Tag: `vsix-v*`
- Fonte da versao: `src/DbSqlLikeMem.VisualStudioExtension/source.extension.vsixmanifest`
- Auditoria base antes do build: `python scripts/check_release_readiness.py`
- Auditoria estrita no publish: `python scripts/check_release_readiness.py --strict-marketplace-placeholders`
- Revise release notes e limitacoes abertas em `CHANGELOG.md` antes de publicar com `vsix-v*`.

### Extensão VS Code

- Workflow: `.github/workflows/vscode-extension-publish.yml`
- Secret: `VSCE_PAT`
- Tag: `vscode-v*`
- Fonte da versao: `src/DbSqlLikeMem.VsCodeExtension/package.json`
- Auditoria base antes do empacotamento: `python3 scripts/check_release_readiness.py`
- Revise release notes e limitacoes abertas em `CHANGELOG.md` antes de publicar com `vscode-v*`.
