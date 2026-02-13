# Publicação

## NuGet (nuget.org)

### Via GitHub Actions (recomendado)

Este repositório está configurado para **Trusted Publishing** no nuget.org (OIDC), sem necessidade de API key no workflow.

1. No nuget.org, configure o Trusted Publisher para este repositório/workflow.
2. Atualize a versão em `src/Directory.Build.props` (`Version`).
3. Crie e envie uma tag de release:

```bash
git tag v0.1.0
git push origin v0.1.0
```

Workflow responsável:
- `.github/workflows/nuget-publish.yml`

Esse pipeline empacota e publica os projetos do solution no nuget.org.

> Observação: para publicação local/manual (fora do GitHub Actions), API key continua sendo necessária.

### Publicação manual (local)

```bash
dotnet pack src/DbSqlLikeMem.slnx -c Release -o ./artifacts
dotnet nuget push "./artifacts/*.nupkg" --api-key "<SUA_API_KEY>" --source "https://api.nuget.org/v3/index.json"
```

---

## Visual Studio Extension (VSIX)

Workflow preparado:
- `.github/workflows/vsix-publish.yml`

### Pré-requisitos

1. Criar PAT para publicação no Visual Studio Marketplace.
2. Salvar no GitHub como secret `VS_MARKETPLACE_TOKEN`.
3. Ajustar placeholders em `eng/visualstudio/PublishManifest.json` (`publisher`, `repo`, `identity.internalName`, etc.).
4. Garantir que exista um projeto VSIX (workflow usa `src/DbSqlLikeMem.VisualStudioExtension/DbSqlLikeMem.VisualStudioExtension.csproj`).

### Como publicar

- **Manual (recomendado para validação):**
  - Execute o workflow **Publish Visual Studio Extension (VSIX)** via `workflow_dispatch`.
  - Defina `publish = true` para publicar.
- **Automático por tag:**
  - Use tags no formato `vsix-v*` (ex.: `vsix-v1.0.0`).

---

## VS Code Extension (Marketplace)

A extensão em `src/DbSqlLikeMem.VsCodeExtension` está preparada para empacotamento/publicação.

- Workflow: `.github/workflows/vscode-extension-publish.yml`
- Secret necessário: `VSCE_PAT`
- Tag para publicação automática: `vscode-v*`

### Publicação manual local

```bash
cd src/DbSqlLikeMem.VsCodeExtension
npm install
npm run compile
npm run package
# ou publicação direta
npm run publish
```

> Antes de publicar, ajuste no `package.json` os placeholders de URL (`repository`, `bugs`, `homepage`) e confirme o `publisher` final.

## Links relacionados

- [Começando rápido](getting-started.md)
- [Provedores e compatibilidade](providers-and-features.md)
- [Wiki do GitHub](wiki/README.md)
