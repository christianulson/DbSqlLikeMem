# Publicação

## NuGet (nuget.org)

### Via GitHub Actions (recomendado)

1. Crie uma API key em https://www.nuget.org/ (Account settings → API Keys).
2. No repositório do GitHub, adicione o secret **`NUGET_API_KEY`** no Environment **`nuget-publish`**.
3. Atualize a versão em `src/Directory.Build.props` (`Version`).
4. Crie e envie uma tag de release:

```bash
git tag v0.1.0
git push origin v0.1.0
```

Workflow responsável:
- `.github/workflows/nuget-publish.yml`

Esse pipeline empacota e publica os projetos do solution no nuget.org.

O pacote `DbSqlLikeMem.VisualStudioExtension.*.nupkg` é ignorado nesse workflow, pois a extensão Visual Studio é publicada separadamente pelo fluxo de VSIX.

> Observação: o workflow usa especificamente `secrets.NUGET_API_KEY` do Environment `nuget-publish` para `dotnet nuget push`.

### Publicação manual (local)

```bash
dotnet pack src/DbSqlLikeMem.slnx -c Release -o ./artifacts
# publica somente pacotes NuGet da biblioteca (exclui o pacote da extensão VS)
for p in ./artifacts/*.nupkg; do
  case "$(basename "$p")" in
    DbSqlLikeMem.VisualStudioExtension.*.nupkg) continue ;;
  esac
  dotnet nuget push "$p" --api-key "<SUA_API_KEY>" --source "https://api.nuget.org/v3/index.json" --skip-duplicate
done
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
