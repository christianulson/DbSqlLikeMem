# Publicação

## NuGet (nuget.org)

### Assinatura de pacote e "0 certificates"

No nuget.org, o campo **Signing Owner ... (0 certificates)** indica que o proprietário ainda não configurou certificado para **Author signing**. Isso não bloqueia publicação, mas melhora confiança e rastreabilidade para consumidores quando você assina.

Formas de melhorar:

1. **Ativar assinatura do pacote no build (`dotnet nuget sign`)** usando um certificado de code signing (PFX) emitido por uma CA confiável.
2. **Usar um provedor de assinatura em nuvem** (Azure Key Vault, DigiCert KeyLocker, etc.) para evitar armazenar PFX localmente.
3. **Publicar com pipeline que assina antes do `nuget push`** e validar assinatura com `nuget verify -signatures`.

Exemplo local (Windows) após `dotnet pack`:

```bash
dotnet nuget sign ./artifacts/DbSqlLikeMem.*.nupkg \
  --certificate-path "./certs/codesign.pfx" \
  --certificate-password "<PASSWORD>" \
  --timestamper "http://timestamp.digicert.com"

nuget verify -Signatures ./artifacts/DbSqlLikeMem.*.nupkg
```

> Dica: use timestamp RFC3161 para a assinatura continuar válida mesmo após expiração do certificado.

Para projetos open source, outra alternativa é habilitar **proveniência de repositório** (SourceLink + pipeline protegida + políticas de release), mesmo quando author-signing ainda não estiver disponível.

Implementação atual deste repositório:

- `src/Directory.Build.props` publica metadados de repositório no pacote, habilita build determinístico em CI e gera `snupkg` para depuração.
- `.github/workflows/nuget-publish.yml` valida metadados de repositório nos `.nupkg` antes do `push`.

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
