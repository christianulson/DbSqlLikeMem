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
- `.github/workflows/nuget-publish.yml` valida metadados dos `.nupkg` via `scripts/check_nuget_package_metadata.py` antes do `push`, usando `src/Directory.Build.props` como fonte de verdade para comparar `version`, `authors`, `repository`, `projectUrl`, `readme`, `tags`, `releaseNotes` e licença.
- A versão do release NuGet sai de `src/Directory.Build.props`; a tag operacional deve seguir `v<versao>` e permanecer em SemVer compatível com esse arquivo.

### Via GitHub Actions (recomendado)

1. Crie uma API key em <https://www.nuget.org/> (Account settings → API Keys).
2. No repositório do GitHub, adicione o secret **`NUGET_API_KEY`** no Environment **`nuget-publish`**.
3. Atualize a versão em `src/Directory.Build.props` (`Version`).
4. Crie e envie uma tag de release:

```bash
git tag v0.1.0
git push origin v0.1.0
```

### Como decidir a próxima versão (SemVer)

Use a regra abaixo antes de publicar no NuGet:

- **PATCH** (`1.4.x`): apenas correções de bug, melhorias internas e ajustes de testes/documentação sem ampliar comportamento público.
- **MINOR** (`1.x.0`): novas features compatíveis (novas capacidades SQL, novos cenários suportados, novas integrações) sem quebrar APIs/contratos existentes.
- **MAJOR** (`x.0.0`): qualquer breaking change em API pública, comportamento padrão incompatível ou remoção/alteração de contrato esperado.

Checklist rápido para confirmar **breaking change**:

1. Houve remoção/renomeação de tipos, métodos, propriedades ou parâmetros públicos?
2. Algum comportamento padrão passou a lançar exceção onde antes era suportado?
3. Algum fluxo compatível de versão anterior exige mudança obrigatória no código consumidor?

Se todas as respostas forem **não**, prefira `PATCH` (sem feature nova) ou `MINOR` (com feature nova).

`python3 scripts/check_release_readiness.py` também passa a validar o formato SemVer das versões configuradas no núcleo (`src/Directory.Build.props`) e nas extensões (`package.json` do VS Code e `source.extension.vsixmanifest` do VSIX), sem impor que todos compartilhem o mesmo número.
O mesmo auditor agora cobre contratos mínimos de publicação das extensões: scripts/arquivos essenciais do pacote VS Code, activation events apontando para comandos/views existentes e presença do `overview`/tags/categorias no manifesto de publicação VSIX.
No caso da VSIX, a auditoria também verifica alinhamento entre `MinimumVisualStudioVersion` do projeto e o range suportado no `source.extension.vsixmanifest`, evitando drift de compatibilidade declarada.
Para a extensão VS Code, a mesma trilha também valida placeholders `%...%` do `package.json` contra `package.nls*.json` e a presença da pasta `l10n`.

### Checklist de release

Antes de publicar:

1. Atualize a versão em `src/Directory.Build.props`.
2. Revise `CHANGELOG.md` com impacto por provider/dialeto e limitações ainda abertas.
3. Confirme que `docs/features-backlog/index.md` reflete os percentuais e incrementos entregues.
4. Registre o andamento operacional em `docs/features-backlog/status-operational.md` quando houver contexto de sprint ainda relevante.
5. Refaça os snapshots cross-dialect aplicáveis (`smoke`, `aggregation`, `parser`) via `scripts/refresh_cross_dialect_snapshots.sh`.
6. Valide que workflows de publicação/CI e documentação apontam para os artefatos corretos.
7. Verifique se alguma limitação conhecida precisa ficar explícita na release.
8. Rode `python3 scripts/check_release_readiness.py` para auditar documentação, workflows, snapshots e metadados de publicação antes de empacotar.
9. Depois do `pack`, rode `python3 scripts/check_nuget_package_metadata.py --artifacts-dir ./artifacts` para auditar os `.nupkg` que serão publicados.
10. Confirme que `CHANGELOG.md` continua com `## [Unreleased]`, subseções de impacto e `Known limitations still open` antes de criar qualquer tag de release.

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
- O workflow executa `python scripts/check_release_readiness.py` antes do build e usa `--strict-marketplace-placeholders` ao publicar, bloqueando publish com `publisher` placeholder.
- A versão operacional da VSIX sai de `src/DbSqlLikeMem.VisualStudioExtension/source.extension.vsixmanifest`; a tag automática deve seguir `vsix-v<versao-da-vsix>`.

### Pré-requisitos

1. Criar PAT para publicação no Visual Studio Marketplace.
2. Salvar no GitHub como secret `VS_MARKETPLACE_TOKEN`.
3. Confirmar os campos operacionais finais em `eng/visualstudio/PublishManifest.json`, principalmente `publisher`.
4. Garantir que exista um projeto VSIX (workflow usa `src/DbSqlLikeMem.VisualStudioExtension/DbSqlLikeMem.VisualStudioExtension.csproj`).

> O campo `repo` do manifesto já aponta para o repositório oficial; o `publisher` ainda deve ser confirmado antes da publicação final.

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
- O workflow executa `python3 scripts/check_release_readiness.py` antes de instalar dependências/empacotar.
- A versão operacional da extensão sai de `src/DbSqlLikeMem.VsCodeExtension/package.json`; a tag automática deve seguir `vscode-v<versao-da-extensao>`.

As URLs de repositório/bugs/homepage do `package.json` já foram alinhadas ao repositório oficial; mantenha a revisão do `publisher` e use `python3 scripts/check_release_readiness.py` como auditoria final de readiness.

## Mapa de versões e tags

- NuGet: versão em `src/Directory.Build.props` e tag `v<versao>`.
- VSIX: versão em `src/DbSqlLikeMem.VisualStudioExtension/source.extension.vsixmanifest` e tag `vsix-v<versao-da-vsix>`.
- VS Code: versão em `src/DbSqlLikeMem.VsCodeExtension/package.json` e tag `vscode-v<versao-da-extensao>`.
- Os três fluxos exigem SemVer válido, mas não precisam compartilhar exatamente o mesmo número de versão.

### Publicação manual local

```bash
cd src/DbSqlLikeMem.VsCodeExtension
npm install
npm run compile
npm run package
# ou publicação direta
npm run publish
```

> Antes de publicar, confirme o `publisher` final e rode `python3 scripts/check_release_readiness.py`.

## Links relacionados

- [Começando rápido](getting-started.md)
- [Provedores e compatibilidade](old/providers-and-features.md)
- [Wiki do GitHub](Wiki/Home.md)
