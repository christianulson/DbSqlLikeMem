# Auditoria de compatibilidade multi-target (estática)
> Status: artefato histórico gerado antes do realinhamento central de targets. Para TFMs e versão vigentes, use `src/Directory.Build.props`, `README.md` e `docs/getting-started.md` como fonte de verdade.
> Escopo: varredura de **92 projetos .csproj** + `Directory.Build.props`. Não foram encontrados `global.json`, `NuGet.config` ou `Directory.Build.targets` no repositório.
## 1) Inventário completo de projetos e metadados MSBuild
- Inventário completo de TFMs/RIDs: `docs/info/project_tfm_inventory.csv`.
- PackageReferences completos (incluindo Condition/Assets): `docs/info/package_references.csv`.
- ProjectReferences completos: `docs/info/project_references.csv`.
- FrameworkReferences: `docs/info/framework_references.csv` (vazio).
- Analyzers/geradores: `docs/info/analyzers_generators.csv`.
- Imports MSBuild explícitos: `docs/info/imports.csv` (vazio).
- Itens condicionais por TFM: `docs/info/conditional_items.csv`.

### Resumo rápido
- Total de projetos: **92**.
- TFMs encontrados: `net10.0` (61), `net472` (2), `net48` (50), `net6.0` (61), `net8.0` (69), `netstandard2.0` (1).
- PackageReferences declarados: **373**.
- ProjectReferences declarados: **143**.

## 2) Tabela solicitada (visão consolidada)
Como a matriz completa é muito grande, a tabela completa está em CSV. Abaixo, resumo dos projetos-chave packáveis:

| Projeto | TFMs | RIDs | #PackageRefs | #ProjectRefs |
|---|---|---:|---:|---:|
| `src/DbSqlLikeMem.Db2/DbSqlLikeMem.Db2.csproj` | `net10.0;net6.0;net8.0` | `- ` | 2 | 1 |
| `src/DbSqlLikeMem.MySql/DbSqlLikeMem.MySql.csproj` | `` | `- ` | 1 | 1 |
| `src/DbSqlLikeMem.Npgsql/DbSqlLikeMem.Npgsql.csproj` | `` | `- ` | 4 | 1 |
| `src/DbSqlLikeMem.Oracle/DbSqlLikeMem.Oracle.csproj` | `` | `- ` | 2 | 1 |
| `src/DbSqlLikeMem.SqlServer/DbSqlLikeMem.SqlServer.csproj` | `` | `- ` | 3 | 1 |
| `src/DbSqlLikeMem.Sqlite/DbSqlLikeMem.Sqlite.csproj` | `` | `- ` | 3 | 1 |
| `src/DbSqlLikeMem/DbSqlLikeMem.csproj` | `` | `- ` | 2 | 0 |

## 3) Grafo de dependências (diretas/transitivas)
- **Diretas**: disponíveis em `docs/info/package_references.csv` e `docs/info/project_references.csv`.
- **Transitivas resolvidas**: **não foi possível** obter neste ambiente porque o `dotnet` não está instalado (`dotnet: command not found`).
- Para obter o grafo completo com versões resolvidas e TFM por pacote, rode localmente:
  - `dotnet restore -v minimal`
  - `dotnet list <sln ou csproj> package --include-transitive`
  - `dotnet msbuild <csproj> /t:Restore /v:minimal /bl:restore.binlog`

### Pacotes com maior probabilidade de elevar baseline mínimo (heurística estática)
- `Microsoft.EntityFrameworkCore.*` (linhas condicionadas por TFM moderno em vários projetos).
- `xunit.v3` (usado em TFMs `net8.0/net10.0`).
- `Microsoft.CodeAnalysis.*` 5.x no `net8/net10` e 4.9.2 no `net6`.
- `System.Text.Json` 10.0.3 em TFMs `net48/net8/net10` via `Directory.Build.props`.

## 4) APIs/linguagem com risco em `net462` / `netstandard2.0`
- Uso extensivo de `record`, `record struct`, `required` e `init` no projeto base `DbSqlLikeMem`.
- Já existem polyfills em `Compatibility/FrameworkPolyfills.cs`, mas **condicionados a `NET48/NETSTANDARD2_1/NET6_0`**, não `NET462/NETSTANDARD2_0`.
- Uso de `System.Text.Json` aparece principalmente em testes; no núcleo, dependência vem por PackageReference global.
- Uso relevante de `Regex` em parser/executor (compatível, mas hot path).

### Classificação de risco
- **Nem compila** (alto risco): `required`/`init` sem polyfill adequado para `net462`/`netstandard2.0`.
- **Compila mas pode falhar em runtime** (médio): diferenças de TLS/handlers/sockets e comportamento de bibliotecas externas por TFM legado.

## 5) NuGet pack output
- Não foi possível validar `lib/<tfm>/`, `build/`, `buildTransitive/`, `analyzers/`, `contentFiles/` do `.nupkg` sem `dotnet pack`.
- Comandos para validar:
  - `dotnet pack <csproj> -c Release -v minimal /bl:pack.binlog`
  - listar conteúdo do `.nupkg` (`unzip -l <arquivo>.nupkg`)

## 6) Performance por TFM
- Hot paths prováveis: parser SQL (`Parser/*`), executor AST (`Query/AstQueryExecutorBase.cs`), regex intensivo e materialização LINQ.
- Otimizações sugeridas por TFM:
  - `#if NET8_0_OR_GREATER`: usar APIs modernas de parsing/coleções e spans quando viável.
  - manter fallback para `net462/netstandard2.0` com implementações sem `Span<T>`/source generators.

## 7) Entregáveis
### A) Recomendação de TFMs
- Para **mínimo risco** + legado + moderno: `net462;netstandard2.0;net8.0` no pacote core (sua hipótese é coerente).
- Para projetos auxiliares/testes, manter TFMs atuais ou simplificar para `net8.0` quando não houver necessidade de legado.
- Remover `net10.0` do pacote público reduz risco de adoção precoce.

### B) Mudanças sugeridas no csproj (copiar/colar)
```xml
<!-- Directory.Build.props -->
<PropertyGroup>
  <TargetFrameworks>net462;netstandard2.0;net8.0</TargetFrameworks>
</PropertyGroup>

<ItemGroup Condition="'$(TargetFramework)' == 'net462' Or '$(TargetFramework)' == 'netstandard2.0'">
  <PackageReference Include="System.Memory" Version="4.5.5" />
  <PackageReference Include="System.Buffers" Version="4.5.1" />
  <PackageReference Include="System.Runtime.CompilerServices.Unsafe" Version="6.1.2" />
  <PackageReference Include="System.Text.Json" Version="8.0.5" />
</ItemGroup>
```
```xml
<!-- FrameworkPolyfills.cs (ajustar condições) -->
#if NET462 || NET48 || NETSTANDARD2_0 || NETSTANDARD2_1
// IsExternalInit
#endif

#if NET462 || NET48 || NETSTANDARD2_0 || NETSTANDARD2_1 || NET6_0
// RequiredMemberAttribute, CompilerFeatureRequiredAttribute, SetsRequiredMembersAttribute
#endif
```

### C) Top 10 riscos e mitigação
1. Polyfills incompletos para net462/ns2.0 -> expandir condicionais e adicionar testes de compilação por TFM.
2. Versões de pacotes muito novas em TFM legado -> pin por condição de TFM.
3. `System.Text.Json` 10.x em legado -> fixar 8.x/7.x para net462/ns2.0 se necessário.
4. Dependências transativas não auditadas -> gerar `dotnet list package --include-transitive`.
5. Quebra no pack por assets indevidos -> inspecionar `.nupkg` por TFM.
6. Projetos de teste misturam xUnit v2/v3 -> separar claramente por TFM.
7. `LangVersion=latest` global -> usar versão fixa para previsibilidade em legado.
8. `net10.0` em pacote público -> risco de ecossistema/CI; considerar remover temporariamente.
9. Regressão de runtime em APIs de IO/regex -> bateria de testes funcionais em net462 + net8.
10. CI sem matriz por TFM -> adicionar matrix build/test/pack por TFM.

### D) Polyfills/fallbacks
- Manter `FrameworkPolyfills.cs` e incluir símbolos para `NET462` e `NETSTANDARD2_0`.
- Adicionar pacotes de compat (`System.Memory`, `System.Buffers`, `Unsafe`) só para TFMs legados.

### E) Plano de testes (CI matrix)
```yaml
strategy:
  matrix:
    tfm: [net462, netstandard2.0, net8.0]
steps:
  - run: dotnet restore -v minimal
  - run: dotnet build src/DbSqlLikeMem/DbSqlLikeMem.csproj -c Release -f ${{ matrix.tfm }} -v minimal
  - run: dotnet test src/DbSqlLikeMem.TestTools/DbSqlLikeMem.TestTools.csproj -c Release -f ${{ matrix.tfm }} -v minimal
  - run: dotnet pack src/DbSqlLikeMem/DbSqlLikeMem.csproj -c Release -f ${{ matrix.tfm }} -v minimal
```

## Dados faltantes (objetivos)
Para fechar 100% os itens 3 e 5, faltam estes outputs (copiar/colar):
- `dotnet --info`
- `dotnet --list-sdks`
- `dotnet --list-runtimes`
- `dotnet restore -v minimal`
- `dotnet list <sln ou csproj> package --include-transitive`
- `dotnet msbuild <csproj> /t:Restore /v:minimal /bl:restore.binlog`
- `dotnet build <csproj> -c Release -v minimal /bl:build.binlog`
- `dotnet pack <csproj> -c Release -v minimal /bl:pack.binlog`
- `lista de arquivos de bin/Release/<tfm>/ e do .nupkg`
