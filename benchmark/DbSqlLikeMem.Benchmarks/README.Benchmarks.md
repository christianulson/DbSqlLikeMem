# DbSqlLikeMem.Benchmarks

Estrutura pensada para comparar **DbSqlLikeMem** contra **banco real** usando o mesmo catálogo de cenários.

## O que tem aqui

- `Benchmarks/Suites`: uma classe por banco/engine.
- `Benchmarks/Sessions`: sessões que sabem abrir conexão mock ou conexão real.
- `Benchmarks/Dialects`: SQL por provedor.
- `benchmark-feature-map.json`: catálogo para gerar a wiki/matriz.
- `Scripts/export-wiki.ps1`: converte os relatórios do BenchmarkDotNet em markdown para Wiki.

## Convenções

- Classes seguem o padrão `<Provider>_<Engine>_Benchmarks`.
- Métodos seguem exatamente o nome da feature (`ConnectionOpen`, `CreateSchema`, `InsertSingle`...).
- O script da wiki usa essas duas convenções para montar a matriz automaticamente.

## Execução sugerida

```powershell
dotnet run -c Release --filter *MySql_DbSqlLikeMem_Benchmarks*
dotnet run -c Release --filter *MySql_Testcontainers_Benchmarks*
pwsh ./Scripts/export-wiki.ps1 -ArtifactsDir ./BenchmarkDotNet.Artifacts/results -OutFile ./wiki/performance-matrix.md
```

## Observações importantes

- `SqlAzure` está como **mock-only**. Para comparação com banco real, use a família `SqlServer` como proxy operacional mais próximo.
- `Sqlite` usa `Microsoft.Data.Sqlite` em memória no lado real, porque SQLite normalmente não entra via container na mesma ergonomia dos demais provedores.
- `Db2` ficou com a imagem em uma constante visível no código para você poder piná-la facilmente na família que quiser comparar (11.5.x para proximidade com o mock, ou a tag mais nova do módulo do Testcontainers).
