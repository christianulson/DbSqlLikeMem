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
dotnet run -c Release -- --validate-catalog
```

## Validação de catálogo

- `dotnet run -c Release -- --validate-catalog` valida por reflexão se todas as suítes públicas seguem o catálogo de features e de provedores.
- Use esse modo quando quiser detectar drift de nomes, benchmarks novos sem catálogo ou catálogo desatualizado sem executar o BenchmarkDotNet completo.
- O comando não roda benchmarks; ele apenas imprime o relatório de validação e encerra com `exit code` `0` quando tudo está consistente.

## Observações importantes

- `SqlAzure` está como **mock-only**. Para comparação com banco real, use a família `SqlServer` como proxy operacional mais próximo.
- `Sqlite` usa `Microsoft.Data.Sqlite` em memória no lado real, porque SQLite normalmente não entra via container na mesma ergonomia dos demais provedores.
- `Db2` ficou com a imagem em uma constante visível no código para você poder piná-la facilmente na família que quiser comparar (11.5.x para proximidade com o mock, ou a tag mais nova do módulo do Testcontainers).

## Scripts build Reports

powershell -ExecutionPolicy Bypass -File .\Scripts\export-wiki.ps1

powershell -ExecutionPolicy Bypass -File .\Scripts\export-wiki-app-specific.single-table.ps1



subir os bancos uma vez:

./Scripts/start-benchmark-databases.ps1

rodar benchmarks externos usando os bancos já disponíveis:

./Scripts/run-benchmarks-preprovisioned.ps1 --filter "*Testcontainers*"

se quiser reduzir também o overhead de processo:

./Scripts/run-benchmarks-preprovisioned.ps1 --inprocess --filter "*Testcontainers*"


docker compose -f docker-compose.benchmarks.yml down
docker compose -f docker-compose.benchmarks.yml up -d



powershell -ExecutionPolicy Bypass -File ./Scripts/start-benchmark-databases.robust.ps1
powershell -ExecutionPolicy Bypass -File ./Scripts/run-benchmarks-preprovisioned.ps1 --inprocess 
powershell -ExecutionPolicy Bypass -File ./Scripts/run-benchmarks-preprovisioned.ps1 --inprocess  --filter "*Sqlite*"
