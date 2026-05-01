# Benchmark Observability

Guia para localizar, interpretar e diagnosticar falhas de benchmark.

## Caminho dos logs

- Os logs ficam em `src/benchmark/DbSqlLikeMem.Benchmarks/Logs/<RunId>/`.
- O `RunId` separa execucoes diferentes e evita mistura de logs entre rodadas.

## Campos obrigatorios de log

- `RunId`
- `ProviderId`
- `Engine`
- `BenchmarkStableId`
- `BenchmarkFeatureId`
- `SuiteName`
- `MethodName`
- `Status`
- `TimestampUtc`

## Códigos de falha e skip

- `NA-IOE`: falha de operacao ou contrato inesperado.
- `NA-NSE`: feature nao suportada no provider.
- `NA-DB2E`: excecao nativa do DB2.
- `NA-SqlE`: excecao nativa do SQL Server.
- `NA-MSE`: excecao nativa do MySQL.
- `NA-NE`: excecao nativa do Npgsql.
- `NA-OE`: excecao nativa do Oracle.
- `NA`: falha generica nao classificada.

## Onde olhar quando falhar

- Primeiro, verificar o arquivo em `Logs/<RunId>/`.
- Depois, conferir o manifest de ambiente em `docs/Wiki/BenchmarkResults/benchmark-run.environment.json`.
- Por fim, revisar o resumo publicado em `docs/Wiki/BenchmarkResults/benchmark-regression-summary.md` quando houver comparacao com baseline.

## Regras de leitura

- `NotSupported` e skip controlado, nao falha inesperada.
- `Failed` indica erro real de execucao ou infraestrutura.
- `Skipped` indica exclusao proposital por perfil ou filtro.
