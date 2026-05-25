# DbSqlLikeMem.Benchmarks

Estrutura pensada para comparar **DbSqlLikeMem** contra **banco real** usando o mesmo catálogo de cenários.

## O que tem aqui

- `Benchmarks/Suites`: uma classe por banco/engine.
- `Benchmarks/Sessions`: sessões que sabem abrir conexão mock ou conexão real.
- `*.TestTools`: SQL por provedor, compartilhado entre benchmark e fidelidade.
- `benchmark-feature-map.json`: catálogo para gerar a wiki/matriz.
- `benchmark-result.schema.json`: contrato estrutural do resultado publicado e da chave estavel do benchmark.
- `Scripts/export-wiki.ps1`: converte os relatórios do BenchmarkDotNet em markdown para Wiki.
- `Scripts/export-wiki-all.ps1`: orquestra a publicacao das duas matrizes principais em uma unica execucao.

O catálogo publicado agora referencia `benchmark-result.schema.json` no topo do JSON para deixar explícito o contrato estrutural esperado pelos exportadores.
Cada execução também grava `docs/Wiki/BenchmarkResults/benchmark-run.environment.json` com `profile`, `jobId` e metadados básicos do ambiente para a etapa de exportação estruturada.
O catálogo também passou a sinalizar `FeatureStatus` no JSON publicado. Entradas marcadas como `Deprecated` continuam rastreáveis no histórico, enquanto `Removed` fica reservado para remoção formal e não deve aparecer como recurso comparável.
O historico consolidado de mudancas de feature fica em `docs/features-backlog/benchmarks/benchmark-feature-history.md` e registra a separacao inicial entre `Comparable` e `MockOnly`, alem das mudancas de ciclo de vida.
O documento de ambiente fica em `docs/features-backlog/benchmarks/benchmark-environment.md` e resume os campos gravados hoje, os perfis oficiais e as regras de comparacao.
O guia de status fica em `docs/features-backlog/benchmarks/benchmark-execution-status.md` e explica os campos estruturados, os status oficiais e a leitura publica do resultado exportado.
O guia de baseline e regressao fica em `docs/features-backlog/benchmarks/benchmark-baseline.md` e registra a politica de armazenamento, chave de comparacao e atualizacao do baseline por perfil.
O runbook manual fica em `docs/features-backlog/benchmarks/benchmark-manual-runbook.md` e organiza os fluxos smoke, core e full com os scripts existentes.
O comparador de baseline fica em `Scripts/compare-benchmark-baseline.ps1` e publica o resumo em `docs/Wiki/BenchmarkResults/benchmark-regression-summary.md`.
O guia de observabilidade fica em `docs/features-backlog/benchmarks/benchmark-observability.md` e aponta o caminho dos logs por `RunId`, os codigos de skip/falha e a ordem pratica de diagnostico.
Os exportadores da wiki agora mostram o status da feature ao lado do nome para deixar explícito quando a entrada é ativa ou depreciada.
As tabelas geradas pela wiki mantêm essa sinalização tanto na matriz comparativa quanto na versão de coluna única.

## Convenções

- Classes seguem o padrão `<Provider>_<Engine>_Benchmarks`.
- Métodos normalmente seguem o nome da feature (`ConnectionOpen`, `CreateSchema`, `InsertSingle`...), mas alias e wrappers continuam válidos quando o catálogo ou o registry os expõem.
- O script da wiki usa essas convenções junto com o catálogo para montar a matriz automaticamente.

## Execução sugerida

```powershell
dotnet run -c Release --filter *MariaDb_DbSqlLikeMem_Benchmarks*
dotnet run -c Release --filter *MariaDb_Testcontainers_Benchmarks*
dotnet run -c Release --filter *Firebird_DbSqlLikeMem_Benchmarks*
dotnet run -c Release --filter *Firebird_Testcontainers_Benchmarks*
dotnet run -c Release --filter *MySql_DbSqlLikeMem_Benchmarks*
dotnet run -c Release --filter *MySql_Testcontainers_Benchmarks*
dotnet run -c Release --filter *Npgsql_DbSqlLikeMem_Benchmarks*
dotnet run -c Release --filter *Npgsql_Testcontainers_Benchmarks*
dotnet run -c Release --filter *SqlServer_DbSqlLikeMem_Benchmarks*
dotnet run -c Release --filter *SqlServer_Testcontainers_Benchmarks*
dotnet run -c Release --filter *Sqlite_DbSqlLikeMem_Benchmarks*
dotnet run -c Release --filter *Sqlite_Native_Benchmarks*
pwsh ./Scripts/run-core-matrix.ps1
pwsh ./Scripts/start-benchmark-databases.ps1
pwsh ./Scripts/start-benchmark-databases.robust.ps1
pwsh ./Scripts/run-benchmarks-preprovisioned.ps1 --filter "*Testcontainers*"
dotnet run -c Release -- --validate-catalog
```

## Validação de catálogo

- `dotnet run -c Release -- --validate-catalog` valida por reflexão se todas as suítes públicas seguem o catálogo de features e de provedores.
- Use esse modo quando quiser detectar drift de nomes, benchmarks novos sem catálogo ou catálogo desatualizado sem executar o BenchmarkDotNet completo.
- O comando não roda benchmarks; ele apenas imprime o relatório de validação e encerra com `exit code` `0` quando tudo está consistente.

## Observações importantes

- `SqlAzure` está como **mock-only**. Para comparação com banco real, use a família `SqlServer` como proxy operacional mais próximo.
- `MariaDB` usa o mesmo fluxo operacional do MySQL, mas com provider, dialeto e sessão próprios.
- `ReturningInsert`, `ReturningUpdate` e `MergeBasic` continuam existindo como entradas de benchmark, mas agora ficam marcados como **não comparáveis** no catálogo porque o fluxo real muda entre provedores ou vira alias de outro caminho.
- `Sqlite` usa `Microsoft.Data.Sqlite` em memória no lado real, porque SQLite normalmente não entra via container na mesma ergonomia dos demais provedores.
- `Db2` ficou com a imagem em uma constante visível no código para você poder piná-la facilmente na família que quiser comparar (11.5.x para proximidade com o mock, ou a tag mais nova do módulo do Testcontainers).
- `Firebird` já tem uma suíte em memória dedicada e agora também uma suíte externa real em Testcontainers; ambas cobrem o slice `EXECUTE BLOCK` com `WHEN SQLSTATE`.
- O gerador da wiki já lê o catálogo de features; ele não precisa de lógica especial para MariaDB, apenas dos relatórios publicados.
- Os runners `run-core-matrix.ps1`, `run-benchmarks-preprovisioned.ps1`, `start-benchmark-databases.ps1` e `start-benchmark-databases.robust.ps1` já conhecem MariaDB e Firebird.

## Scripts build Reports

powershell -ExecutionPolicy Bypass -File .\Scripts\export-wiki.ps1
powershell -ExecutionPolicy Bypass -File .\Scripts\export-wiki-app-specific.ps1
powershell -ExecutionPolicy Bypass -File .\Scripts\export-wiki-all.ps1
powershell -ExecutionPolicy Bypass -File .\Scripts\export-wiki-all.ps1 -IncludeLegacySingleTable

`export-wiki-app-specific.single-table.ps1` ficou como atalho legado opcional e nao faz parte do fluxo normal de publicacao.



subir os bancos uma vez:

./Scripts/start-benchmark-databases.ps1

rodar benchmarks externos usando os bancos já disponíveis:

./Scripts/run-benchmarks-preprovisioned.ps1 --filter "*Testcontainers*"

se quiser reduzir também o overhead de processo:

./Scripts/run-benchmarks-preprovisioned.ps1 --inprocess --filter "*DbSqlLikeMem*"

`inprocess` fica reservado para filtros curtos de `DbSqlLikeMem` ou `Sqlite`; para `Testcontainers`, use o runner sem essa flag.
Quando você roda o lote completo com `--inprocess`, o runner separa automaticamente os benchmarks rápidos (`DbSqlLikeMem` e `Sqlite`) da passagem `Testcontainers`, para evitar o erro de toolchain e manter a execução rápida onde faz sentido.

Perfis aceitos pelo runner:

- `smoke`
- `core`
- `full`
- `diagnostic`

Exemplo:

```powershell
dotnet run -c Release --profile smoke --filter *Sqlite_DbSqlLikeMem_Benchmarks*
```


docker compose -f docker-compose.benchmarks.yml down
docker compose -f docker-compose.benchmarks.yml up -d



powershell -ExecutionPolicy Bypass -File ./Scripts/start-benchmark-databases.robust.ps1
powershell -ExecutionPolicy Bypass -File ./Scripts/run-benchmarks-preprovisioned.ps1 --inprocess --filter "*DbSqlLikeMem*"
powershell -ExecutionPolicy Bypass -File ./Scripts/run-benchmarks-preprovisioned.ps1 --inprocess --filter "*Sqlite*"
powershell -ExecutionPolicy Bypass -File ./Scripts/run-benchmarks-preprovisioned.ps1 --inprocess --filter "*Firebird*"
