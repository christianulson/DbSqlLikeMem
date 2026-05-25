# Benchmark Environment

Documento de suporte para reproducibilidade e comparacao de benchmarks.

## O que o runner grava hoje

- `RunId`
- `JobId`
- `Environment.Profile`
- `Environment.Os`
- `Environment.Framework`
- `Environment.Runtime`
- `Environment.Machine`
- `Environment.BenchmarkDotNetVersion`
- `Environment.TimestampUtc`

## Perfis oficiais

- `smoke`: execucao rapida para validar catalogo, scripts e publicacao basica.
- `core`: matriz essencial usada em validacao manual.
- `full`: execucao completa para comparacao manual.
- `diagnostic`: execucao com logs adicionais para investigacao.

## Versoes declaradas no catalogo

As versoes abaixo sao as declaradas hoje em `ProviderCatalog` e servem como contexto de comparacao:

- MySQL: `8.4`, imagem `mysql:8.4`
- MariaDB: `11.0`, imagem `mariadb:11.0`
- SQL Server: `2022`, imagem `mcr.microsoft.com/mssql/server:2022-CU14-ubuntu-22.04`
- SQL Azure: `170`, sem imagem local
- Oracle: `23`, imagem `gvenzl/oracle-free:23-slim-faststart`
- PostgreSQL / Npgsql: `17`, imagem `postgres:17`
- SQLite: `3`, sem container
- Firebird: `5.0`, imagem `firebirdsql/firebird:5.0.3-noble`
- DB2: `11`, imagem `icr.io/db2_community/db2:12.1.0.0`

## Regras de comparacao

- Compare apenas execucoes da mesma familia de provider e do mesmo perfil.
- Compare apenas resultados com o mesmo `BenchmarkStableId`, `ProviderId`, `Engine` e `Profile`.
- Use a versao declarada no catalogo como contexto, nao como chave principal de comparacao.
- Use `smoke` para validar o fluxo sem rodar a matriz inteira.
- Use `core` e `full` como referencia de comparacao de resultados.
- Use `diagnostic` para analise de falhas, nao como baseline principal.

## Metadados ainda ausentes

- CPU
- memoria
- Docker/Testcontainers version
- imagem e versao de cada banco
- detalhes especiais por provider quando nao estiverem no catalogo ou no log da execucao
