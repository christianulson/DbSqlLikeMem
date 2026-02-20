# Providers and Compatibility

## Supported providers

- MySQL
- SQL Server
- Oracle
- PostgreSQL (Npgsql)
- SQLite
- DB2

## Key compatibility topics

- Parser differences by version/dialect
- Distinct UPSERT support (`ON DUPLICATE`, `ON CONFLICT`, `MERGE`)
- Pagination by dialect (`LIMIT/OFFSET`, `OFFSET/FETCH`, `FETCH FIRST`)
- JSON operators and functions by database
- NHibernate contract coverage shared across providers (native SQL params, mapped entity lifecycle, transaction rollback, pagination, HQL/Criteria, and null/typed parameters in both INSERT and WHERE filters, plus optimistic concurrency for versioned entities)

For full details, see the local repository documentation at `docs/providers-and-features.md`.

---

## Português

### Provedores suportados

- MySQL
- SQL Server
- Oracle
- PostgreSQL (Npgsql)
- SQLite
- DB2

### Temas importantes de compatibilidade

- Diferenças de parser por versão/dialeto
- Suporte distinto para UPSERT (`ON DUPLICATE`, `ON CONFLICT`, `MERGE`)
- Paginação por dialeto (`LIMIT/OFFSET`, `OFFSET/FETCH`, `FETCH FIRST`)
- Operadores e funções JSON por banco
- Cobertura de contrato NHibernate compartilhada entre provedores (parâmetros em SQL nativo, ciclo de vida de entidade mapeada, rollback transacional, paginação, HQL/Criteria e parâmetros nulos/tipados em INSERT e filtros WHERE, além de concorrência otimista para entidades versionadas)
