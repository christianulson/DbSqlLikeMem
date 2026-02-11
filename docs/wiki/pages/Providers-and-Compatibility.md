# Providers and Compatibility

## Provedores suportados

- MySQL
- SQL Server
- Oracle
- PostgreSQL (Npgsql)
- SQLite
- DB2

## Temas importantes de compatibilidade

- Diferenças de parser por versão/dialeto
- Suporte distinto para UPSERT (`ON DUPLICATE`, `ON CONFLICT`, `MERGE`)
- Paginação por dialeto (`LIMIT/OFFSET`, `OFFSET/FETCH`, `FETCH FIRST`)
- Operadores e funções JSON por banco

Para detalhes completos, consulte a documentação local do repositório em `docs/providers-and-features.md`.
