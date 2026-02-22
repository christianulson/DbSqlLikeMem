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
- NHibernate contract coverage shared across providers (native SQL params, mapped entity lifecycle, transaction rollback, pagination, HQL/Criteria, null/typed parameters in both INSERT and WHERE filters, session lifecycle with Evict/Clear/Merge (including detached changes after Evict not auto-persisting on Flush, full detach verification on Clear, and state reload via Refresh, and association reload after relationship mutation), realistic relationship updates including FK changes by navigation, child removal from collections, and child moves between groups (including SQL FK-distribution checks and rollback-preserved distribution), optimistic concurrency scenarios including stale detection/version increments/refresh between sessions (including version growth per successful commit and stale-read reconciliation via Refresh), plus advanced querying with HQL projection (including alias maps via AliasToEntityMap, relationship aggregate projections, and scalar counts by relationship filter), relationship-based ordering (including deterministic paged windows), and Criteria with multiple restrictions (including relationship aliases and scalar counts by relationship alias))

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
- Cobertura de contrato NHibernate compartilhada entre provedores (parâmetros em SQL nativo, ciclo de vida de entidade mapeada, rollback transacional, paginação, HQL/Criteria e parâmetros nulos/tipados em INSERT e filtros WHERE, ciclo de vida de sessão com Evict/Clear/Merge (incluindo garantia de que mudanças destacadas após Evict não persistem automaticamente no Flush, detach completo com Clear e recarga de estado via Refresh e recarga de associação após mutação de relacionamento), atualizações realistas de relacionamento com troca de FK por navegação, remoção de filho da coleção e movimentação de filho entre grupos (incluindo validações SQL da distribuição de FK e preservação da distribuição em rollback), concorrência otimista com detecção de stale/incremento de versão/refresh entre sessões (incluindo crescimento de versão por commit bem-sucedido e reconciliação de leitura obsoleta via Refresh), além de consultas avançadas com projeção HQL (incluindo mapas por alias via AliasToEntityMap, projeções com agregação por relacionamento e contagens escalares por filtro de relacionamento), ordenação por propriedade de relacionamento (incluindo janelas paginadas determinísticas) e Criteria com múltiplas restrições (incluindo alias de relacionamento e contagens escalares por alias))
