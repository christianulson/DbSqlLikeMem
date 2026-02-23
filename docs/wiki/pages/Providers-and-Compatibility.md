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
- NHibernate contract coverage shared across providers (native SQL params, mapped entity lifecycle, transaction rollback, pagination, HQL/Criteria, null/typed parameters in both INSERT and WHERE filters, session lifecycle with Evict/Clear/Merge (including detached changes after Evict not auto-persisting on Flush, full detach verification on Clear, and state reload via Refresh, and association reload after relationship mutation), realistic relationship updates including FK changes by navigation, child removal from collections, and child moves between groups (including SQL FK-distribution checks and rollback-preserved distribution), optimistic concurrency scenarios including stale detection/version increments/refresh between sessions (including version growth per successful commit and stale-read reconciliation via Refresh), plus advanced querying with HQL projection (including alias maps via AliasToEntityMap, relationship aggregate projections, left-join projections preserving rows without relationships, and scalar counts by relationship filter), relationship-based ordering (including deterministic paged windows), and Criteria with multiple restrictions (including relationship aliases, Conjunction/Disjunction combinations with deterministic ordering, and scalar counts by relationship alias), SaveOrUpdate on detached entities, FlushMode.Commit commit-time persistence visibility, multiple many-to-one reassignments in the same session persisting the final FK, inverse-collection consistency after reparenting in a new session, stale-update recovery with refresh+controlled retry (including business-intent reapply after refresh), alternating sequential versioned updates across sessions with predictable version increments, and FlushMode.Manual with and without explicit Flush (persist + no-persist on commit), plus nullable many-to-one association persistence with null FK, HQL null-association filtering, SaveOrUpdate transient-entity insert behavior, and FlushMode.Auto auto-flush before query))

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
- Cobertura de contrato NHibernate compartilhada entre provedores (parâmetros em SQL nativo, ciclo de vida de entidade mapeada, rollback transacional, paginação, HQL/Criteria e parâmetros nulos/tipados em INSERT e filtros WHERE, ciclo de vida de sessão com Evict/Clear/Merge (incluindo garantia de que mudanças destacadas após Evict não persistem automaticamente no Flush, detach completo com Clear e recarga de estado via Refresh e recarga de associação após mutação de relacionamento), atualizações realistas de relacionamento com troca de FK por navegação, remoção de filho da coleção e movimentação de filho entre grupos (incluindo validações SQL da distribuição de FK e preservação da distribuição em rollback), concorrência otimista com detecção de stale/incremento de versão/refresh entre sessões (incluindo crescimento de versão por commit bem-sucedido e reconciliação de leitura obsoleta via Refresh), além de consultas avançadas com projeção HQL (incluindo mapas por alias via AliasToEntityMap, projeções com agregação por relacionamento, projeção com left join preservando linha sem relacionamento e contagens escalares por filtro de relacionamento), ordenação por propriedade de relacionamento (incluindo janelas paginadas determinísticas) e Criteria com múltiplas restrições (incluindo alias de relacionamento, combinações Conjunction/Disjunction com ordenação determinística e contagens escalares por alias), SaveOrUpdate em entidade detached, visibilidade de persistência no commit com FlushMode.Commit, múltiplas trocas many-to-one na mesma sessão persistindo o FK final, consistência de coleção inversa após reparenting em nova sessão, recuperação de stale-update com refresh+retry controlado (incluindo reaplicação de intenção de negócio após refresh), updates sequenciais alternados entre sessões com incremento previsível de versão e FlushMode.Manual com e sem Flush explícito (persistência e não-persistência no commit), além de associação many-to-one anulável com FK nula persistida, filtro HQL para associação nula, SaveOrUpdate de entidade transient (insert) e autoflush em FlushMode.Auto antes de query))
