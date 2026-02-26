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
- NHibernate contract coverage shared across providers:
  - **Core ORM**: native SQL params, mapped entity lifecycle, transaction rollback, pagination, HQL/Criteria basics, null/typed params in `INSERT` and `WHERE`.
  - **Session/state**: Evict/Clear/Merge detached flows, detached-change protection after Evict, full detach checks with Clear, Refresh-based reload, explicit Update-vs-Merge reattach semantics (`Contains`/identity), including Update identity-conflict (`NonUniqueObjectException`) and Merge over an already managed instance, `IsDirty`, SaveOrUpdate and FlushMode contracts.
  - **Relationships**: realistic FK updates by navigation/reparenting/controlled removal, SQL FK-distribution verification with rollback-preserved distribution, `Cascade.None` graph-update behavior (no implicit transient-child persistence), repeated optional-association transitions `null -> group -> null` in a fresh session, physical-FK parent-delete behavior (fails with existing children, succeeds after explicit child dissociation), inverse-collection-only mutation semantics (remove/add do not change/assign FK while many-to-one remains the owning side), owning-side assignment semantics (many-to-one assignment persists FK and is visible from parent collection in a fresh session), and conflict resolution where owning-side assignment wins over inverse-only collection add.
  - **Optimistic concurrency**: stale detection, version increments, refresh+retry, idempotent business-intent reapply, alternating updates across three sessions with predictable version growth.
  - **Advanced querying**: HQL projections (including alias maps), relationship aggregates, left-join projections preserving rows without relationships, Criteria restrictions/projections (including projection+ordering+paging), relationship ordering + deterministic paging, and HQL `join fetch` eager-initialization checks.
  - **Known limitation (mock harness)**: exact SQL-count/N+1 metrics are documented as out of scope.

For full details, see the local repository documentation at `docs/old/providers-and-features.md`.

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
- Cobertura de contrato NHibernate compartilhada entre provedores:
  - **Core ORM**: parâmetros em SQL nativo, ciclo de vida de entidade mapeada, rollback transacional, paginação, base de HQL/Criteria, parâmetros nulos/tipados em `INSERT` e filtros `WHERE`.
  - **Sessão/estado**: fluxos detached com Evict/Clear/Merge, proteção de mudanças destacadas após Evict, validação de detach completo com Clear, recarga via Refresh, semântica explícita de reattach em Update-vs-Merge (`Contains`/identidade), incluindo conflito de identidade em Update (`NonUniqueObjectException`) e Merge sobre instância já gerenciada, `IsDirty`, SaveOrUpdate e contratos de FlushMode.
  - **Relacionamentos**: troca de FK por navegação/reparenting/remoção controlada, validações SQL da distribuição de FK com preservação em rollback, `Cascade.None` sem persistência implícita de filho transient e transições repetidas de associação opcional `null -> group -> null` com consistência em nova sessão, comportamento com FK física na exclusão do pai (falha com filhos existentes e sucesso após dissociação explícita do filho), semântica de mutação apenas da coleção inversa (remoção/adição sem mudança/atribuição de FK enquanto many-to-one for o lado dono), semântica de atribuição no lado dono (many-to-one persiste FK e reaparece na coleção do pai em nova sessão), e resolução de conflito na qual a atribuição no lado dono prevalece sobre add apenas na coleção inversa.
  - **Concorrência otimista**: detecção de stale, incremento de versão, refresh+retry, reaplicação idempotente de intenção de negócio e alternância entre três sessões com incremento previsível de versão.
  - **Consultas avançadas**: projeções HQL (incluindo mapas por alias), agregações por relacionamento, left join preservando linha sem relacionamento, Criteria com restrições/projeções (incluindo projeção+ordenação+paginação), ordenação relacional com paginação determinística e verificação de inicialização eager via `join fetch`.
  - **Limitação conhecida (harness mock)**: métricas exatas de contagem SQL/N+1 ficam documentadas como fora de escopo.
