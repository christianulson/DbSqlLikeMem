# Checklist de known gaps (SQL)

> Checklist operacional para acompanhar gaps de compatibilidade e regressões por provider.

## Progresso de implementação (%)

> Status de checklist de escopo/implementação. A confirmação efetiva de execução completa deve ser sempre feita pela suíte local/CI do momento.

- **Parser e dialetos:** itens mapeados e implementados (4/4).
- **Executor e comportamento de runtime:** itens mapeados e implementados (3/3).
- **Testes e regressão:** itens mapeados com cobertura adicionada (4/4).
- **Documentação:** itens mapeados e atualizados (3/3).
- **Geral do checklist:** 14/14 itens tratados em código/documentação; validar execução no ambiente atual.

## Parser e dialetos

- [x] Cobrir `MERGE` por dialeto/versão (SQL Server/Oracle/DB2) com semântica mínima comum. (validação top-level de `USING`/`ON`/`WHEN`, incluindo regressões para `WHEN` aninhado e alias chamado `when`)
- [x] Expandir validação de `WITH RECURSIVE` por dialeto para mensagens de erro ainda mais orientadas a ação.
- [x] Consolidar suporte a paginação com normalização de AST (`LIMIT/OFFSET`, `OFFSET/FETCH`, `FETCH FIRST`) (normalização para `SqlLimitOffset` no parser + regressão por dialeto em suites de parser).
- [x] Revisar regras de quoting de identificadores por dialeto para casos de alias complexos. (cobertura de aceitação/rejeição + unescape de escapes dobrados para ``, "" e ]] por dialeto)

## Executor e comportamento de runtime

- [x] Aumentar cobertura de `UPDATE/DELETE ... JOIN` multi-tabela por dialeto. (execução validada para MySQL/SQL Server/PostgreSQL e bloqueio padronizado nos demais dialetos via `SqlUnsupported.ForDialect(...)`)
- [x] Completar execução de expressões JSON avançadas por provider. (cobertura de runtime reforçada para caminhos suportados e mensagens padronizadas para funções JSON não suportadas por dialeto)
- [x] Padronizar comportamento de erros de runtime entre providers para operações não suportadas. (uso consistente de `SqlUnsupported.ForDialect(...)` nos fluxos de mutação multi-tabela e regressões de mensagem em suites Dapper)

## Testes e regressão

- [x] Padronizar mensagem de `NotSupportedException` para SQL não suportado no parser.
- [x] Adicionar regressão de mensagem padronizada em testes de parser para MySQL/SQL Server/Oracle/Npgsql/DB2/SQLite.
- [x] Automatizar geração de relatório de regressão por provider em pipeline CI (workflow `provider-test-matrix.yml`).
- [x] Criar suíte de comparação cruzada (mesmo SQL em múltiplos dialetos) com snapshot de resultados esperados (suite em `scripts/run_cross_dialect_equivalence.sh`, baseline em `docs/cross-dialect-smoke-snapshot.md` e execução contínua no workflow `provider-test-matrix.yml` job `cross-dialect-smoke`).

## Documentação

- [x] Publicar matriz simplificada de compatibilidade feature x dialeto.
- [x] Versionar matriz por release (ex.: `vNext`, `vCurrent`) para facilitar leitura histórica.
- [x] Incluir links diretos de cada item da matriz para o teste correspondente.
