# Checklist de known gaps (SQL)

> Checklist operacional para acompanhar gaps de compatibilidade e regressões por provider.

## Parser e dialetos

- [ ] Cobrir `MERGE` por dialeto/versão (SQL Server/Oracle/DB2) com semântica mínima comum. (diagnóstico acionável de não suporte implementado; evolução semântica por dialeto ainda pendente)
- [x] Expandir validação de `WITH RECURSIVE` por dialeto para mensagens de erro ainda mais orientadas a ação.
- [x] Consolidar suporte a paginação com normalização de AST (`LIMIT/OFFSET`, `OFFSET/FETCH`, `FETCH FIRST`) (normalização para `SqlLimitOffset` no parser + regressão por dialeto em suites de parser).
- [ ] Revisar regras de quoting de identificadores por dialeto para casos de alias complexos.

## Executor e comportamento de runtime

- [ ] Aumentar cobertura de `UPDATE/DELETE ... JOIN` multi-tabela por dialeto.
- [ ] Completar execução de expressões JSON avançadas por provider.
- [ ] Padronizar comportamento de erros de runtime entre providers para operações não suportadas.

## Testes e regressão

- [x] Padronizar mensagem de `NotSupportedException` para SQL não suportado no parser.
- [x] Adicionar regressão de mensagem padronizada em testes de parser para MySQL/SQL Server/Oracle/Npgsql/DB2/SQLite.
- [x] Automatizar geração de relatório de regressão por provider em pipeline CI (workflow `provider-test-matrix.yml`).
- [ ] Criar suíte de comparação cruzada (mesmo SQL em múltiplos dialetos) com snapshot de resultados esperados (smoke + export para snapshot adicionados em `scripts/run_cross_dialect_equivalence.sh`; baseline em `docs/cross-dialect-smoke-snapshot.md`, execução contínua pendente de ambiente CI/local com dotnet).

## Documentação

- [x] Publicar matriz simplificada de compatibilidade feature x dialeto.
- [x] Versionar matriz por release (ex.: `vNext`, `vCurrent`) para facilitar leitura histórica.
- [x] Incluir links diretos de cada item da matriz para o teste correspondente.
