# Documentação do DbSqlLikeMem

Este diretório organiza o conteúdo por contexto para facilitar navegação, manutenção e evolução da documentação.

## Índice

- [Começando rápido](getting-started.md)
  - instalação
  - setup de provider
  - exemplos de uso
  - checklist de revisão da documentação pós-mudanças
- [Provedores, versões e compatibilidade](old/providers-and-features.md)
  - matriz por banco
  - capacidades SQL por dialeto/versão
  - sugestões de evolução do parser
- [Plano global de evolução (TDD-first)](old/global-evolution-plan.md)
  - avaliação consolidada de documentação, planos e código
  - estratégia integrada por fases com foco em TDD
- [Roadmap do Core (Parser/Executor)](old/core-parser-executor-roadmap.md)
  - melhorias priorizadas por parser/executor
  - separação de especificidades por dialeto/versão
- [Prompts de implementação (copy/paste)](old/implementation-prompts.md)
  - roadmap em fases
  - prompts prontos para paralelizar implementações
- [Playbook de IA para testes de repositório/integração em projeto externo](ai-nuget-test-projects-playbook.md)
  - prompt pronto para pedir criação de testes no projeto XPTO
  - fluxo rápido para IA se localizar no repositório
  - template e checklist de validação
- [Plano executável P7–P10](old/p7-p10-implementation-plan.md)
  - matriz por provider
  - testes-alvo para implementação
- [Matriz SQL (feature x dialeto)](sql-compatibility-matrix.md)
  - visão resumida por recursos
  - status por provider
  - links diretos para testes de referência
- [Matriz SQL versionada (vCurrent/vNext)](sql-compatibility-matrix-vcurrent.md)
  - leitura histórica por release
  - acompanhamento de evolução planejada
- [Checklist de known gaps](old/known-gaps-checklist.md)
  - backlog técnico de compatibilidade
  - acompanhamento de hardening/regressão
- [Snapshot cross-dialect (smoke)](cross-dialect-smoke-snapshot.md)
  - baseline de equivalência entre providers
  - atualização via script automatizado
- [Relatório de hardening/regressão](old/hardening-regression-report.md)
  - regressões corrigidas
  - próximos itens priorizados
- [Tracker de transação e concorrência](old/transaction-concurrency-implementation-tracker.md)
  - fases de implementação por concorrência transacional
  - progresso e próximos passos de isolamento/savepoint/stress
- [Backlog técnico de testes de gaps](old/gap-tests-technical-backlog.md)
  - priorização de testes por feature SQL
  - referência para cobertura incremental
- [Relatório de readiness para NuGet](nuget-readiness-validation-report.md)
  - checklist de empacotamento
  - validações antes de publicar
- [Revisão de performance (work branch)](performance-review-work-branch.md)
  - achados de performance
  - recomendações de otimização
- [Testes por versão de dialeto](testes-por-versao-dialect.md)
  - cobertura por versão simulada
  - rastreabilidade de comportamento por provider
- [Publicação](publishing.md)
  - NuGet
  - Visual Studio (VSIX)
  - VS Code Marketplace
- [Wiki do GitHub](wiki/README.md)
  - como habilitar e estruturar
  - páginas prontas para copiar/publicar

## Convenções sugeridas para novos documentos

- Um tópico por arquivo (evitar README gigante).
- Títulos orientados a tarefa (`como instalar`, `como publicar`, `compatibilidade` etc.).
- Sempre incluir links cruzados para páginas relacionadas.
- Quando possível, manter exemplos executáveis (`dotnet test`, `dotnet pack`, etc.).
