# Documentação do DbSqlLikeMem

Este diretório organiza o conteúdo por contexto para facilitar navegação, manutenção e evolução da documentação.

## Índice

- [Começando rápido](getting-started.md)
  - instalação
  - setup de provider
  - exemplos de uso
- [Provedores, versões e compatibilidade](providers-and-features.md)
  - matriz por banco
  - capacidades SQL por dialeto/versão
  - sugestões de evolução do parser
- [Matriz SQL (feature x dialeto)](sql-compatibility-matrix.md)
  - visão resumida por recursos
  - status por provider
- [Checklist de known gaps](known-gaps-checklist.md)
  - backlog técnico de compatibilidade
  - acompanhamento de hardening/regressão
- [Relatório de hardening/regressão](hardening-regression-report.md)
  - regressões corrigidas
  - próximos itens priorizados
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
