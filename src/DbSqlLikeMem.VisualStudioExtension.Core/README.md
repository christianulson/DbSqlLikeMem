# DbSqlLikeMem.VisualStudioExtension.Core

Projeto inicial para suportar uma extensão do Visual Studio focada em geração de classes a partir de objetos de banco.

## Escopo inicial implementado

- Modelos de domínio para conexões, objetos de banco, mapeamentos e geração.
- Montagem de árvore no formato:
  - Tipo de Banco
    - Nome do Banco
      - Tipo de Objeto (Tables, Views, Procedures, Sequences)
        - Lista de objetos
- Filtro de objetos por `Equals` e `Like` (contains case-insensitive).
- Planejador de geração que identifica ausência de mapeamento e permite bloquear geração até configurar.
- Gerador de classes que sobrescreve arquivos existentes.
  - Suporta placeholders de nome de arquivo por objeto: `{Name}`, `{NamePascal}`, `{Type}`, `{Schema}`, `{DatabaseType}`, `{DatabaseName}` e `{Namespace}`.
  - Padrão alinhado ao console para todos os bancos suportados: `{NamePascal}{Type}Factory.cs`.
- Serviço de validação de consistência (vermelho/amarelo/verde) comparando objeto local vs base, distinguindo trio local incompleto de divergência real de metadados e expondo os artefatos faltantes em ordem determinística para a UI.
- O mesmo serviço de consistência agora também detecta drift de snapshot em `class/model/repository`, reaproveitando `GeneratedClassSnapshotReader` para verificar se os três artefatos ainda apontam para o mesmo objeto de banco.
- Persistência local de conexões/configurações + exportação/importação em JSON.
- Provedor SQL de metadados estruturais (`SqlDatabaseMetadataProvider`) com consultas por banco (MySql, SqlServer/SqlAzure, PostgreSql, Oracle, Sqlite, Db2) para listar objetos e colunas.
- Extração de metadata de `Sequence` para os bancos suportados por essa feature (SqlServer/SqlAzure, PostgreSql, Oracle e Db2).
- Fábrica de conteúdo estruturado (`StructuredClassContentFactory`) com metadados serializados no arquivo gerado.
- Renderizador compartilhado de tokens de template (`TemplateContentRenderer`) para `Model`/`Repository`, incluindo `{{Namespace}}`.
- O mesmo renderizador de template também prependa cabeçalho padronizado `// DBSqlLikeMem:*` para preservar rastreabilidade do objeto de origem nos artefatos `Model`/`Repository`.
- O leitor de snapshot (`GeneratedClassSnapshotReader`) agora também preserva `Triggers` e a checagem de consistência compara propriedades dos artefatos complementares contra a classe principal gerada para reduzir falso verde em `Model`/`Repository`.
- Catálogo de baseline versionada (`TemplateBaselineCatalog`) apontando para `templates/dbsqllikemem/vCurrent` com perfis `api` e `worker`, incluindo resolução da raiz do repositório para reaproveitamento pela VSIX.
- Leitor de metadados de revisão (`TemplateReviewMetadataReader`) para carregar `templates/dbsqllikemem/review-metadata.json` quando a baseline versionada está disponível no ambiente atual.
- Validador de governança (`TemplateBaselineGovernance`) para detectar drift entre o catálogo do core e os metadados versionados de revisão, incluindo janela de revisão vencida.
- Formatter compartilhado (`TemplateBaselinePresentation`) para expor descrição, foco de testes, revisão e recomendações de mapeamento dos perfis nas UIs sem duplicar strings da baseline.
- O mesmo formatter agora também expõe os diretórios recomendados de saída de `Model`/`Repository`, mantendo a decisão operacional alinhada ao catálogo versionado.
- Catálogo de tokens suportados (`TemplateTokenCatalog`) para detectar placeholders fora do contrato antes de aceitar templates customizados.
- Resolvedor compartilhado de nome de arquivo (`TemplateFileNamePatternResolver`) para geração e consistência de `Model`/`Repository` com placeholders configuráveis.
- Serviço de mapeamento por conexão/tipo (`ConnectionMappingService`) para evitar que ajustes de `Table`, `View`, `Procedure` ou `Sequence` contaminem outros tipos já configurados na VSIX, além de reutilizar os defaults versionados `api`/`worker` no fluxo de `Configure Mappings`.
- Leitor de snapshot local (`GeneratedClassSnapshotReader`) para comparar arquivo gerado vs estrutura atual extraída do banco via `ObjectConsistencyChecker`.

## Próximos passos sugeridos

> Status: os itens 1, 2 e 3 já possuem implementação funcional inicial em `../DbSqlLikeMem.VisualStudioExtension` (conexão, mapeamento, árvore, geração e checagem de consistência).

1. Evoluir fluxo de conexão (teste de conexão, edição/remoção, segurança de credenciais).
2. Evoluir fluxo de mapeamento por tipo de objeto e por conexão.
3. Implementar menus de contexto na treeview:
   - Gerar classes
   - Checar consistência
4. Integrar provedores reais de metadados para SQL Server, Oracle, PostgreSQL etc.
