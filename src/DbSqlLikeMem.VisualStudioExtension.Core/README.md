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
- Serviço de validação de consistência (vermelho/amarelo/verde) comparando objeto local vs base e distinguindo trio local incompleto de divergência real de metadados.
- Persistência local de conexões/configurações + exportação/importação em JSON.
- Provedor SQL de metadados estruturais (`SqlDatabaseMetadataProvider`) com consultas por banco (MySql, SqlServer/SqlAzure, PostgreSql, Oracle, Sqlite, Db2) para listar objetos e colunas.
- Extração de metadata de `Sequence` para os bancos suportados por essa feature (SqlServer/SqlAzure, PostgreSql, Oracle e Db2).
- Fábrica de conteúdo estruturado (`StructuredClassContentFactory`) com metadados serializados no arquivo gerado.
- Renderizador compartilhado de tokens de template (`TemplateContentRenderer`) para `Model`/`Repository`, incluindo `{{Namespace}}`.
- Catálogo de baseline versionada (`TemplateBaselineCatalog`) apontando para `templates/dbsqllikemem/vCurrent` com perfis `api` e `worker`, incluindo resolução da raiz do repositório para reaproveitamento pela VSIX.
- Catálogo de tokens suportados (`TemplateTokenCatalog`) para detectar placeholders fora do contrato antes de aceitar templates customizados.
- Resolvedor compartilhado de nome de arquivo (`TemplateFileNamePatternResolver`) para geração e consistência de `Model`/`Repository` com placeholders configuráveis.
- Serviço de mapeamento por conexão/tipo (`ConnectionMappingService`) para evitar que ajustes de `Table`, `View`, `Procedure` ou `Sequence` contaminem outros tipos já configurados na VSIX.
- Leitor de snapshot local (`GeneratedClassSnapshotReader`) para comparar arquivo gerado vs estrutura atual extraída do banco via `ObjectConsistencyChecker`.

## Próximos passos sugeridos

> Status: os itens 1, 2 e 3 já possuem implementação funcional inicial em `../DbSqlLikeMem.VisualStudioExtension` (conexão, mapeamento, árvore, geração e checagem de consistência).

1. Evoluir fluxo de conexão (teste de conexão, edição/remoção, segurança de credenciais).
2. Evoluir fluxo de mapeamento por tipo de objeto e por conexão.
3. Implementar menus de contexto na treeview:
   - Gerar classes
   - Checar consistência
4. Integrar provedores reais de metadados para SQL Server, Oracle, PostgreSQL etc.
