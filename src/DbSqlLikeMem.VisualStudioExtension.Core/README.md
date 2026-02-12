# DbSqlLikeMem.VisualStudioExtension.Core

Projeto inicial para suportar uma extensão do Visual Studio focada em geração de classes a partir de objetos de banco.

## Escopo inicial implementado

- Modelos de domínio para conexões, objetos de banco, mapeamentos e geração.
- Montagem de árvore no formato:
  - Tipo de Banco
    - Nome do Banco
      - Tipo de Objeto (Tables, Views, Procedures)
        - Lista de objetos
- Filtro de objetos por `Equals` e `Like` (contains case-insensitive).
- Planejador de geração que identifica ausência de mapeamento e permite bloquear geração até configurar.
- Gerador de classes que sobrescreve arquivos existentes.
  - Suporta placeholders de nome de arquivo por objeto: `{Name}`, `{NamePascal}`, `{Type}`, `{Schema}`, `{DatabaseType}` e `{DatabaseName}`.
  - Padrão alinhado ao console para todos os bancos suportados: `{NamePascal}{Type}Factory.cs`.
- Serviço de validação de consistência (vermelho/amarelo/verde) comparando objeto local vs base.
- Persistência local de conexões/configurações + exportação/importação em JSON.
- Provedor SQL de metadados estruturais (`SqlDatabaseMetadataProvider`) com consultas por banco (MySql, SqlServer, PostgreSql, Oracle, Sqlite, Db2) para listar objetos e colunas.
- Fábrica de conteúdo estruturado (`StructuredClassContentFactory`) com metadados serializados no arquivo gerado.
- Leitor de snapshot local (`GeneratedClassSnapshotReader`) para comparar arquivo gerado vs estrutura atual extraída do banco via `ObjectConsistencyChecker`.

## Próximos passos sugeridos

> Status: os itens 1, 2 e 3 já possuem um esqueleto inicial em `../DbSqlLikeMem.VisualStudioExtension` (VSIX + Tool Window + popups).

1. Evoluir fluxo de conexão (teste de conexão, edição/remoção, segurança de credenciais).
2. Evoluir fluxo de mapeamento por tipo de objeto e por conexão.
3. Implementar menus de contexto na treeview:
   - Gerar classes
   - Checar consistência
4. Integrar provedores reais de metadados para SQL Server, Oracle, PostgreSQL etc.
