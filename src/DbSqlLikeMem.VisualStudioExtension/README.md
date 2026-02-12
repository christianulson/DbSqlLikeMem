# DbSqlLikeMem.VisualStudioExtension

Projeto VSIX inicial para hospedar a interface do DbSqlLikeMem no Visual Studio.

## O que está implementado

- `AsyncPackage` com comando de menu em **View > DbSqlLikeMem Explorer**.
- `ToolWindow` WPF com árvore baseada no `TreeViewBuilder` do Core.
- Botão **Adicionar conexão** com popup para nome, tipo de banco e connection string.
- Botão **Configurar mapeamentos** com popup para padrão de nome de arquivo e diretório de saída.
- Persistência de conexões e mapeamentos usando `StatePersistenceService` do Core.
- Compatibilidade de instalação configurada para Visual Studio **2019, 2022 e linha futura (incluindo 2026)** (`[16.0,19.0)`) nas edições Community/Professional/Enterprise.

## Próximos incrementos recomendados

- Menus de contexto da árvore (Gerar classes / Checar consistência).
- Carregamento real dos objetos do banco via `SqlDatabaseMetadataProvider`.
- Fluxo de configuração de mapeamento por tipo de objeto.
