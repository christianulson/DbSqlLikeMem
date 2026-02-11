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
- Serviço de validação de consistência (vermelho/amarelo/verde) comparando objeto local vs base.
- Persistência local de conexões/configurações + exportação/importação em JSON.

## Próximos passos sugeridos

1. Criar projeto VSIX + Tool Window (WPF) consumindo este Core.
2. Implementar botão **Adicionar Conexão** e popup de configuração de conexão.
3. Implementar botão de **Configurar Mapeamentos** ao lado de Adicionar Conexão.
4. Implementar menus de contexto na treeview:
   - Gerar classes
   - Checar consistência
5. Integrar provedores reais de metadados para SQL Server, Oracle, PostgreSQL etc.
