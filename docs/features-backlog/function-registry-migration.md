# Function registry migration tracker

Este tracker acompanha a migracao do modelo de funcoes do core para `DbFunctionDef` e o fechamento da ponte de compatibilidade.

## Objetivo

- Centralizar o contrato de funcoes em um modelo unico e mais rico.
- Reduzir dependencias entre parser, dialetos, runtime e registries legados.
- Manter o projeto buildavel durante a transicao, sem quebrar provider-specific behavior.

## Status atual

- [x] Novo modelo detalhado de funcao criado em `src/DbSqlLikeMem/Models/DbFunctionDef.cs`.
- [x] Contrato de compatibilidade criado em `src/DbSqlLikeMem/Models/DbFunctionCompatibility.cs`.
- [x] Runtime de create function migrado para `DbFunctionDef`.
- [x] Parser de `CREATE FUNCTION` migrado para construir `DbFunctionDef`.
- [x] AST de `SqlCreateFunctionQuery` passou a carregar `Definition`.
- [x] Helpers de table function e window function foram alinhados ao novo modelo.
- [x] Registries de `SqlServer`, `Npgsql` e `Oracle` foram adaptados aos novos overloads principais.
- [x] Extensoes de registro passaram a aceitar `DbFunctionDef` diretamente.
- [x] Extensoes de registro passaram a aceitar `DbInvocationStyle` diretamente.
- [x] Ponte legada (`DbScalarFunctionDef`/`DbTableFunctionDef`) foi removida do caminho normal.
- [x] Registries de `MySql`, `Db2`, `Sqlite` e `SqlServer` foram migrados para o contrato novo onde ainda havia wrappers.
- [x] `Npgsql` deixou de depender de `SqlFunctionBodyFactory.Identity()` nos registros principais.
- [x] `MySql` e `Oracle` deixaram de depender de `SqlFunctionBodyFactory.Identity()` nos registros principais.
- [x] Registries centrais de `Auto`, `Db2`, `Sqlite` e `SqlServer` passaram a usar `DbInvocationStyle`.
- [x] Objetos e overloads legados foram marcados como `Obsolete` para guiar a migracao.
- [x] Testes de parser afetados por `SqlCreateFunctionQuery` foram ajustados para `Definition`.
- [x] `Auto`, `SqlServer`, `Db2` e `Sqlite` deixaram de depender de `SqlFunctionBodyFactory.Identity()` nos registros principais.
- [x] Remover a ponte `DbScalarFunctionDef` e `DbTableFunctionDef` quando nao houver mais consumidores.
- [x] Lógica interna repetida em `SqlDialectScalarFunctionRegistryExtensions` foi consolidada.
- [x] Consolidar overloads de registro para reduzir duplicacao entre `Func<SqlExpr, object>` e `AstQueryGeneralScalarFunctionHandler`.
- [x] API condicional `AddScalarFunctionIf`/`AddScalarFunctionsIf` foi removida dos registries.
- [x] Helper legado `SqlFunctionBodyFactory` foi removido por nao haver mais consumidores.
- [x] Migrar os providers restantes para `DbInvocationStyle` e remover o enum legado quando nao houver mais consumo.
- [x] Revisar `SqlAzure` e confirmar que nao havia registry de funcoes dedicado para migrar.
- [x] Remover DTOs legados de funcao quando a compatibilidade nao for mais necessaria.
- [x] Completar a documentacao XML de novos tipos publicos expostos pela migracao.

## Fases

### Fase 1 - Estrutura do contrato

- [x] Criar `DbFunctionDef` com capacidades, assinaturas e estilo de invocacao.
- [x] Criar `DbFunctionParameterDef` e `DbFunctionSignature`.
- [x] Incluir suporte para funcoes escalares, de janela, de tabela e temporais.

### Fase 2 - Compatibilidade temporaria

- [x] Criar ponte para os registries existentes.
- [x] Manter runtime e parser funcionando enquanto a transicao acontece.
- [x] Encerrar a ponte depois da migração dos providers.

### Fase 3 - Parser e AST

- [x] Migrar `SqlCreateFunctionQuery` para armazenar `Definition`.
- [x] Migrar o parser de parametros de funcao para `DbFunctionParameterDef`.
- [x] Migrar o parser de corpo de funcao para `DbFunctionDef.CreateUserDefined(...)`.
- [x] Atualizar testes de parser que dependiam dos campos antigos.

### Fase 4 - Providers

- [x] Adaptar `SqlServer` aos overloads novos.
- [x] Adaptar `Npgsql` aos overloads novos.
- [x] Adaptar `Oracle` aos overloads novos.
- [x] Revisar `MySql`, `Db2` e `Sqlite` para reduzir ainda mais o uso de wrappers temporarios.
- [x] Revisar `SqlAzure` e qualquer provider restante para reduzir ainda mais o uso de wrappers temporarios.
- [x] Validar se existe alguma funcao especial que ainda merece registrador dedicado por provider.

### Fase 5 - Cleanup

- [x] Remover arquivos e tipos legados apos a eliminacao dos ultimos consumidores.
- [ ] Revisar warnings de nullability e XML docs nos novos tipos publicos.
- [x] Consolidar o backlog em um documento final quando a migracao estiver fechada.

## Proximo passo recomendado

1. Consolidar os overloads restantes em `SqlDialectScalarFunctionRegistryExtensions`.
2. Revisar warnings de nullability e XML docs nos novos tipos publicos.

## Notas

- Este tracker acompanha apenas a migracao das funcoes.
- O backlog funcional principal continua em `docs/features-backlog/index.md`.
- O status operacional de curto prazo continua em `docs/features-backlog/status-operational.md`.
