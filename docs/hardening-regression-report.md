# Relatório de hardening — qualidade, regressão e compatibilidade

## Escopo executado

1. **Padronização de mensagens de erro** para SQL não suportado (parser e `CommandMock` por provider).
2. **Testes de regressão adicionados** para validar o contrato de mensagem padronizada por dialeto.
3. **Matriz de compatibilidade** feature x dialeto publicada.
4. **Checklist de known gaps** atualizado com itens concluídos e pendentes.

## Regressões corrigidas

### 1) Mensagens inconsistentes de SQL não suportado

- **Problema**: exceções `NotSupportedException` com formatos heterogêneos, dificultando diagnóstico e assertivas de teste cross-provider.
- **Correção**: criação do helper interno `SqlUnsupported` com formato único:
  - `SQL não suportado para dialeto '{dialeto}' (v{versão}): {feature}.`
  - `SQL não suportado no parser: {feature}.`
  - `SQL não suportado em {operação} para dialeto '{dialeto}' (v{versão}): {tipoAST}.`
- **Impacto**: previsibilidade de erro e base para automação de validações por provider.

### 2) Ausência de regressão específica para contrato de erro por dialeto

- **Problema**: faltavam testes explícitos garantindo o formato de erro não suportado após mudanças no parser.
- **Correção**: novos testes em todos os providers principais para garantir presença do prefixo padronizado.
- **Impacto**: reduz chance de regressão silenciosa na experiência de uso/diagnóstico.

## Execução da suíte e regressões por provider

- **Status no ambiente atual**: não foi possível executar `dotnet test` pois o SDK .NET não está disponível no container (`dotnet: command not found`).
- **Ação recomendada**: rodar em ambiente com SDK .NET e consolidar resultados por projeto de teste:
  - `DbSqlLikeMem.MySql.Test`
  - `DbSqlLikeMem.SqlServer.Test`
  - `DbSqlLikeMem.Oracle.Test`
  - `DbSqlLikeMem.Npgsql.Test`
  - `DbSqlLikeMem.Db2.Test`
  - `DbSqlLikeMem.Sqlite.Test`

## Próximos itens priorizados por impacto

### Alta prioridade

1. **Automatizar execução matricial por provider no CI** com publicação de relatório por projeto.
2. **Expandir testes de equivalência cross-dialeto** para SQL comum (SELECT/WHERE/JOIN/GROUP).
3. **Mapear gaps de `MERGE` por dialeto/versão** com cobertura mínima de parser.

### Média prioridade

4. **Rastreabilidade matriz → teste** (cada célula com link para teste fonte).
5. **Normalização de paginação no AST** para reduzir divergência entre dialetos.

### Baixa prioridade

6. **Versionamento da matriz de compatibilidade por release** para histórico.
