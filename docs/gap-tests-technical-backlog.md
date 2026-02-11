# Backlog técnico automatizado a partir de *GapTests

Fonte automática: varredura dos arquivos `*SqlCompatibilityGapTests.cs` e `*AdvancedSqlGapTests.cs` por provider.

## Priorização

Prioridade calculada por cobertura de providers + risco de regressão - esforço estimado.

## Épico: Parser

| Prioridade | Título | Provider(s) | Esforço | Risco de regressão | Dependências técnicas |
|---|---|---|---|---|---|
| P0 | Cast StringToInt | DB2, MySQL, Oracle, PostgreSQL, SQL Server, SQLite | M | Alto | AST + precedência de operadores |
| P0 | Regexp Operator | DB2, MySQL, Oracle, PostgreSQL, SQL Server, SQLite | M | Médio | AST + precedência de operadores |
| P0 | Union | DB2, MySQL, Oracle, PostgreSQL, SQL Server, SQLite | M | Médio | AST + precedência de operadores; Normalização de schemas em set operators |
| P1 | Where OR | DB2, MySQL, Oracle, PostgreSQL, SQL Server, SQLite | P | Baixo | AST + precedência de operadores |
| P1 | Cte With | DB2, MySQL, Oracle, PostgreSQL, SQL Server, SQLite | G | Médio | AST + precedência de operadores; Suporte a CTE no parser + binding |
| P1 | Union Inside SubSelect | DB2, MySQL, Oracle, PostgreSQL, SQL Server, SQLite | G | Médio | AST + precedência de operadores; Normalização de schemas em set operators |
| P2 | Where ParenthesesGrouping | DB2, MySQL, Oracle, PostgreSQL, SQL Server, SQLite | M | Baixo | AST + precedência de operadores |
| P2 | Where Precedence AND ShouldBindStrongerThan OR | DB2, MySQL, Oracle, PostgreSQL, SQL Server, SQLite | M | Baixo | AST + precedência de operadores |

## Épico: Executor

| Prioridade | Título | Provider(s) | Esforço | Risco de regressão | Dependências técnicas |
|---|---|---|---|---|---|
| P0 | CorrelatedSubquery InSelectList | DB2, MySQL, Oracle, PostgreSQL, SQL Server, SQLite | G | Alto | Planejador de execução em memória; Escopo de aliases em subquery correlata |
| P0 | Distinct ShouldBeConsistent | DB2, MySQL, Oracle, PostgreSQL, SQL Server, SQLite | P | Baixo | Planejador de execução em memória |
| P0 | GroupBy Having ShouldSupportAggregates | DB2, MySQL, Oracle, PostgreSQL, SQL Server, SQLite | M | Médio | Planejador de execução em memória; Pipeline de agregação + HAVING |
| P1 | Join ComplexOn WithOr | DB2, MySQL, Oracle, PostgreSQL, SQL Server, SQLite | G | Alto | Planejador de execução em memória |
| P1 | OrderBy Field Function | DB2, MySQL, Oracle, PostgreSQL, SQL Server, SQLite | M | Médio | Planejador de execução em memória |
| P1 | OrderBy ShouldSupportAlias And Ordinal | DB2, MySQL, Oracle, PostgreSQL, SQL Server, SQLite | M | Médio | Planejador de execução em memória |
| P2 | Select Expressions Arithmetic | DB2, MySQL, Oracle, PostgreSQL, SQL Server, SQLite | P | Baixo | Planejador de execução em memória |
| P2 | Window RowNumber PartitionBy | DB2, MySQL, Oracle, PostgreSQL, SQL Server, SQLite | G | Alto | Planejador de execução em memória; Engine de funções de janela |
| P2 | Date Function WithModifier | SQLite | P | Baixo | Planejador de execução em memória |
| P2 | TimestampAdd Day | DB2 | P | Baixo | Planejador de execução em memória |

## Épico: Funções SQL

| Prioridade | Título | Provider(s) | Esforço | Risco de regressão | Dependências técnicas |
|---|---|---|---|---|---|
| P0 | DateAdd IntervalDay | DB2, MySQL, Oracle, PostgreSQL, SQL Server, SQLite | M | Médio | Registro/catálogo de funções por provider |
| P0 | Functions COALESCE | DB2, MySQL, Oracle, PostgreSQL, SQL Server, SQLite | P | Baixo | Registro/catálogo de funções por provider |
| P0 | Functions CONCAT | DB2, MySQL, Oracle, PostgreSQL, SQL Server, SQLite | P | Baixo | Registro/catálogo de funções por provider |
| P1 | Functions IFNULL | DB2, MySQL, Oracle, PostgreSQL, SQL Server, SQLite | P | Baixo | Registro/catálogo de funções por provider |
| P1 | Select Expressions CASE WHEN | DB2, MySQL, Oracle, PostgreSQL, SQL Server, SQLite | M | Médio | Registro/catálogo de funções por provider |
| P1 | Select Expressions IF | DB2, MySQL, Oracle, PostgreSQL, SQL Server, SQLite | P | Baixo | Registro/catálogo de funções por provider |
| P2 | Select Expressions IIF ShouldWork AsAliasForIF | DB2, MySQL, Oracle, PostgreSQL, SQL Server, SQLite | P | Baixo | Registro/catálogo de funções por provider |

## Épico: Tipagem/Collation

| Prioridade | Título | Provider(s) | Esforço | Risco de regressão | Dependências técnicas |
|---|---|---|---|---|---|
| P0 | Collation CaseSensitivity ShouldFollowColumnCollation | DB2, MySQL, Oracle, PostgreSQL, SQL Server, SQLite | P | Alto | Matriz de coerção de tipos; Comparador com collation configurável |
| P0 | Typing ImplicitCasts And Collation ShouldMatchMySqlDefault | MySQL, Oracle, PostgreSQL, SQL Server | G | Alto | Matriz de coerção de tipos; Comparador com collation configurável |
| P0 | Typing ImplicitCasts And Collation ShouldMatchDb2Default | DB2 | G | Alto | Matriz de coerção de tipos; Comparador com collation configurável |
| P1 | Typing ImplicitCasts And Collation ShouldMatchSqliteDefault | SQLite | G | Alto | Matriz de coerção de tipos; Comparador com collation configurável |

## Formato alternativo (checklist para GitHub Projects/Jira)

### Parser
- [ ] **Cast StringToInt**  `providers: DB2, MySQL, Oracle, PostgreSQL, SQL Server, SQLite` · `esforço: M` · `risco: Alto`  Dependências: AST + precedência de operadores
- [ ] **Regexp Operator**  `providers: DB2, MySQL, Oracle, PostgreSQL, SQL Server, SQLite` · `esforço: M` · `risco: Médio`  Dependências: AST + precedência de operadores
- [ ] **Union**  `providers: DB2, MySQL, Oracle, PostgreSQL, SQL Server, SQLite` · `esforço: M` · `risco: Médio`  Dependências: AST + precedência de operadores; Normalização de schemas em set operators
- [ ] **Where OR**  `providers: DB2, MySQL, Oracle, PostgreSQL, SQL Server, SQLite` · `esforço: P` · `risco: Baixo`  Dependências: AST + precedência de operadores
- [ ] **Cte With**  `providers: DB2, MySQL, Oracle, PostgreSQL, SQL Server, SQLite` · `esforço: G` · `risco: Médio`  Dependências: AST + precedência de operadores; Suporte a CTE no parser + binding
- [ ] **Union Inside SubSelect**  `providers: DB2, MySQL, Oracle, PostgreSQL, SQL Server, SQLite` · `esforço: G` · `risco: Médio`  Dependências: AST + precedência de operadores; Normalização de schemas em set operators
- [ ] **Where ParenthesesGrouping**  `providers: DB2, MySQL, Oracle, PostgreSQL, SQL Server, SQLite` · `esforço: M` · `risco: Baixo`  Dependências: AST + precedência de operadores
- [ ] **Where Precedence AND ShouldBindStrongerThan OR**  `providers: DB2, MySQL, Oracle, PostgreSQL, SQL Server, SQLite` · `esforço: M` · `risco: Baixo`  Dependências: AST + precedência de operadores

### Executor
- [ ] **CorrelatedSubquery InSelectList**  `providers: DB2, MySQL, Oracle, PostgreSQL, SQL Server, SQLite` · `esforço: G` · `risco: Alto`  Dependências: Planejador de execução em memória; Escopo de aliases em subquery correlata
- [ ] **Distinct ShouldBeConsistent**  `providers: DB2, MySQL, Oracle, PostgreSQL, SQL Server, SQLite` · `esforço: P` · `risco: Baixo`  Dependências: Planejador de execução em memória
- [ ] **GroupBy Having ShouldSupportAggregates**  `providers: DB2, MySQL, Oracle, PostgreSQL, SQL Server, SQLite` · `esforço: M` · `risco: Médio`  Dependências: Planejador de execução em memória; Pipeline de agregação + HAVING
- [ ] **Join ComplexOn WithOr**  `providers: DB2, MySQL, Oracle, PostgreSQL, SQL Server, SQLite` · `esforço: G` · `risco: Alto`  Dependências: Planejador de execução em memória
- [ ] **OrderBy Field Function**  `providers: DB2, MySQL, Oracle, PostgreSQL, SQL Server, SQLite` · `esforço: M` · `risco: Médio`  Dependências: Planejador de execução em memória
- [ ] **OrderBy ShouldSupportAlias And Ordinal**  `providers: DB2, MySQL, Oracle, PostgreSQL, SQL Server, SQLite` · `esforço: M` · `risco: Médio`  Dependências: Planejador de execução em memória
- [ ] **Select Expressions Arithmetic**  `providers: DB2, MySQL, Oracle, PostgreSQL, SQL Server, SQLite` · `esforço: P` · `risco: Baixo`  Dependências: Planejador de execução em memória
- [ ] **Window RowNumber PartitionBy**  `providers: DB2, MySQL, Oracle, PostgreSQL, SQL Server, SQLite` · `esforço: G` · `risco: Alto`  Dependências: Planejador de execução em memória; Engine de funções de janela
- [ ] **Date Function WithModifier**  `providers: SQLite` · `esforço: P` · `risco: Baixo`  Dependências: Planejador de execução em memória
- [ ] **TimestampAdd Day**  `providers: DB2` · `esforço: P` · `risco: Baixo`  Dependências: Planejador de execução em memória

### Funções SQL
- [ ] **DateAdd IntervalDay**  `providers: DB2, MySQL, Oracle, PostgreSQL, SQL Server, SQLite` · `esforço: M` · `risco: Médio`  Dependências: Registro/catálogo de funções por provider
- [ ] **Functions COALESCE**  `providers: DB2, MySQL, Oracle, PostgreSQL, SQL Server, SQLite` · `esforço: P` · `risco: Baixo`  Dependências: Registro/catálogo de funções por provider
- [ ] **Functions CONCAT**  `providers: DB2, MySQL, Oracle, PostgreSQL, SQL Server, SQLite` · `esforço: P` · `risco: Baixo`  Dependências: Registro/catálogo de funções por provider
- [ ] **Functions IFNULL**  `providers: DB2, MySQL, Oracle, PostgreSQL, SQL Server, SQLite` · `esforço: P` · `risco: Baixo`  Dependências: Registro/catálogo de funções por provider
- [ ] **Select Expressions CASE WHEN**  `providers: DB2, MySQL, Oracle, PostgreSQL, SQL Server, SQLite` · `esforço: M` · `risco: Médio`  Dependências: Registro/catálogo de funções por provider
- [ ] **Select Expressions IF**  `providers: DB2, MySQL, Oracle, PostgreSQL, SQL Server, SQLite` · `esforço: P` · `risco: Baixo`  Dependências: Registro/catálogo de funções por provider
- [ ] **Select Expressions IIF ShouldWork AsAliasForIF**  `providers: DB2, MySQL, Oracle, PostgreSQL, SQL Server, SQLite` · `esforço: P` · `risco: Baixo`  Dependências: Registro/catálogo de funções por provider

### Tipagem/Collation
- [ ] **Collation CaseSensitivity ShouldFollowColumnCollation**  `providers: DB2, MySQL, Oracle, PostgreSQL, SQL Server, SQLite` · `esforço: P` · `risco: Alto`  Dependências: Matriz de coerção de tipos; Comparador com collation configurável
- [ ] **Typing ImplicitCasts And Collation ShouldMatchMySqlDefault**  `providers: MySQL, Oracle, PostgreSQL, SQL Server` · `esforço: G` · `risco: Alto`  Dependências: Matriz de coerção de tipos; Comparador com collation configurável
- [ ] **Typing ImplicitCasts And Collation ShouldMatchDb2Default**  `providers: DB2` · `esforço: G` · `risco: Alto`  Dependências: Matriz de coerção de tipos; Comparador com collation configurável
- [ ] **Typing ImplicitCasts And Collation ShouldMatchSqliteDefault**  `providers: SQLite` · `esforço: G` · `risco: Alto`  Dependências: Matriz de coerção de tipos; Comparador com collation configurável
