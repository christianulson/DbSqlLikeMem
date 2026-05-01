# Fase 8 - Testes faltantes do banco real

## Status

DONE

## Percentual de entrega

100%

## Objetivo

Criar testes para funcionalidades que existem no banco real mas ainda não têm cobertura de fidelidade.

## Regra Central

- Se a funcionalidade existe no banco real, deve existir um teste para ela.
- Não importa se a funcionalidade existe no mock, o teste vem primeiro.
- O teste serve de base para futura implementação no código.

## Inventário De Funcionalidades Por Provider

### SQLite

### SQL Server

- `NEXT VALUE FOR ... FROM Users WHERE ...` para validar a expressao de sequence em consulta filtrada.
- `sys.sequences.current_value` para validar o valor atual antes e depois do consumo da sequence.
- `CREATE SEQUENCE ... INCREMENT BY` para alterar o passo da sequence gerada.
- `CREATE SEQUENCE ... CYCLE` para validar o reinicio automatico ao atingir o valor maximo.
- `ALTER SEQUENCE ... RESTART WITH` para reiniciar a proxima sequence gerada.
- `DROP SEQUENCE IF EXISTS` para validar remocao idempotente da sequence.

### SQL Azure

### MySQL

- `CREATE TEMPORARY TABLE ... AS SELECT` para validar parser e execucao em multiplas instrucoes.

### MariaDB

### Npgsql

- `ALTER SEQUENCE ... RESTART WITH` para reiniciar a próxima sequência gerada.
- `ALTER SEQUENCE ... INCREMENT BY` para alterar o passo da próxima sequência gerada.
- `ALTER SEQUENCE ... OWNED BY NONE` para desvincular a sequence da tabela proprietária.
- `ALTER SEQUENCE ... OWNED BY` para validar queda automática da sequence junto com a tabela proprietária.
- `CREATE SEQUENCE ... CYCLE` para validar o reinicio automatico ao atingir o valor maximo.
- `CREATE SEQUENCE ... MAXVALUE` para validar o limite superior da sequence.
- `CREATE SEQUENCE ... INCREMENT BY -2` para validar contagem descendente da sequence.
- `CREATE SEQUENCE ... MINVALUE` para validar o limite inferior da sequence.
- `DROP SEQUENCE IF EXISTS` para validar remocao idempotente da sequence.
- `currval` e `lastval` após `ALTER SEQUENCE ... RESTART WITH` para validar a sessão atual.
- `setval(..., false)` para manter `lastval` estável até o próximo consumo.
- `currval` e `lastval` como valores locais à sessão em duas conexões independentes.
- sequence qualificada por schema para validar resolução completa do nome.
- `DROP SEQUENCE` dentro de transação para validar rollback de DDL de sequence.
- `CREATE SEQUENCE IF NOT EXISTS` para validar criação idempotente da sequence.

### Oracle

### Db2

- `NEXT VALUE FOR` e `PREVIOUS VALUE FOR` como valores locais à sessão em duas conexões independentes.
- `NEXT VALUE FOR ... FROM Users WHERE ...` para validar a expressão de sequence em consulta filtrada.

### Firebird

## Testes Criados

- [SequenceTests.cs](../../../src/code/DbSqlLikeMem.Npgsql.Test/Fidelity/DML/SequenceTests.cs): `AlterSequenceRestartWith_ShouldResetNextGeneratedValue`
- [SequenceTests.cs](../../../src/code/DbSqlLikeMem.Npgsql.Test/Fidelity/DML/SequenceTests.cs): `AlterSequenceIncrementBy_ShouldChangeNextGeneratedValue`
- [SequenceTests.cs](../../../src/code/DbSqlLikeMem.Npgsql.Test/Fidelity/DML/SequenceTests.cs): `AlterSequenceOwnedByNone_ShouldPreserveSequenceAfterTableDrop`
- [SequenceTests.cs](../../../src/code/DbSqlLikeMem.Npgsql.Test/Fidelity/DML/SequenceTests.cs): `AlterSequenceOwnedByTable_ShouldDropSequenceWithTable`
- [SequenceTests.cs](../../../src/code/DbSqlLikeMem.Npgsql.Test/Fidelity/DML/SequenceTests.cs): `CreateCycleSequence_ShouldWrapBackToMinimumValue`
- [SequenceTests.cs](../../../src/code/DbSqlLikeMem.Npgsql.Test/Fidelity/DML/SequenceTests.cs): `CreateBoundedSequence_ShouldStopAtMaximumValue`
- [SequenceTests.cs](../../../src/code/DbSqlLikeMem.Npgsql.Test/Fidelity/DML/SequenceTests.cs): `CreateDescendingSequence_ShouldFollowNegativeIncrement`
- [SequenceTests.cs](../../../src/code/DbSqlLikeMem.Npgsql.Test/Fidelity/DML/SequenceTests.cs): `CreateBoundedDescendingSequence_ShouldStopAtMinimumValue`
- [SequenceTests.cs](../../../src/code/DbSqlLikeMem.Npgsql.Test/Fidelity/DML/SequenceTests.cs): `DropSequenceIfExists_ShouldBeIdempotent`
- [SequenceTests.cs](../../../src/code/DbSqlLikeMem.Npgsql.Test/Fidelity/DML/SequenceTests.cs): `CurrVal_And_LastVal_ShouldFollowRestartedSequence`
- [SequenceTests.cs](../../../src/code/DbSqlLikeMem.Npgsql.Test/Fidelity/DML/SequenceTests.cs): `SetVal_WithIsCalledFalse_ShouldKeepLastValUntilNextValue`
- [SequenceTests.cs](../../../src/code/DbSqlLikeMem.Npgsql.Test/Fidelity/DML/SequenceTests.cs): `CurrVal_ShouldBeSessionLocal`
- [SequenceTests.cs](../../../src/code/DbSqlLikeMem.Npgsql.Test/Fidelity/DML/SequenceTests.cs): `LastVal_ShouldBeSessionLocal`
- [SequenceTests.cs](../../../src/code/DbSqlLikeMem.Npgsql.Test/Fidelity/DML/SequenceTests.cs): `SchemaQualifiedSequence_ShouldWork`
- [SequenceTests.cs](../../../src/code/DbSqlLikeMem.Npgsql.Test/Fidelity/DML/SequenceTests.cs): `DropSequence_ShouldRollbackWithTransaction`
- [SequenceTests.cs](../../../src/code/DbSqlLikeMem.Npgsql.Test/Fidelity/DML/SequenceTests.cs): `CreateSequenceIfNotExists_ShouldPreserveExistingSequence`
- [SequenceTests.cs](../../../src/code/DbSqlLikeMem.SqlServer.Test/Fidelity/DML/SequenceTests.cs): `SequenceExpressions_ShouldReturnExpectedValues`
- [SequenceTestsBase.cs](../../../src/code/DbSqlLikeMem.TestTools/FidelityBaseTests/DML/SequenceTestsBase.cs): `SequenceExpressionFilterTest`
- [SequenceTests.cs](../../../src/code/DbSqlLikeMem.SqlServer.Test/Fidelity/DML/SequenceTests.cs): `CurrentValue_ShouldTrackGeneratedValues`
- [SequenceTests.cs](../../../src/code/DbSqlLikeMem.SqlServer.Test/Fidelity/DML/SequenceTests.cs): `CreateSequenceIncrementBy_ShouldChangeNextGeneratedValue`
- [SequenceTests.cs](../../../src/code/DbSqlLikeMem.SqlServer.Test/Fidelity/DML/SequenceTests.cs): `CreateCycleSequence_ShouldWrapBackToMinimumValue`
- [SequenceTests.cs](../../../src/code/DbSqlLikeMem.SqlServer.Test/Fidelity/DML/SequenceTests.cs): `AlterSequenceRestartWith_ShouldResetNextGeneratedValue`
- [SequenceTests.cs](../../../src/code/DbSqlLikeMem.SqlServer.Test/Fidelity/DML/SequenceTests.cs): `DropSequenceIfExists_ShouldBeIdempotent`
- [SequenceTests.cs](../../../src/code/DbSqlLikeMem.Db2.Test/Fidelity/DML/SequenceTests.cs): `SequenceExpressions_ShouldReturnExpectedValues`
- [SequenceTests.cs](../../../src/code/DbSqlLikeMem.Db2.Test/Fidelity/DML/SequenceTests.cs): `SequenceValues_ShouldBeSessionLocal`
- [CrudTestsBase.cs](../../../src/code/DbSqlLikeMem.TestTools/FidelityBaseTests/DML/CrudTestsBase.cs): `ReturningUpdateTest`
- [MySqlTemporaryTableParserTests.cs](../../../src/code/DbSqlLikeMem.MySql.Test/TemporaryTable/MySqlTemporaryTableParserTests.cs): `ParseMulti_ShouldAccept_CreateTemporaryTable_AsSelect_FollowedBySelect`
- [MySqlTemporaryTableEngineTests.cs](../../../src/code/DbSqlLikeMem.MySql.Test/TemporaryTable/MySqlTemporaryTableEngineTests.cs): `CreateTemporaryTable_AsSelect_ThenSelect_ShouldReturnProjectedRows`

## Gaps Documentados

- Os gaps inventariados nesta rodada foram cobertos pelos testes criados acima.
- O alias de benchmark `ReturningUpdate` agora tem uma correspondencia explicita em `ReturningUpdateTest`, mesmo compartilhando o mesmo fluxo de update por chave de `UpdateByPkTest`.
- Novos gaps devem ser registrados em rodadas futuras quando surgirem funcionalidades reais ainda sem cobertura.

## Validacao Da Cobertura De Benchmarks

- O projeto `src\benchmark\DbSqlLikeMem.Benchmarks` ja cobre por alias ou correspondencia direta os fluxos centrais de DML, batch, query, JSON, temporal, typed field e sequencia basica.
- Entradas como `CreateTableWithFKInsert`, `InsertInTableWithFK`, `InsertDefaultColumns`, `InsertNullableColumns`, `InsertNotNullWithoutDefault`, `CheckConstraintsValidInsert`, `CheckConstraintsInvalidInsert`, `CheckConstraintsInvalidUpdate`, `ParameterTypeMatrix`, `ParameterDateCurrencyMatrix`, `TypedFieldCastCalculationMatrix`, `TypedFieldNullComparisonMatrix`, `TypedFieldTextLengthMatrix`, `TypedFieldTextCaseMatrix`, `ReturningUpdate`, `BatchReturningInsert`, `BatchRowCountInBatch` e `MergeBasic` ja aparecem no catalogo de benchmark.
- As variacoes extras de sequencia acima de `SequenceNextValue` agora tambem possuem entradas dedicadas no benchmark, incluindo `SequenceCurrentValue`, `SequenceInsertRoundTrip`, `SequenceInsertExpression`, `SequenceSelectProjection`, `SequenceExpressionFilter`, `SequenceCaseWhereMatrix`, `SequenceTemporalMatrix` e `SequenceJoinAggregate`.
- As funcoes tabulares de JSON agora tambem possuem entradas proprias no benchmark, incluindo `JsonEachFromArray`, `JsonEachFromObject`, `JsonTreeStructure` e `OpenJsonArray`.
- Os casos temporais especializados agora tambem possuem entradas proprias no benchmark, incluindo `TemporalDateTrunc`, `TemporalTimeZoneOffset`, `TemporalFromParts`, `TemporalEndOfMonth` e `TemporalDateDiffBig`.
- Os calculos matematicos compartilhados agora tambem possuem entradas proprias no benchmark, incluindo `MathFunctions`, `MathLogBaseFunction`, `MathLog2Function`, `MathPiFunction`, `MathRandFunction`, `MathRemainderFunction`, `MathTruncFunction` e `MathCotFunction`.
- Os aliases e extensoes matematicas do provedor agora tambem possuem entradas proprias no benchmark, incluindo `MySqlUtilityMathFunctions`, `GreatestLeastModFunctions`, `Db2AliasMathFunctions`, `FirebirdAliasMathFunctions` e `MathTranscendentalFunctions`.
- Os casos escalares de JSON agora tambem possuem entradas proprias no benchmark, incluindo `JsonMissingPathRead`, `JsonMissingPathReturnsNull`, `JsonQueryRootFragment`, `JsonModifyReplace`, `JsonInsertCast` e `JsonInsertCastReturnsNull`.
- As agregacoes de strings com resumo, agrupamento e variantes agora tambem possuem entradas proprias no benchmark, incluindo `StringAggregateSummaryMatrix`, `StringAggregateGroupCaseMatrix`, `StringAggregationSummaryMatrix`, `StringAggregationGroupCaseMatrix` e `StringAggregationVariants`.
- A matriz temporal escalar agora tambem possui entrada propria no benchmark, incluindo `ScalarTemporalMatrix`.
- As funcoes escalares de string do SQL Server agora tambem possuem entradas proprias no benchmark, incluindo `StringEscape`, `Translate`, `FormatMessage`, `IsJson`, `Format`, `StringUtilityFunctions` e `StringMetadataFunctions`.
- Os testes especificos de SQL Server em `FieldTypeFunctionTestsBase` agora tambem possuem benchmark dedicado, como `SqlServerMetadataFunctions`, `ScopeIdentity`, `SqlServerSystemFunctions`, `SqlServerSpecialFunctions`, `SqlServerContextFunctions`, `SqlServerTransactionStateFunctions`, `SqlServerSessionFunctions`, `StringBasicFunctions`, `ParseFamily`, `Soundex`, `Compression`, `ApproxCountDistinct`, `PercentileAggregateFunctions` e `SqlServerAggregateFunctions`.
- As projecoes relacionais agora tambem possuem entradas proprias no benchmark, incluindo `StringSplitProjection`, `ForJsonPathProjection`, `JoinTemporalMatrix`, `JoinWindowMatrix`, `JoinWindowTemporalMatrix` e `JoinWindowAggregateTemporalMatrix`.
- Os compostos de APPLY agora tambem possuem entradas proprias no benchmark, incluindo `ApplyTemporalComposite` e `ApplyWindowTemporalComposite`.
- O composto relacional agora tambem possui entrada propria no benchmark, incluindo `RelationalComposite`.
- Os fluxos de update/delete e de insert round-trip com parametros agora tambem possuem entradas proprias no benchmark, incluindo `ParameterUpdateDeleteRoundTrip`, `ParameterInsertRoundTrip` e `ParameterInsertNullRoundTrip`.
- Os fluxos de insert-then-update para merge e upsert agora tambem possuem entradas proprias no benchmark, incluindo `MergeInsertThenUpdate` e `UpsertInsertThenUpdate`.
- Os testes `SelectJoinCount`, `SelectApplyProjection`, `SelectWindowFunctions`, `SelectScalarSubqueryCaseMatrix`, `SelectRangeAndPivot` e os predicados `SelectInListPredicate`, `SelectBetweenPredicate`, `SelectLikePredicate`, `SelectNotLikePredicate`, `SelectNotEqualPredicate`, `SelectEqualPredicate`, `SelectGreaterThanPredicate`, `SelectLessThanPredicate`, `SelectGreaterThanOrEqualPredicate`, `SelectLessThanOrEqualPredicate` e `SelectNotInSubqueryNull` agora tambem possuem entradas proprias no benchmark.
- As consultas `SelectAllRowsCount`, `SelectAllRowsSnapshot`, `SelectCteMaterializedHint`, `SelectDistinctOnProjection`, `SelectOrderByName`, `SelectOrderByOrdinal`, `SelectOrderByNameDescending`, `SelectNamePaginationMatrix`, `SelectGroupByNameInitialMatrix`, `SelectGroupByNameHaving`, `SelectGroupByOrdinal`, `SelectDistinctOrderByOrdinal`, `SelectDistinctLikeOrderByOrdinal`, `SelectJoinTypedExpressionMatrix`, `SelectJoinNullAggregateMatrix`, `SelectJoinCastNullMatrix`, `SelectJoinCastTextComparisonMatrix`, `SelectJoinHavingCastMatrix`, `SelectJoinLengthNumericMatrix`, `SelectJoinTextCaseLengthMatrix`, `SelectJoinDistinctCaseMatrix` e `SelectJoinDistinctHavingMatrix` agora tambem possuem entradas proprias no benchmark.
- A varredura atual de `SelectTestsBase` agora fecha com zero lacunas restantes no projeto `src\benchmark\DbSqlLikeMem.Benchmarks`.
- A varredura ampla dos testes base agora fecha com zero lacunas restantes no projeto `src\benchmark\DbSqlLikeMem.Benchmarks`.

## Próximos Passos

- Inventariar mais funcionalidades faltantes por provider.
- Criar o próximo teste de fidelidade para a funcionalidade real ainda não coberta.
- Documentar o gap e o ponto de entrada do teste no acompanhamento da fase.
