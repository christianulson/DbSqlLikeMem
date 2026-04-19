# Fase 8 - Testes faltantes do banco real

## Status

IN PROGRESS

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
- [SequenceTests.cs](../../../src/code/DbSqlLikeMem.SqlServer.Test/Fidelity/DML/SequenceTests.cs): `CurrentValue_ShouldTrackGeneratedValues`
- [SequenceTests.cs](../../../src/code/DbSqlLikeMem.SqlServer.Test/Fidelity/DML/SequenceTests.cs): `CreateSequenceIncrementBy_ShouldChangeNextGeneratedValue`
- [SequenceTests.cs](../../../src/code/DbSqlLikeMem.SqlServer.Test/Fidelity/DML/SequenceTests.cs): `CreateCycleSequence_ShouldWrapBackToMinimumValue`
- [SequenceTests.cs](../../../src/code/DbSqlLikeMem.SqlServer.Test/Fidelity/DML/SequenceTests.cs): `AlterSequenceRestartWith_ShouldResetNextGeneratedValue`
- [SequenceTests.cs](../../../src/code/DbSqlLikeMem.SqlServer.Test/Fidelity/DML/SequenceTests.cs): `DropSequenceIfExists_ShouldBeIdempotent`
- [SequenceTests.cs](../../../src/code/DbSqlLikeMem.Db2.Test/Fidelity/DML/SequenceTests.cs): `SequenceExpressions_ShouldReturnExpectedValues`
- [SequenceTests.cs](../../../src/code/DbSqlLikeMem.Db2.Test/Fidelity/DML/SequenceTests.cs): `SequenceValues_ShouldBeSessionLocal`

## Gaps Documentados

- Cobertura inicial de `ALTER SEQUENCE ... RESTART WITH` para PostgreSQL/Npgsql.
- Cobertura de `ALTER SEQUENCE ... INCREMENT BY` para PostgreSQL/Npgsql.
- Cobertura de `ALTER SEQUENCE ... OWNED BY NONE` para PostgreSQL/Npgsql.
- Cobertura de `ALTER SEQUENCE ... OWNED BY` para PostgreSQL/Npgsql.
- Cobertura de `CREATE SEQUENCE ... CYCLE` para PostgreSQL/Npgsql.
- Cobertura de `CREATE SEQUENCE ... MAXVALUE` para PostgreSQL/Npgsql.
- Cobertura de `CREATE SEQUENCE ... INCREMENT BY -2` para PostgreSQL/Npgsql.
- Cobertura de `CREATE SEQUENCE ... MINVALUE` para PostgreSQL/Npgsql.
- Cobertura de `DROP SEQUENCE IF EXISTS` para PostgreSQL/Npgsql.
- Cobertura de `currval` e `lastval` após restart de sequence no PostgreSQL/Npgsql.
- Cobertura de `setval(..., false)` e `lastval` estável no PostgreSQL/Npgsql.
- Cobertura de `currval` e `lastval` locais à sessão no PostgreSQL/Npgsql.
- Cobertura de sequence qualificada por schema no PostgreSQL/Npgsql.
- Cobertura de `DROP SEQUENCE` transacional com rollback no PostgreSQL/Npgsql.
- Cobertura de `CREATE SEQUENCE IF NOT EXISTS` idempotente no PostgreSQL/Npgsql.
- Cobertura de `NEXT VALUE FOR ... FROM Users WHERE ...` para SQL Server.
- Cobertura de `sys.sequences.current_value` para SQL Server.
- Cobertura de `CREATE SEQUENCE ... INCREMENT BY` para SQL Server.
- Cobertura de `CREATE SEQUENCE ... CYCLE` para SQL Server.
- Cobertura de `ALTER SEQUENCE ... RESTART WITH` para SQL Server.
- Cobertura de `DROP SEQUENCE IF EXISTS` idempotente no SQL Server.
- Cobertura de `NEXT VALUE FOR` e `PREVIOUS VALUE FOR` locais à sessão no DB2.
- Cobertura de `NEXT VALUE FOR ... FROM Users WHERE ...` para DB2.

## Próximos Passos

- Inventariar mais funcionalidades faltantes por provider.
- Criar o próximo teste de fidelidade para a funcionalidade real ainda não coberta.
- Documentar o gap e o ponto de entrada do teste no acompanhamento da fase.
