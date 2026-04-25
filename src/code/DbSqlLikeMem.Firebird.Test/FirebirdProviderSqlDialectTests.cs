namespace DbSqlLikeMem.Firebird.Test;

/// <summary>
/// EN: Contains tests for the Firebird provider SQL dialect helper.
/// PT: Contém testes para o helper de dialeto SQL do provedor Firebird.
/// </summary>
public sealed class FirebirdProviderSqlDialectTests
{
    /// <summary>
    /// EN: Verifies that the provider metadata matches the Firebird helper contract.
    /// PT: Verifica se os metadados do provedor correspondem ao contrato do helper Firebird.
    /// </summary>
    [Fact]
    public void ProviderMetadata_ShouldMatchFirebird()
    {
        var dialect = new FirebirdProviderSqlDialect();

        Assert.Equal(ProviderId.Firebird, dialect.Provider);
        Assert.Equal("Firebird", dialect.DisplayName);
        Assert.True(dialect.SupportsUpsert);
        Assert.True(dialect.SupportsSequence);
        Assert.False(dialect.SupportsJsonScalarRead);
        Assert.True(dialect.SupportsReleaseSavepoints);
        Assert.False(dialect.SupportsDateTimeOffsetInputOutputParameters);
        Assert.False(dialect.SupportsGuidInputOutputParameters);
    }

    /// <summary>
    /// EN: Verifies that the helper emits Firebird-compatible table and sequence SQL.
    /// PT: Verifica se o helper emite SQL compatível com Firebird para tabela e sequence.
    /// </summary>
    [Fact]
    public void ProviderSql_ShouldUseFirebirdSyntax()
    {
        var dialect = new FirebirdProviderSqlDialect();

        var context = new FidelityTestContext();

        var usersTable = dialect.CreateUsersTable(context);
        var tempTable = dialect.CreateTemporaryUsersTable(context);
        var sequenceSql = dialect.NextSequenceValue(context);
        var sequenceSelectSql = dialect.SelectNextSequenceValue(context);
        var batchInsertSql = dialect.InsertUsers(context, (1, "Ana"), (2, "Beto"));
        var parameterProjectionSql = dialect.SelectParameterProjection("""
    @dateValue AS DateValue,
    @currencyValue AS CurrencyValue,
    @textValue AS TextValue,
    @dateTimeOffsetValue AS DateTimeOffsetValue,
    @binaryValue AS BinaryValue
""");

        Assert.Contains($"CREATE TABLE {context.TbUsersFullName}", usersTable);
        Assert.Contains("BLOB SUB_TYPE TEXT", usersTable);
        Assert.Contains($"CREATE GLOBAL TEMPORARY TABLE {context.TempTbFullName}", tempTable);
        Assert.Contains($"NEXT VALUE FOR {context.Seq}", sequenceSql);
        Assert.Contains("FROM RDB$DATABASE", sequenceSelectSql);
        Assert.Contains($"INSERT INTO {context.TbUsersFullName} (Id, Name, IsActive, Balance, CreatedAt)", batchInsertSql);
        Assert.Contains("FROM (", batchInsertSql);
        Assert.Contains("UNION ALL SELECT 2 AS counter FROM RDB$DATABASE", batchInsertSql);
        Assert.Contains("User-' || counter", batchInsertSql);
        Assert.Contains("CAST(@dateValue AS DATE) AS DateValue", parameterProjectionSql);
        Assert.Contains("CAST(@currencyValue AS DECIMAL(19,2)) AS CurrencyValue", parameterProjectionSql);
        Assert.Contains("CAST(@textValue AS VARCHAR(100)) AS TextValue", parameterProjectionSql);
        Assert.Contains("CAST(@dateTimeOffsetValue AS VARCHAR(40)) AS DateTimeOffsetValue", parameterProjectionSql);
        Assert.Contains("CAST(@binaryValue AS VARBINARY(16)) AS BinaryValue", parameterProjectionSql);
    }
}
