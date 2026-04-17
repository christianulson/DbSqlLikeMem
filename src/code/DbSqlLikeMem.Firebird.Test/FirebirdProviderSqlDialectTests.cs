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

        Assert.Contains("CREATE TABLE Users_ABCD1234", usersTable);
        Assert.Contains("BLOB SUB_TYPE TEXT", usersTable);
        Assert.Contains("CREATE GLOBAL TEMPORARY TABLE Users", tempTable);
        Assert.Contains("NEXT VALUE FOR SEQ_USERS", sequenceSql);
        Assert.Contains("FROM RDB$DATABASE", sequenceSelectSql);
        Assert.Contains("INSERT INTO Users (Id, Name, IsActive, Balance, CreatedAt)", batchInsertSql);
        Assert.Contains("FROM (", batchInsertSql);
        Assert.Contains("UNION ALL SELECT 2 AS counter FROM RDB$DATABASE", batchInsertSql);
        Assert.Contains("User-' || counter", batchInsertSql);
    }
}
