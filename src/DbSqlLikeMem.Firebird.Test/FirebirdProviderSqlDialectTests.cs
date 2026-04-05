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

        var usersTable = dialect.CreateUsersTable("Users", "ABCD1234");
        var tempTable = dialect.CreateTemporaryUsersTable("Users");
        var sequenceSql = dialect.NextSequenceValue("SEQ_USERS");

        Assert.Contains("CREATE TABLE Users_ABCD1234", usersTable);
        Assert.Contains("BLOB SUB_TYPE TEXT", usersTable);
        Assert.Contains("CREATE GLOBAL TEMPORARY TABLE Users", tempTable);
        Assert.Contains("NEXT VALUE FOR SEQ_USERS", sequenceSql);
    }
}
