using DbSqlLikeMem.SqlServer.TestTools;

namespace DbSqlLikeMem.SqlServer.Test;

/// <summary>
/// EN: Contains tests for the SQL Server provider SQL dialect helper.
/// PT-br: Contém testes para o helper de dialeto SQL do provedor SQL Server.
/// </summary>
public sealed class SqlServerProviderSqlDialectTests
{
    /// <summary>
    /// EN: Verifies that the provider metadata matches the SQL Server helper contract.
    /// PT-br: Verifica se os metadados do provedor correspondem ao contrato do helper SQL Server.
    /// </summary>
    [Fact]
    public void ProviderMetadata_ShouldMatchSqlServer()
    {
        var dialect = new SqlServerProviderSqlDialect();

        Assert.Equal(ProviderId.SqlServer, dialect.Provider);
        Assert.Equal("SQL Server", dialect.DisplayName);
        Assert.True(dialect.SupportsUpsert);
        Assert.True(dialect.SupportsSequence);
        Assert.False(dialect.SupportsReleaseSavepoints);
        Assert.True(dialect.SupportsJsonScalarRead);
    }

    /// <summary>
    /// EN: Verifies that the helper emits SQL Server-compatible savepoint SQL.
    /// PT-br: Verifica se o helper emite SQL compatível com SQL Server para savepoint.
    /// </summary>
    [Fact]
    public void ReleaseSavepoint_ShouldUseSqlServerSyntax()
    {
        var dialect = new SqlServerProviderSqlDialect();

        Assert.Equal("SAVE TRANSACTION sp_1", dialect.Savepoint("sp_1"));
        Assert.Equal("ROLLBACK TRANSACTION sp_1", dialect.RollbackToSavepoint("sp_1"));
        Assert.Equal("SELECT 1", dialect.ReleaseSavepoint("sp_1"));
    }
}
