using DbSqlLikeMem.SqlAzure.TestTools;

namespace DbSqlLikeMem.SqlAzure.Test;

/// <summary>
/// EN: Contains tests for the SQL Azure provider SQL dialect helper.
/// PT-br: Contem testes para o helper de dialeto SQL do provedor SQL Azure.
/// </summary>
public sealed class SqlAzureProviderSqlDialectTests
{
    /// <summary>
    /// EN: Verifies that the provider metadata matches the SQL Azure helper contract.
    /// PT-br: Verifica se os metadados do provedor correspondem ao contrato do helper SQL Azure.
    /// </summary>
    [Fact]
    public void ProviderMetadata_ShouldMatchSqlAzure()
    {
        var dialect = new SqlAzureProviderSqlDialect();

        Assert.Equal(ProviderId.SqlAzure, dialect.Provider);
        Assert.Equal("SQL Azure", dialect.DisplayName);
        Assert.True(dialect.SupportsUpsert);
        Assert.True(dialect.SupportsSequence);
        Assert.False(dialect.SupportsReleaseSavepoints);
        Assert.True(dialect.SupportsJsonScalarRead);
        Assert.True(dialect.SupportsUpdateDeleteJoinRuntime);
    }

    /// <summary>
    /// EN: Verifies that the helper emits SQL Azure-compatible savepoint SQL.
    /// PT-br: Verifica se o helper emite SQL compativel com SQL Azure para savepoint.
    /// </summary>
    [Fact]
    public void SavepointSql_ShouldUseSqlAzureSyntax()
    {
        var dialect = new SqlAzureProviderSqlDialect();

        Assert.Equal("SAVE TRANSACTION sp_1", dialect.Savepoint("sp_1"));
        Assert.Equal("ROLLBACK TRANSACTION sp_1", dialect.RollbackToSavepoint("sp_1"));
        Assert.Equal("SELECT 1", dialect.ReleaseSavepoint("sp_1"));
    }
}
