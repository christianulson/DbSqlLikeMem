using DbSqlLikeMem.MySql.TestTools;

namespace DbSqlLikeMem.MySql.Test;

/// <summary>
/// EN: Contains tests for the MySQL provider SQL dialect helper.
/// PT: Contem testes para o helper de dialeto SQL do provedor MySQL.
/// </summary>
public sealed class MySqlProviderSqlDialectTests
{
    /// <summary>
    /// EN: Verifies that the provider metadata matches the MySQL helper contract.
    /// PT: Verifica se os metadados do provedor correspondem ao contrato do helper MySQL.
    /// </summary>
    [Fact]
    public void ProviderMetadata_ShouldMatchMySql()
    {
        var dialect = new MySqlProviderSqlDialect();

        Assert.Equal(ProviderId.MySql, dialect.Provider);
        Assert.Equal(nameof(ProviderId.MySql), dialect.DisplayName);
        Assert.True(dialect.SupportsUpsert);
        Assert.True(dialect.SupportsReleaseSavepoints);
        Assert.True(dialect.SupportsJsonScalarRead);
        Assert.True(dialect.SupportsUpdateDeleteJoinRuntime);
    }

    /// <summary>
    /// EN: Verifies that the helper emits MySQL-compatible savepoint SQL.
    /// PT: Verifica se o helper emite SQL compativel com MySQL para savepoint.
    /// </summary>
    [Fact]
    public void SavepointSql_ShouldUseMySqlSyntax()
    {
        var dialect = new MySqlProviderSqlDialect();

        Assert.Equal("SAVEPOINT sp_1", dialect.Savepoint("sp_1"));
        Assert.Equal("ROLLBACK TO SAVEPOINT sp_1", dialect.RollbackToSavepoint("sp_1"));
        Assert.Equal("RELEASE SAVEPOINT sp_1", dialect.ReleaseSavepoint("sp_1"));
    }

}
