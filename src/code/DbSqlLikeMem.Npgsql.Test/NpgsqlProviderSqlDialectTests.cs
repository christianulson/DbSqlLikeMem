using DbSqlLikeMem.Npgsql.TestTools;

namespace DbSqlLikeMem.Npgsql.Test;

/// <summary>
/// EN: Contains tests for the PostgreSQL provider SQL dialect helper.
/// PT-br: Contem testes para o helper de dialeto SQL do provedor PostgreSQL.
/// </summary>
public sealed class NpgsqlProviderSqlDialectTests
{
    /// <summary>
    /// EN: Verifies that the provider metadata matches the PostgreSQL helper contract.
    /// PT-br: Verifica se os metadados do provedor correspondem ao contrato do helper PostgreSQL.
    /// </summary>
    [Fact]
    public void ProviderMetadata_ShouldMatchNpgsql()
    {
        var dialect = new NpgsqlProviderSqlDialect();

        Assert.Equal(ProviderId.Npgsql, dialect.Provider);
        Assert.Equal("PostgreSQL / Npgsql", dialect.DisplayName);
        Assert.True(dialect.SupportsUpsert);
        Assert.True(dialect.SupportsSequence);
        Assert.True(dialect.SupportsReleaseSavepoints);
        Assert.True(dialect.SupportsJsonScalarRead);
        Assert.True(dialect.SupportsUpdateDeleteJoinRuntime);
    }

    /// <summary>
    /// EN: Verifies that the helper emits PostgreSQL-compatible savepoint SQL.
    /// PT-br: Verifica se o helper emite SQL compativel com PostgreSQL para savepoint.
    /// </summary>
    [Fact]
    public void SavepointSql_ShouldUsePostgreSqlSyntax()
    {
        var dialect = new NpgsqlProviderSqlDialect();

        Assert.Equal("SAVEPOINT sp_1", dialect.Savepoint("sp_1"));
        Assert.Equal("ROLLBACK TO SAVEPOINT sp_1", dialect.RollbackToSavepoint("sp_1"));
        Assert.Equal("RELEASE SAVEPOINT sp_1", dialect.ReleaseSavepoint("sp_1"));
    }
}
