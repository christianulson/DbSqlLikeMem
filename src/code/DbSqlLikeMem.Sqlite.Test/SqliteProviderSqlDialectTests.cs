using DbSqlLikeMem.Sqlite.TestTools;

namespace DbSqlLikeMem.Sqlite.Test;

/// <summary>
/// EN: Contains tests for the SQLite provider SQL dialect helper.
/// PT: Contem testes para o helper de dialeto SQL do provedor SQLite.
/// </summary>
public sealed class SqliteProviderSqlDialectTests
{
    /// <summary>
    /// EN: Verifies that the provider metadata matches the SQLite helper contract.
    /// PT: Verifica se os metadados do provedor correspondem ao contrato do helper SQLite.
    /// </summary>
    [Fact]
    public void ProviderMetadata_ShouldMatchSqlite()
    {
        var dialect = new SqliteProviderSqlDialect();

        Assert.Equal(ProviderId.Sqlite, dialect.Provider);
        Assert.Equal("SQLite", dialect.DisplayName);
        Assert.True(dialect.SupportsUpsert);
        Assert.True(dialect.SupportsReleaseSavepoints);
        Assert.True(dialect.SupportsJsonScalarRead);
        Assert.True(dialect.SupportsJsonTableFunctions);
        Assert.False(dialect.SupportsOuterApplyProjection);
    }

    /// <summary>
    /// EN: Verifies that the helper emits SQLite-compatible savepoint SQL.
    /// PT: Verifica se o helper emite SQL compativel com SQLite para savepoint.
    /// </summary>
    [Fact]
    public void SavepointSql_ShouldUseSqliteSyntax()
    {
        var dialect = new SqliteProviderSqlDialect();

        Assert.Equal("SAVEPOINT sp_1", dialect.Savepoint("sp_1"));
        Assert.Equal("ROLLBACK TO SAVEPOINT sp_1", dialect.RollbackToSavepoint("sp_1"));
        Assert.Equal("RELEASE SAVEPOINT sp_1", dialect.ReleaseSavepoint("sp_1"));
    }
}
