using DbSqlLikeMem.MariaDb.TestTools;

namespace DbSqlLikeMem.MariaDb.Test;

/// <summary>
/// EN: Contains tests for the MariaDB provider SQL dialect helper.
/// PT-br: Contém testes para o helper de dialeto SQL do provedor MariaDB.
/// </summary>
public sealed class MariaDbProviderSqlDialectTests
{
    /// <summary>
    /// EN: Verifies that the provider metadata matches the MariaDB helper contract.
    /// PT-br: Verifica se os metadados do provedor correspondem ao contrato do helper MariaDB.
    /// </summary>
    [Fact]
    public void ProviderMetadata_ShouldMatchMariaDb()
    {
        var dialect = new MariaDbProviderSqlDialect();

        Assert.Equal(ProviderId.MariaDb, dialect.Provider);
        Assert.Equal("MariaDB", dialect.DisplayName);
        Assert.True(dialect.SupportsUpsert);
        Assert.True(dialect.SupportsSequence);
        Assert.True(dialect.SupportsJsonScalarRead);
        Assert.True(dialect.SupportsReleaseSavepoints);
        Assert.True(dialect.SupportsInsertReturning);
    }

    /// <summary>
    /// EN: Verifies that the helper emits MariaDB-compatible savepoint SQL.
    /// PT-br: Verifica se o helper emite SQL compatível com MariaDB para savepoint.
    /// </summary>
    [Fact]
    public void SavepointSql_ShouldUseMariaDbSyntax()
    {
        var dialect = new MariaDbProviderSqlDialect();

        Assert.Equal("SAVEPOINT sp_1", dialect.Savepoint("sp_1"));
        Assert.Equal("ROLLBACK TO SAVEPOINT sp_1", dialect.RollbackToSavepoint("sp_1"));
        Assert.Equal("RELEASE SAVEPOINT sp_1", dialect.ReleaseSavepoint("sp_1"));
    }
}
