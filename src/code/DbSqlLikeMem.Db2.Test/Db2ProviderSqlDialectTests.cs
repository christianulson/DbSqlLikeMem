using DbSqlLikeMem.Db2.TestTools;

namespace DbSqlLikeMem.Db2.Test;

/// <summary>
/// EN: Contains tests for the DB2 provider SQL dialect helper.
/// PT: Contem testes para o helper de dialeto SQL do provedor DB2.
/// </summary>
public sealed class Db2ProviderSqlDialectTests
{
    /// <summary>
    /// EN: Verifies that the provider metadata matches the DB2 helper contract.
    /// PT: Verifica se os metadados do provedor correspondem ao contrato do helper DB2.
    /// </summary>
    [Fact]
    public void ProviderMetadata_ShouldMatchDb2()
    {
        var dialect = new Db2ProviderSqlDialect();

        Assert.Equal(ProviderId.Db2, dialect.Provider);
        Assert.Equal("DB2", dialect.DisplayName);
        Assert.True(dialect.SupportsUpsert);
        Assert.True(dialect.SupportsSequence);
        Assert.True(dialect.SupportsReleaseSavepoints);
        Assert.True(dialect.SupportsJsonScalarRead);
        Assert.False(dialect.SupportsGuidInputOutputParameters);
    }

    /// <summary>
    /// EN: Verifies that the helper emits DB2-compatible savepoint SQL.
    /// PT: Verifica se o helper emite SQL compativel com DB2 para savepoint.
    /// </summary>
    [Fact]
    public void SavepointSql_ShouldUseDb2Syntax()
    {
        var dialect = new Db2ProviderSqlDialect();

        Assert.Equal("SAVEPOINT sp_1 ON ROLLBACK RETAIN CURSORS", dialect.Savepoint("sp_1"));
        Assert.Equal("ROLLBACK TO SAVEPOINT sp_1", dialect.RollbackToSavepoint("sp_1"));
        Assert.Equal("RELEASE SAVEPOINT sp_1", dialect.ReleaseSavepoint("sp_1"));
    }
}
