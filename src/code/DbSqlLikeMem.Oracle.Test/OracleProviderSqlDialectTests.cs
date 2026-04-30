using DbSqlLikeMem.Oracle.TestTools;

namespace DbSqlLikeMem.Oracle.Test;

/// <summary>
/// EN: Contains tests for the Oracle provider SQL dialect helper.
/// PT: Contém testes para o helper de dialeto SQL do provedor Oracle.
/// </summary>
public sealed class OracleProviderSqlDialectTests
{
    /// <summary>
    /// EN: Verifies that the provider metadata matches the Oracle helper contract.
    /// PT: Verifica se os metadados do provedor correspondem ao contrato do helper Oracle.
    /// </summary>
    [Fact]
    public void ProviderMetadata_ShouldMatchOracle()
    {
        var dialect = new OracleProviderSqlDialect();

        Assert.Equal(ProviderId.Oracle, dialect.Provider);
        Assert.Equal("Oracle", dialect.DisplayName);
        Assert.True(dialect.SupportsUpsert);
        Assert.True(dialect.SupportsSequence);
        Assert.True(dialect.SupportsJsonScalarRead);
        Assert.False(dialect.SupportsReleaseSavepoints);
        Assert.False(dialect.SupportsGuidInputOutputParameters);
    }

    /// <summary>
    /// EN: Verifies that the helper emits Oracle-compatible savepoint SQL.
    /// PT: Verifica se o helper emite SQL compatível com Oracle para savepoint.
    /// </summary>
    [Fact]
    public void ReleaseSavepoint_ShouldUseOracleSyntax()
    {
        var dialect = new OracleProviderSqlDialect();

        var sql = dialect.ReleaseSavepoint("sp_1");

        Assert.Equal("RELEASE SAVEPOINT sp_1", sql);
    }
}
