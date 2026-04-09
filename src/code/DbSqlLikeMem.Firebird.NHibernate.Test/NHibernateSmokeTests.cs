namespace DbSqlLikeMem.Firebird.NHibernate.Test;

/// <summary>
/// EN: Runs smoke tests for NHibernate integration using the Firebird in-memory mock provider.
/// PT: Executa testes de fumaça para integração do NHibernate usando o provedor simulado em memória do Firebird.
/// </summary>
public sealed class NHibernateSmokeTests(
    ITestOutputHelper helper
) : NHibernateSupportTestsBase(helper)
{
    /// <inheritdoc />
    protected override string NhDialectClass => "NHibernate.Dialect.FirebirdDialect, NHibernate";

    /// <inheritdoc />
    protected override string NhDriverClass => typeof(FirebirdNhMockDriver).AssemblyQualifiedName!;

    /// <inheritdoc />
    protected override bool UseInMemoryPaginationFallback => true;

    /// <inheritdoc />
    protected override DbConnection CreateOpenConnection()
    {
        var connection = new FirebirdConnectionMock();
        connection.Open();
        return connection;
    }
}
