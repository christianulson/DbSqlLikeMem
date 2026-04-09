namespace DbSqlLikeMem.Oracle.EfCore.Test;

/// <summary>
/// EN: Validates interception-capable Oracle EF Core connection factories.
/// PT: Valida factories de conexao Oracle para EF Core com suporte a interceptacao.
/// </summary>
public sealed class OracleEfCoreInterceptionConnectionFactoryTests
{
    /// <summary>
    /// EN: Verifies the interceptor-aware constructor returns an opened wrapped connection.
    /// PT: Verifica se o construtor com interceptacao retorna uma conexao encapsulada ja aberta.
    /// </summary>
    [Fact]
    public void ConstructorWithInterceptors_ShouldReturnOpenedWrappedConnection()
    {
        var recorder = new RecordingDbConnectionInterceptor();
        var factory = new OracleEfCoreConnectionFactory(recorder);

        using var connection = factory.CreateOpenConnection();

        Assert.IsType<InterceptingDbConnection>(connection);
        Assert.Equal(ConnectionState.Open, connection.State);
        Assert.Contains(recorder.Events, x => x.EventKind == DbInterceptionEventKind.ConnectionOpened);
    }
}
