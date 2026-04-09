namespace DbSqlLikeMem.MySql.EfCore.Test;

/// <summary>
/// EN: Validates interception-capable MySql EF Core connection factories.
/// PT: Valida factories de conexao MySql para EF Core com suporte a interceptacao.
/// </summary>
public sealed class MySqlEfCoreInterceptionConnectionFactoryTests
{
    /// <summary>
    /// EN: Verifies the interceptor-aware constructor returns an opened wrapped connection.
    /// PT: Verifica se o construtor com interceptacao retorna uma conexao encapsulada ja aberta.
    /// </summary>
    [Fact]
    public void ConstructorWithInterceptors_ShouldReturnOpenedWrappedConnection()
    {
        var recorder = new RecordingDbConnectionInterceptor();
        var factory = new MySqlEfCoreConnectionFactory(recorder);

        using var connection = factory.CreateOpenConnection();

        Assert.IsType<InterceptingDbConnection>(connection);
        Assert.Equal(ConnectionState.Open, connection.State);
        Assert.Contains(recorder.Events, x => x.EventKind == DbInterceptionEventKind.ConnectionOpened);
    }
}
