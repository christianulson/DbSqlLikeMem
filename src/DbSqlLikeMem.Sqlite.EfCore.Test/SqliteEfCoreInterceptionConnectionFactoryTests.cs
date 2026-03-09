namespace DbSqlLikeMem.Sqlite.EfCore.Test;

/// <summary>
/// EN: Validates interception-capable Sqlite EF Core connection factories.
/// PT: Valida factories de conexao Sqlite para EF Core com suporte a interceptacao.
/// </summary>
public sealed class SqliteEfCoreInterceptionConnectionFactoryTests
{
    /// <summary>
    /// EN: Verifies the interceptor-aware constructor returns an opened wrapped connection.
    /// PT: Verifica se o construtor com interceptacao retorna uma conexao encapsulada ja aberta.
    /// </summary>
    [Fact]
    public void ConstructorWithInterceptors_ShouldReturnOpenedWrappedConnection()
    {
        var recorder = new RecordingDbConnectionInterceptor();
        var factory = new SqliteEfCoreConnectionFactory(recorder);

        using var connection = factory.CreateOpenConnection();

        Assert.IsType<InterceptingDbConnection>(connection);
        Assert.Equal(ConnectionState.Open, connection.State);
        Assert.Contains(recorder.Events, x => x.EventKind == DbInterceptionEventKind.ConnectionOpened);
    }
}
