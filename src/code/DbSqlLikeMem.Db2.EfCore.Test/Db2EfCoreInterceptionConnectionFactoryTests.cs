namespace DbSqlLikeMem.Db2.EfCore.Test;

/// <summary>
/// EN: Validates interception-capable Db2 EF Core connection factories.
/// PT: Valida factories de conexao Db2 para EF Core com suporte a interceptacao.
/// </summary>
public sealed class Db2EfCoreInterceptionConnectionFactoryTests(
        ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Verifies the interceptor-aware constructor returns an opened wrapped connection.
    /// PT: Verifica se o construtor com interceptacao retorna uma conexao encapsulada ja aberta.
    /// </summary>
    [Fact]
    public void ConstructorWithInterceptors_ShouldReturnOpenedWrappedConnection()
    {
        var recorder = new RecordingDbConnectionInterceptor();
        var factory = new Db2EfCoreConnectionFactory(recorder);

        using var connection = factory.CreateOpenConnection();

        connection.Should().BeOfType<InterceptingDbConnection>();
        connection.State.Should().Be(ConnectionState.Open);
        recorder.Events.Should().Contain(x => x.EventKind == DbInterceptionEventKind.ConnectionOpened);
    }
}
