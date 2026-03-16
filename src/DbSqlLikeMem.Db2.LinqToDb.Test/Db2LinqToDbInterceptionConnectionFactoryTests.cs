namespace DbSqlLikeMem.Db2.LinqToDb.Test;

/// <summary>
/// EN: Validates interception-capable Db2 LinqToDB connection factories.
/// PT: Valida factories de conexao Db2 para LinqToDB com suporte a interceptacao.
/// </summary>
public sealed class Db2LinqToDbInterceptionConnectionFactoryTests(
        ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Verifies the options-based constructor returns an opened wrapped connection.
    /// PT: Verifica se o construtor baseado em opcoes retorna uma conexao encapsulada ja aberta.
    /// </summary>
    [Fact]
    public void ConstructorWithOptions_ShouldReturnOpenedWrappedConnection()
    {
        var recorder = new RecordingDbConnectionInterceptor();
        var factory = new Db2LinqToDbConnectionFactory(new DbInterceptionOptions
        {
            EnableRecording = true,
            RecordingInterceptor = recorder
        });

        using var connection = factory.CreateOpenConnection();

        Assert.IsType<InterceptingDbConnection>(connection);
        Assert.Equal(ConnectionState.Open, connection.State);
        Assert.Contains(recorder.Events, x => x.EventKind == DbInterceptionEventKind.ConnectionOpened);
    }
}
