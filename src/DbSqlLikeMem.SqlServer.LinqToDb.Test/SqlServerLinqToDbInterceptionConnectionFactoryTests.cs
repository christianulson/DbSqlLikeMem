namespace DbSqlLikeMem.SqlServer.LinqToDb.Test;

/// <summary>
/// EN: Validates interception-capable SqlServer LinqToDB connection factories.
/// PT: Valida factories de conexao SqlServer para LinqToDB com suporte a interceptacao.
/// </summary>
public sealed class SqlServerLinqToDbInterceptionConnectionFactoryTests
{
    /// <summary>
    /// EN: Verifies the options-based constructor returns an opened wrapped connection.
    /// PT: Verifica se o construtor baseado em opcoes retorna uma conexao encapsulada ja aberta.
    /// </summary>
    [Fact]
    public void ConstructorWithOptions_ShouldReturnOpenedWrappedConnection()
    {
        var recorder = new RecordingDbConnectionInterceptor();
        var factory = new SqlServerLinqToDbConnectionFactory(new DbInterceptionOptions
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
