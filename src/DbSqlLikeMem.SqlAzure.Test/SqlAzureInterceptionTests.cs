namespace DbSqlLikeMem.SqlAzure.Test;

/// <summary>
/// EN: Validates interception pipeline usage on SQL Azure provider entry points.
/// PT: Valida o uso do pipeline de interceptacao nos pontos de entrada do provider SQL Azure.
/// </summary>
public sealed class SqlAzureInterceptionTests(
        ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Verifies direct interception over SqlAzureConnectionMock keeps command execution working and records events.
    /// PT: Verifica se a interceptacao direta sobre SqlAzureConnectionMock mantem a execucao de comandos funcionando e registra eventos.
    /// </summary>
    [Fact]
    public void Intercept_OnSqlAzureConnectionMock_ShouldExecuteCommandsAndRecordEvents()
    {
        var db = new SqlAzureDbMock();
        var users = db.AddTable("users");
        users.AddColumn("id", DbType.Int32, false);
        users.AddColumn("name", DbType.String, false);
        users.Add(new Dictionary<int, object?> { [0] = 1, [1] = "Ada" });

        var recorder = new RecordingDbConnectionInterceptor();
        using var connection = new SqlAzureConnectionMock(db).Intercept(recorder);
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = "select name from users where id = 1";

        var result = command.ExecuteScalar();

        result.Should().Be("Ada");
        recorder.Events.Should().Contain(x =>
            x.EventKind == DbInterceptionEventKind.CommandExecuted
            && x.CommandText == "select name from users where id = 1"
            && x.CommandExecutionKind == DbCommandExecutionKind.Scalar);
    }

    /// <summary>
    /// EN: Verifies the SQL Azure factory shortcut can also return an intercepted connection.
    /// PT: Verifica se o atalho de factory do SQL Azure tambem consegue retornar uma conexao interceptada.
    /// </summary>
    [Fact]
    public void CreateSqlAzureWithTablesIntercepted_ShouldReturnWrappedConnection()
    {
        var recorder = new RecordingDbConnectionInterceptor();
        var (db, connection) = DbMockConnectionFactory.CreateSqlAzureWithTablesIntercepted(
            new DbInterceptionOptions
            {
                EnableRecording = true,
                RecordingInterceptor = recorder
            },
            static mock =>
            {
                var users = mock.AddTable("users");
                users.AddColumn("id", DbType.Int32, false);
                users.AddColumn("name", DbType.String, false);
                users.Add(new Dictionary<int, object?> { [0] = 2, [1] = "Grace" });
            });

        using (connection)
        using (var command = connection.CreateCommand())
        {
            command.CommandText = "select name from users where id = 2";
            command.ExecuteScalar().Should().Be("Grace");
        }

        db.Should().BeOfType<SqlAzureDbMock>();
        connection.Should().BeOfType<InterceptingDbConnection>();
        recorder.Events.Should().Contain(x =>
            x.EventKind == DbInterceptionEventKind.CommandExecuted
            && x.CommandText == "select name from users where id = 2");
    }
}
