namespace DbSqlLikeMem.Sqlite.Dapper.Test;

/// <summary>
/// EN: Validates Dapper usage over intercepted SQLite mock connections.
/// PT: Valida o uso de Dapper sobre conexoes SQLite simuladas e interceptadas.
/// </summary>
public sealed class InterceptionDapperIntegrationTests(
        ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Verifies Dapper queries keep working when the connection is wrapped by the interception pipeline.
    /// PT: Verifica se consultas Dapper continuam funcionando quando a conexao e encapsulada pelo pipeline de interceptacao.
    /// </summary>
    [Fact]
    [Trait("Category", "Dapper")]
    [Trait("Category", "Interception")]
    public void DapperQuery_OnInterceptedConnection_ShouldReturnRowsAndRecordEvents()
    {
        var db = new SqliteDbMock();
        var users = db.AddTable("users");
        users.AddColumn("id", DbType.Int32, false);
        users.AddColumn("name", DbType.String, false);
        users.Add(new Dictionary<int, object?> { { 0, 1 }, { 1, "Ada" } });

        var recorder = new RecordingDbConnectionInterceptor();
        using var connection = new SqliteConnectionMock(db).Intercept(recorder);
        connection.Open();

        var row = connection.QuerySingle<(int Id, string Name)>(
            "select id, name from users where id = @id",
            new { id = 1 });

        Assert.Equal(1, row.Id);
        Assert.Equal("Ada", row.Name);
        Assert.Contains(
            recorder.Events,
            x => x.EventKind == DbInterceptionEventKind.CommandExecuted
                && x.CommandText == "select id, name from users where id = @id"
                && x.CommandExecutionKind == DbCommandExecutionKind.Reader);
    }

    /// <summary>
    /// EN: Verifies Dapper transaction flows still notify begin and commit callbacks on intercepted connections.
    /// PT: Verifica se fluxos transacionais do Dapper ainda notificam callbacks de inicio e commit em conexoes interceptadas.
    /// </summary>
    [Fact]
    [Trait("Category", "Dapper")]
    [Trait("Category", "Interception")]
    public void DapperTransaction_OnInterceptedConnection_ShouldRecordTransactionEvents()
    {
        var db = new SqliteDbMock();
        var users = db.AddTable("users");
        users.AddColumn("id", DbType.Int32, false);
        users.AddColumn("name", DbType.String, false);

        var recorder = new RecordingDbConnectionInterceptor();
        using var connection = new SqliteConnectionMock(db).Intercept(recorder);
        connection.Open();

        using (var transaction = connection.BeginTransaction())
        {
            _ = connection.Execute(
                "insert into users (id, name) values (@id, @name)",
                new { id = 2, name = "Grace" },
                transaction);
            transaction.Commit();
        }

        Assert.Single(users);
        Assert.Equal(2, users[0][0]);
        Assert.Equal("Grace", users[0][1]);
        Assert.Contains(
            recorder.Events,
            x => x.EventKind == DbInterceptionEventKind.TransactionStarted
                && x.TransactionOperationKind == DbTransactionOperationKind.Begin);
        Assert.Contains(
            recorder.Events,
            x => x.EventKind == DbInterceptionEventKind.TransactionExecuted
                && x.TransactionOperationKind == DbTransactionOperationKind.Commit);
    }
}
