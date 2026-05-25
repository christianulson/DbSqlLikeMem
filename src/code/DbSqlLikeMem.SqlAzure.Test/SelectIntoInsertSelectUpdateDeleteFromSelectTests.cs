namespace DbSqlLikeMem.SqlAzure.Test;

/// <summary>
/// EN: Runs shared tests for SELECT/INSERT/UPDATE/DELETE-from-select flows using SQL Azure mocks.
/// PT-br: Executa os testes compartilhados de fluxos SELECT/INSERT/UPDATE/DELETE-via-select usando mocks de SQL Azure.
/// </summary>
/// <param name="helper">
/// EN: xUnit output helper used by the shared base test class.
/// PT-br: Helper de saída do xUnit usado pela classe base de testes compartilhada.
/// </param>
public sealed class SelectIntoInsertSelectUpdateDeleteFromSelectTests(
        ITestOutputHelper helper
    ) : SelectIntoInsertSelectUpdateDeleteFromSelectTestsBase<SqlAzureDbMock>(helper)
{
    /// <inheritdoc />
    protected override DbSqlLikeMem.TestTools.ProviderSqlDialect Dialect { get; } = new DbSqlLikeMem.SqlAzure.TestTools.SqlAzureProviderSqlDialect();

    /// <summary>
    /// EN: Creates a new SQL Azure mock database instance for each test.
    /// PT-br: Cria uma nova instância de banco simulado de SQL Azure para cada teste.
    /// </summary>
    protected override SqlAzureDbMock CreateDb() => [];

    /// <summary>
    /// EN: Gets the affected-row count expected for CREATE TABLE AS SELECT in SQL Azure.
    /// PT-br: Obtém a contagem de linhas afetadas esperada para CREATE TABLE AS SELECT no SQL Azure.
    /// </summary>
    protected override int CreateTableAsSelectExpectedAffectedRows => 2;

    /// <summary>
    /// EN: Executes a non-query SQL statement against the provided SQL Azure mock database.
    /// PT-br: Executa um comando SQL sem retorno no banco simulado de SQL Azure informado.
    /// </summary>
    protected override int ExecuteNonQuery(
        SqlAzureDbMock db,
        string sql)
    {
        using var c = new SqlAzureConnectionMock(db);
        using var cmd = new SqlAzureCommandMock(c) { CommandText = sql };
        return cmd.ExecuteNonQuery();
    }

    /// <summary>
    /// EN: Verifies SQL Azure execution rejects PostgreSQL DELETE USING syntax with a clear unsupported message.
    /// PT-br: Verifica que a execução no SQL Azure rejeita a sintaxe DELETE USING do PostgreSQL com mensagem clara de não suportado.
    /// </summary>
    [Fact]
    [Trait("Category", "SelectIntoInsertSelectUpdateDeleteFromSelect")]
    public void DeleteUsingSyntax_ShouldThrowNotSupported_ForSqlAzure()
    {
        var db = CreateDb();
        var users = db.AddTable("users");
        users.AddColumn("id", DbType.Int32, false);
        users.Add(new Dictionary<int, object?> { { 0, 1 } });

        const string sql = "DELETE FROM users u USING (SELECT 1 AS id) s WHERE s.id = u.id";

        var ex = FluentActions.Invoking(() => ExecuteNonQuery(db, sql)).Should().Throw<NotSupportedException>().Which;
        ex.Message.Should().Contain("SQL não suportado para dialeto");
        ex.Message.Should().Contain("DELETE FROM ... USING");
    }
}

