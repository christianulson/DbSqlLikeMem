namespace DbSqlLikeMem.SqlServer.Test;

/// <summary>
/// EN: Runs shared tests for SELECT/INSERT/UPDATE/DELETE-from-select flows using SQL Server mocks.
/// PT: Executa os testes compartilhados de fluxos SELECT/INSERT/UPDATE/DELETE-via-select usando mocks de SQL Server.
/// </summary>
/// <param name="helper">
/// EN: xUnit output helper used by the shared base test class.
/// PT: Helper de saída do xUnit usado pela classe base de testes compartilhada.
/// </param>
public sealed class SelectIntoInsertSelectUpdateDeleteFromSelectTests(
        ITestOutputHelper helper
    ) : SelectIntoInsertSelectUpdateDeleteFromSelectTestsBase<SqlServerDbMock>(helper)
{
    /// <summary>
    /// EN: Creates a new SQL Server mock database instance for each test.
    /// PT: Cria uma nova instância de banco simulado de SQL Server para cada teste.
    /// </summary>
    protected override SqlServerDbMock CreateDb() => [];

    /// <summary>
    /// EN: Indicates SQL Server test runtime should execute UPDATE/DELETE scenarios that use JOIN-aware paths.
    /// PT: Indica que o runtime de teste do SQL Server deve executar cenários de UPDATE/DELETE que usam caminhos com JOIN.
    /// </summary>
    protected override bool SupportsUpdateDeleteJoinRuntime => true;

    /// <summary>
    /// EN: Gets SQL Server-specific SQL used to update rows from a derived select joined in FROM.
    /// PT: Obtém o SQL específico de SQL Server usado para atualizar linhas a partir de um select derivado com join no FROM.
    /// </summary>
    protected override string UpdateJoinDerivedSelectSql
        => @"
UPDATE u
SET u.total = s.total
FROM users u
JOIN (SELECT userid, SUM(amount) AS total FROM orders GROUP BY userid) s ON s.userid = u.id
WHERE u.tenantid = 10";

    /// <summary>
    /// EN: Provides SQL Server specific syntax for delete with join over a derived select.
    /// PT: Fornece a sintaxe específica do SQL Server para delete com join sobre subselect derivado.
    /// </summary>
    protected override string DeleteJoinDerivedSelectSql
        => "DELETE u FROM users u JOIN (SELECT id FROM users WHERE tenantid = 10) s ON s.id = u.id";

    /// <summary>
    /// EN: Executes a non-query SQL statement against the provided SQL Server mock database.
    /// PT: Executa um comando SQL sem retorno no banco simulado de SQL Server informado.
    /// </summary>
    protected override int ExecuteNonQuery(
        SqlServerDbMock db,
        string sql)
    {
        using var c = new SqlServerConnectionMock(db);
        using var cmd = new SqlServerCommandMock(c) { CommandText = sql };
        return cmd.ExecuteNonQuery();
    }

    /// <summary>
    /// EN: Verifies SQL Server execution rejects PostgreSQL DELETE USING syntax with a clear unsupported message.
    /// PT: Verifica que a execução no SQL Server rejeita a sintaxe DELETE USING do PostgreSQL com mensagem clara de não suportado.
    /// </summary>
    [Fact]
    [Trait("Category", "SelectIntoInsertSelectUpdateDeleteFromSelect")]
    public void DeleteUsingSyntax_ShouldThrowNotSupported_ForSqlServer()
    {
        var db = CreateDb();
        var users = db.AddTable("users");
        users.AddColumn("id", DbType.Int32, false);
        users.Add(new Dictionary<int, object?> { { 0, 1 } });

        const string sql = "DELETE FROM users u USING (SELECT 1 AS id) s WHERE s.id = u.id";

        var ex = Assert.Throws<NotSupportedException>(() => ExecuteNonQuery(db, sql));
        Assert.Contains("SQL não suportado para dialeto", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("DELETE FROM ... USING", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

}
