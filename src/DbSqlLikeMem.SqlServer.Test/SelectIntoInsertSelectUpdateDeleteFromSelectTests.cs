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
    /// PT: Cria uma nova instância de banco mock de SQL Server para cada teste.
    /// </summary>
    protected override SqlServerDbMock CreateDb() => [];

    /// <summary>
    /// EN: Executes a non-query SQL statement against the provided SQL Server mock database.
    /// PT: Executa um comando SQL sem retorno no banco mock de SQL Server informado.
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
    /// EN: Provides SQL Server specific syntax for delete with join over a derived select.
    /// PT: Fornece a sintaxe específica do SQL Server para delete com join sobre subselect derivado.
    /// </summary>
    protected override string DeleteJoinDerivedSelectSql
        => "DELETE u FROM users u JOIN (SELECT id FROM users WHERE tenantid = 10) s ON s.id = u.id";
}
