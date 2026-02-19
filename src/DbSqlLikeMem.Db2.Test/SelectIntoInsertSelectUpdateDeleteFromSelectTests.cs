namespace DbSqlLikeMem.Db2.Test;

/// <summary>
/// EN: Runs shared tests for SELECT/INSERT/UPDATE/DELETE-from-select flows using Db2 mocks.
/// PT: Executa os testes compartilhados de fluxos SELECT/INSERT/UPDATE/DELETE-via-select usando mocks de Db2.
/// </summary>
/// <param name="helper">
/// EN: xUnit output helper used by the shared base test class.
/// PT: Helper de saída do xUnit usado pela classe base de testes compartilhada.
/// </param>
public sealed class SelectIntoInsertSelectUpdateDeleteFromSelectTests(
        ITestOutputHelper helper
    ) : SelectIntoInsertSelectUpdateDeleteFromSelectTestsBase<Db2DbMock>(helper)
{
    /// <summary>
    /// EN: Creates a new Db2 mock database instance for each test.
    /// PT: Cria uma nova instância de banco mock de Db2 para cada teste.
    /// </summary>
    protected override Db2DbMock CreateDb() => new();

    /// <summary>
    /// EN: Executes a non-query SQL statement against the provided Db2 mock database.
    /// PT: Executa um comando SQL sem retorno no banco mock de Db2 informado.
    /// </summary>
    protected override int ExecuteNonQuery(
        Db2DbMock db,
        string sql)
    {
        using var c = new Db2ConnectionMock(db);
        using var cmd = new Db2CommandMock(c) { CommandText = sql };
        return cmd.ExecuteNonQuery();
    }
}
