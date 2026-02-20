namespace DbSqlLikeMem.Oracle.Test;

/// <summary>
/// EN: Runs shared tests for SELECT/INSERT/UPDATE/DELETE-from-select flows using Oracle mocks.
/// PT: Executa os testes compartilhados de fluxos SELECT/INSERT/UPDATE/DELETE-via-select usando mocks de Oracle.
/// </summary>
/// <param name="helper">
/// EN: xUnit output helper used by the shared base test class.
/// PT: Helper de saída do xUnit usado pela classe base de testes compartilhada.
/// </param>
public sealed class SelectIntoInsertSelectUpdateDeleteFromSelectTests(
        ITestOutputHelper helper
    ) : SelectIntoInsertSelectUpdateDeleteFromSelectTestsBase<OracleDbMock>(helper)
{
    /// <summary>
    /// EN: Creates a new Oracle mock database instance for each test.
    /// PT: Cria uma nova instância de banco mock de Oracle para cada teste.
    /// </summary>
    protected override OracleDbMock CreateDb() => [];

    /// <summary>
    /// EN: Executes a non-query SQL statement against the provided Oracle mock database.
    /// PT: Executa um comando SQL sem retorno no banco mock de Oracle informado.
    /// </summary>
    protected override int ExecuteNonQuery(
        OracleDbMock db,
        string sql)
    {
        using var c = new OracleConnectionMock(db);
        using var cmd = new OracleCommandMock(c) { CommandText = sql };
        return cmd.ExecuteNonQuery();
    }
}
