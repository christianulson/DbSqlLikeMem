namespace DbSqlLikeMem.Db2.Test;

/// <summary>
/// EN: Runs shared tests for SELECT/INSERT/UPDATE/DELETE-from-select flows using Db2 mocks.
/// PT-br: Executa os testes compartilhados de fluxos SELECT/INSERT/UPDATE/DELETE-via-select usando mocks de Db2.
/// </summary>
/// <param name="helper">
/// EN: xUnit output helper used by the shared base test class.
/// PT-br: Helper de saída do xUnit usado pela classe base de testes compartilhada.
/// </param>
public sealed class SelectIntoInsertSelectUpdateDeleteFromSelectTests(
        ITestOutputHelper helper
    ) : SelectIntoInsertSelectUpdateDeleteFromSelectTestsBase<Db2DbMock>(helper)
{
    /// <inheritdoc />
    protected override DbSqlLikeMem.TestTools.ProviderSqlDialect Dialect { get; } = new DbSqlLikeMem.Db2.TestTools.Db2ProviderSqlDialect();

    /// <summary>
    /// EN: Gets the affected-row count expected for CREATE TABLE AS SELECT in Db2.
    /// PT-br: Obtém a contagem de linhas afetadas esperada para CREATE TABLE AS SELECT no Db2.
    /// </summary>
    protected override int CreateTableAsSelectExpectedAffectedRows => 2;

    /// <summary>
    /// EN: Creates a new Db2 mock database instance for each test.
    /// PT-br: Cria uma nova instância de banco simulado de Db2 para cada teste.
    /// </summary>
    protected override Db2DbMock CreateDb() => [];

    /// <summary>
    /// EN: Executes a non-query SQL statement against the provided Db2 mock database.
    /// PT-br: Executa um comando SQL sem retorno no banco simulado de Db2 informado.
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
