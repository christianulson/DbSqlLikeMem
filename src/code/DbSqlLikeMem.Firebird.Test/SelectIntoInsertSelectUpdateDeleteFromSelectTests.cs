namespace DbSqlLikeMem.Firebird.Test;

/// <summary>
/// EN: Runs Firebird fidelity tests for shared SELECT-into and derived-select DML flows.
/// PT-br: Executa testes de fidelidade Firebird para fluxos compartilhados de SELECT-into e DML com subselect derivado.
/// </summary>
/// <param name="helper">
/// EN: xUnit output helper used by the shared base test class.
/// PT-br: Helper de saída do xUnit usado pela classe base de testes compartilhada.
/// </param>
public sealed class SelectIntoInsertSelectUpdateDeleteFromSelectTests(
        ITestOutputHelper helper
    ) : SelectIntoInsertSelectUpdateDeleteFromSelectTestsBase<FirebirdDbMock>(helper)
{
    /// <inheritdoc />
    protected override DbSqlLikeMem.TestTools.ProviderSqlDialect Dialect { get; } = new DbSqlLikeMem.Firebird.TestTools.FirebirdProviderSqlDialect();

    /// <inheritdoc />
    protected override int CreateTableAsSelectExpectedAffectedRows => 2;

    /// <summary>
    /// EN: Creates a new Firebird mock database instance for each scenario.
    /// PT-br: Cria uma nova instância de banco simulado Firebird para cada cenário.
    /// </summary>
    protected override FirebirdDbMock CreateDb() => [];

    /// <summary>
    /// EN: Executes a non-query SQL statement against the supplied Firebird mock database.
    /// PT-br: Executa um comando SQL sem retorno no banco simulado Firebird informado.
    /// </summary>
    protected override int ExecuteNonQuery(
        FirebirdDbMock db,
        string sql)
    {
        using var connection = new FirebirdConnectionMock(db);
        using var command = new FirebirdCommandMock(connection) { CommandText = sql };
        return command.ExecuteNonQuery();
    }
}
