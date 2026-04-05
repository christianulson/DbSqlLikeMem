namespace DbSqlLikeMem.Firebird.Test;

/// <summary>
/// EN: Runs Firebird fidelity tests for shared SELECT-into and derived-select DML flows.
/// PT: Executa testes de fidelidade Firebird para fluxos compartilhados de SELECT-into e DML com subselect derivado.
/// </summary>
/// <param name="helper">
/// EN: xUnit output helper used by the shared base test class.
/// PT: Helper de saída do xUnit usado pela classe base de testes compartilhada.
/// </param>
public sealed class SelectIntoInsertSelectUpdateDeleteFromSelectTests(
        ITestOutputHelper helper
    ) : SelectIntoInsertSelectUpdateDeleteFromSelectTestsBase<FirebirdDbMock>(helper)
{
    /// <summary>
    /// EN: Creates a new Firebird mock database instance for each scenario.
    /// PT: Cria uma nova instância de banco simulado Firebird para cada cenário.
    /// </summary>
    protected override FirebirdDbMock CreateDb() => [];

    /// <summary>
    /// EN: Executes a non-query SQL statement against the supplied Firebird mock database.
    /// PT: Executa um comando SQL sem retorno no banco simulado Firebird informado.
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
