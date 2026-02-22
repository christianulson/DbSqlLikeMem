namespace DbSqlLikeMem.Sqlite.Test;

/// <summary>
/// EN: Exercises select-into, insert-select, update, and delete-from-select flows for SQLite.
/// PT: Exercita fluxos de select-into, insert-select, update e delete-from-select para SQLite.
/// </summary>
/// <param name="helper">
/// EN: Output helper used by the test base.
/// PT: Helper de saída usado pela base de testes.
/// </param>
public sealed class SelectIntoInsertSelectUpdateDeleteFromSelectTests(
        ITestOutputHelper helper
    ) : SelectIntoInsertSelectUpdateDeleteFromSelectTestsBase<SqliteDbMock>(helper)
{
    /// <summary>
    /// EN: Creates a new SQLite mock database for each scenario.
    /// PT: Cria um novo banco simulado de SQLite para cada cenário.
    /// </summary>
    protected override SqliteDbMock CreateDb() => [];

    /// <summary>
    /// EN: Executes a non-query command using a SQLite mock connection.
    /// PT: Executa um comando sem retorno usando uma conexão simulada de SQLite.
    /// </summary>
    protected override int ExecuteNonQuery(
        SqliteDbMock db,
        string sql)
    {
        using var c = new SqliteConnectionMock(db);
        using var cmd = new SqliteCommandMock(c) { CommandText = sql };
        return cmd.ExecuteNonQuery();
    }
}
