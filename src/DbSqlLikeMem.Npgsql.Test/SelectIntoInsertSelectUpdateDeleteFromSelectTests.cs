namespace DbSqlLikeMem.Npgsql.Test;

/// <summary>
/// EN: Exercises select-into, insert-select, update, and delete-from-select flows for PostgreSQL.
/// PT: Exercita fluxos de select-into, insert-select, update e delete-from-select para PostgreSQL.
/// </summary>
/// <param name="helper">
/// EN: Output helper used by the test base.
/// PT: Helper de saída usado pela base de testes.
/// </param>
public sealed class SelectIntoInsertSelectUpdateDeleteFromSelectTests(
        ITestOutputHelper helper
    ) : SelectIntoInsertSelectUpdateDeleteFromSelectTestsBase<NpgsqlDbMock>(helper)
{
    /// <summary>
    /// EN: Creates a new PostgreSQL mock database for each scenario.
    /// PT: Cria um novo banco simulado de PostgreSQL para cada cenário.
    /// </summary>
    protected override NpgsqlDbMock CreateDb() => [];

    /// <summary>
    /// EN: Executes a non-query command using a PostgreSQL mock connection.
    /// PT: Executa um comando sem retorno usando uma conexão simulada de PostgreSQL.
    /// </summary>
    protected override int ExecuteNonQuery(
        NpgsqlDbMock db,
        string sql)
    {
        using var c = new NpgsqlConnectionMock(db);
        using var cmd = new NpgsqlCommandMock(c) { CommandText = sql };
        return cmd.ExecuteNonQuery();
    }
}
