namespace DbSqlLikeMem.Sqlite.Test;

/// <summary>
/// EN: Exercises select-into, insert-select, update, and delete-from-select flows for SQLite.
/// PT-br: Exercita fluxos de select-into, insert-select, update e delete-from-select para SQLite.
/// </summary>
/// <param name="helper">
/// EN: Output helper used by the test base.
/// PT-br: Helper de saída usado pela base de testes.
/// </param>
public sealed class SelectIntoInsertSelectUpdateDeleteFromSelectTests(
        ITestOutputHelper helper
    ) : SelectIntoInsertSelectUpdateDeleteFromSelectTestsBase<SqliteDbMock>(helper)
{
    /// <inheritdoc />
    protected override ProviderSqlDialect Dialect { get; } = new TestTools.SqliteProviderSqlDialect();

    /// <summary>
    /// EN: Creates a new SQLite mock database for each scenario.
    /// PT-br: Cria um novo banco simulado de SQLite para cada cenário.
    /// </summary>
    protected override SqliteDbMock CreateDb() => [];

    /// <summary>
    /// EN: Gets the affected-row count expected for CREATE TABLE AS SELECT in SQLite.
    /// PT-br: Obtém a contagem de linhas afetadas esperada para CREATE TABLE AS SELECT no SQLite.
    /// </summary>
    protected override int CreateTableAsSelectExpectedAffectedRows => 2;

    /// <summary>
    /// EN: Executes a non-query command using a SQLite mock connection.
    /// PT-br: Executa um comando sem retorno usando uma conexão simulada de SQLite.
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
