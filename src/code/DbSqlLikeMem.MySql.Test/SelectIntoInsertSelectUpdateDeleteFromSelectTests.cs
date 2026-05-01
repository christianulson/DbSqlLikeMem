namespace DbSqlLikeMem.MySql.Test;

/// <summary>
/// EN: Exercises select-into, insert-select, update, and delete-from-select flows for MySQL.
/// PT-br: Exercita fluxos de select-into, insert-select, update e delete-from-select para MySQL.
/// </summary>
/// <param name="helper">
/// EN: Output helper used by the test base.
/// PT-br: Helper de saída usado pela base de testes.
/// </param>
public sealed class SelectIntoInsertSelectUpdateDeleteFromSelectTests(
        ITestOutputHelper helper
    ) : SelectIntoInsertSelectUpdateDeleteFromSelectTestsBase<MySqlDbMock>(helper)
{
    /// <inheritdoc />
    protected override DbSqlLikeMem.TestTools.ProviderSqlDialect Dialect { get; } = new DbSqlLikeMem.MySql.TestTools.MySqlProviderSqlDialect();

    /// <summary>
    /// EN: Gets the affected-row count expected for CREATE TABLE AS SELECT in MySQL.
    /// PT-br: Obtém a contagem de linhas afetadas esperada para CREATE TABLE AS SELECT no MySQL.
    /// </summary>
    protected override int CreateTableAsSelectExpectedAffectedRows => 2;

    /// <summary>
    /// EN: Creates a new MySQL mock database for each scenario.
    /// PT-br: Cria um novo banco simulado de MySQL para cada cenário.
    /// </summary>
    protected override MySqlDbMock CreateDb() => [];

    /// <summary>
    /// EN: Executes a non-query command using a MySQL mock connection.
    /// PT-br: Executa um comando sem retorno usando uma conexão simulada de MySQL.
    /// </summary>
    protected override int ExecuteNonQuery(
        MySqlDbMock db,
        string sql)
    {
        using var c = new MySqlConnectionMock(db);
        using var cmd = new MySqlCommandMock(c) { CommandText = sql };
        return cmd.ExecuteNonQuery();
    }

    /// <summary>
    /// EN: Verifies MySQL-style execution rejects UPDATE ... FROM ... JOIN syntax with an actionable unsupported message.
    /// PT-br: Verifica que a execução no estilo MySQL rejeita a sintaxe UPDATE ... FROM ... JOIN com mensagem acionável de não suportado.
    /// </summary>
    [Fact]
    [Trait("Category", "SelectIntoInsertSelectUpdateDeleteFromSelect")]
    public void UpdateFromJoinSyntax_ShouldThrowNotSupported_ForMySql()
    {
        var db = CreateDb();
        var users = db.AddTable("users");
        users.AddColumn("id", DbType.Int32, false);
        users.AddColumn("total", DbType.Decimal, true, decimalPlaces: 2);
        users.Add(new Dictionary<int, object?> { { 0, 1 }, { 1, null } });

        var orders = db.AddTable("orders");
        orders.AddColumn("userid", DbType.Int32, false);
        orders.AddColumn("amount", DbType.Decimal, false, decimalPlaces: 2);
        orders.Add(new Dictionary<int, object?> { { 0, 1 }, { 1, 10m } });

        const string sql = @"
UPDATE u
SET u.total = s.total
FROM users u
JOIN (SELECT userid, SUM(amount) AS total FROM orders GROUP BY userid) s ON s.userid = u.id";

        Action act = () => ExecuteNonQuery(db, sql);
        var ex = act.Should().Throw<NotSupportedException>().Which;
        ex.Message.Should().Contain("SQL não suportado para dialeto");
        ex.Message.Should().Contain("UPDATE ... FROM ... JOIN");
    }
}
