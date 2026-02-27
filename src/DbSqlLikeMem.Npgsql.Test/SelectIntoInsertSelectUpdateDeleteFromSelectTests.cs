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
    /// EN: Indicates PostgreSQL test runtime should execute UPDATE/DELETE scenarios that rely on JOIN support.
    /// PT: Indica que o runtime de teste do PostgreSQL deve executar cenários de UPDATE/DELETE que dependem de suporte a JOIN.
    /// </summary>
    protected override bool SupportsUpdateDeleteJoinRuntime => true;

    /// <summary>
    /// EN: Gets PostgreSQL-specific SQL used to update target rows from a derived select joined in FROM.
    /// PT: Obtém o SQL específico de PostgreSQL usado para atualizar linhas alvo a partir de um select derivado em join no FROM.
    /// </summary>
    protected override string UpdateJoinDerivedSelectSql
        => @"
UPDATE u
SET u.total = s.total
FROM users u
JOIN (SELECT userid, SUM(amount) AS total FROM orders GROUP BY userid) s ON s.userid = u.id
WHERE u.tenantid = 10";

    /// <summary>
    /// EN: Gets PostgreSQL-specific SQL used to delete rows using USING with a derived select source.
    /// PT: Obtém o SQL específico de PostgreSQL usado para excluir linhas com USING e uma origem de select derivado.
    /// </summary>
    protected override string DeleteJoinDerivedSelectSql
        => "DELETE FROM users u USING (SELECT id FROM users WHERE tenantid = 10) s WHERE s.id = u.id";

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

    /// <summary>
    /// EN: Verifies DELETE USING with join condition plus extra filter removes only rows matching both predicates.
    /// PT: Verifica que DELETE USING com condição de join e filtro extra remove apenas linhas que atendem aos dois predicados.
    /// </summary>
    [Fact]
    [Trait("Category", "SelectIntoInsertSelectUpdateDeleteFromSelect")]
    public void DeleteUsing_WithJoinConditionAndExtraFilter_ShouldDeleteOnlyFilteredRows()
    {
        var db = CreateDb();
        var users = db.AddTable("users");
        users.AddColumn("id", DbType.Int32, false);
        users.AddColumn("tenantid", DbType.Int32, false);
        users.Add(new Dictionary<int, object?> { { 0, 1 }, { 1, 10 } });
        users.Add(new Dictionary<int, object?> { { 0, 2 }, { 1, 20 } });

        const string sql = "DELETE FROM users u USING (SELECT id FROM users) s WHERE (s.id = u.id) AND u.tenantid = 10";

        var deleted = ExecuteNonQuery(db, sql);

        Assert.Equal(1, deleted);
        Assert.Single(users);
        Assert.Equal(2, (int)users[0][0]!);
    }



    /// <summary>
    /// EN: Verifies DELETE USING accepts nested parenthesized join predicates and deletes matching rows.
    /// PT: Verifica que DELETE USING aceita predicados de join aninhados entre parênteses e exclui as linhas correspondentes.
    /// </summary>
    [Fact]
    [Trait("Category", "SelectIntoInsertSelectUpdateDeleteFromSelect")]
    public void DeleteUsing_WithNestedParenthesizedJoinCondition_ShouldDeleteRows()
    {
        var db = CreateDb();
        var users = db.AddTable("users");
        users.AddColumn("id", DbType.Int32, false);
        users.AddColumn("tenantid", DbType.Int32, false);
        users.Add(new Dictionary<int, object?> { { 0, 1 }, { 1, 10 } });
        users.Add(new Dictionary<int, object?> { { 0, 2 }, { 1, 20 } });

        const string sql = "DELETE FROM users u USING (SELECT id FROM users WHERE tenantid = 10) s WHERE ((s.id = u.id))";

        var deleted = ExecuteNonQuery(db, sql);

        Assert.Equal(1, deleted);
        Assert.Single(users);
        Assert.Equal(2, (int)users[0][0]!);
    }

    /// <summary>
    /// EN: Verifies DELETE USING without a join condition is rejected with an actionable guidance message.
    /// PT: Verifica que DELETE USING sem condição de join é rejeitado com mensagem de orientação acionável.
    /// </summary>
    [Fact]
    [Trait("Category", "SelectIntoInsertSelectUpdateDeleteFromSelect")]
    public void DeleteUsing_WithoutJoinCondition_ShouldThrowActionableMessage()
    {
        var db = CreateDb();
        var users = db.AddTable("users");
        users.AddColumn("id", DbType.Int32, false);
        users.AddColumn("tenantid", DbType.Int32, false);
        users.Add(new Dictionary<int, object?> { { 0, 1 }, { 1, 10 } });

        const string sql = "DELETE FROM users u USING (SELECT id FROM users) s WHERE u.tenantid = 10";

        var ex = Assert.Throws<InvalidOperationException>(() => ExecuteNonQuery(db, sql));
        Assert.Contains("WHERE deve conter uma condição de junção", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

}
