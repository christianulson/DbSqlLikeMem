namespace DbSqlLikeMem.Npgsql.Test.TemporaryTable;

/// <summary>
/// EN: Covers CREATE TEMPORARY TABLE execution scenarios in the Npgsql mock.
/// PT-br: Cobre cenarios de execucao de CREATE TEMPORARY TABLE no mock Npgsql.
/// </summary>
public sealed class PostgreSqlTemporaryTableEngineTests
{
    private static readonly int[] expected = [1, 2];

    /// <summary>
    /// EN: Verifies that CREATE TEMPORARY TABLE AS SELECT returns the projected rows.
    /// PT-br: Verifica se CREATE TEMPORARY TABLE AS SELECT retorna as linhas projetadas.
    /// </summary>
    [Fact]
    [Trait("Category", "TemporaryTable")]
    public void CreateTemporaryTable_AsSelect_ThenSelect_ShouldReturnProjectedRows()
    {
        var db = new NpgsqlDbMock();
        var users = db.AddTable("users");
        users.AddColumn("id", DbType.Int32, false);
        users.AddColumn("name", DbType.String, false);
        users.AddColumn("tenantid", DbType.Int32, false);
        users.Add(new Dictionary<int, object?> { [0] = 1, [1] = "John", [2] = 10 });
        users.Add(new Dictionary<int, object?> { [0] = 2, [1] = "Bob", [2] = 10 });
        users.Add(new Dictionary<int, object?> { [0] = 3, [1] = "Jane", [2] = 20 });

        using var cnn = new NpgsqlConnectionMock(db);
        cnn.Open();

        const string sql = @"
CREATE TEMPORARY TABLE tmp_users AS
SELECT id, name FROM users WHERE tenantid = 10;

SELECT id FROM tmp_users ORDER BY id;";

        // TDD contract: engine must execute both statements in order.
        using var cmd = new NpgsqlCommandMock(cnn) { CommandText = sql };

        // TDD contract: ExecuteReader should return the last SELECT results.
        using var r = cmd.ExecuteReader();

        var ids = new List<int>();
        while (r.Read()) ids.Add(r.GetInt32(0));

        ids.Should().Equal(expected);
    }

    /// <summary>
    /// EN: Verifies that pg_temp-backed temporary tables return the projected rows.
    /// PT-br: Verifica se tabelas temporarias apoiadas por pg_temp retornam as linhas projetadas.
    /// </summary>
    [Fact]
    [Trait("Category", "TemporaryTable")]
    public void CreateTemporaryTable_InPgTempSchema_ShouldReturnProjectedRows()
    {
        var db = new NpgsqlDbMock();
        var users = db.AddTable("users");
        users.AddColumn("id", DbType.Int32, false);
        users.AddColumn("name", DbType.String, false);
        users.AddColumn("tenantid", DbType.Int32, false);
        users.Add(new Dictionary<int, object?> { [0] = 1, [1] = "John", [2] = 10 });
        users.Add(new Dictionary<int, object?> { [0] = 2, [1] = "Bob", [2] = 10 });
        users.Add(new Dictionary<int, object?> { [0] = 3, [1] = "Jane", [2] = 20 });

        using var cnn = new NpgsqlConnectionMock(db);
        cnn.Open();

        const string sql = @"
CREATE TABLE pg_temp.tmp_users AS
SELECT id, name FROM users WHERE tenantid = 10;

SELECT id FROM pg_temp.tmp_users ORDER BY id;";

        using var cmd = new NpgsqlCommandMock(cnn) { CommandText = sql };
        using var r = cmd.ExecuteReader();

        var ids = new List<int>();
        while (r.Read()) ids.Add(r.GetInt32(0));

        ids.Should().Equal(expected);
    }
}
