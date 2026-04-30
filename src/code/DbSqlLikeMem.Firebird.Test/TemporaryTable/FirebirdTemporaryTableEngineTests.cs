namespace DbSqlLikeMem.Firebird.Test.TemporaryTable;

/// <summary>
/// EN: Covers Firebird temporary table execution scenarios in the mock engine.
/// PT: Cobre cenarios de execucao de tabela temporaria Firebird no motor simulado.
/// </summary>
public sealed class FirebirdTemporaryTableEngineTests
{
    /// <summary>
    /// EN: Verifies CREATE TEMPORARY TABLE AS SELECT returns the projected rows.
    /// PT: Verifica se CREATE TEMPORARY TABLE AS SELECT retorna as linhas projetadas.
    /// </summary>
    [Fact]
    [Trait("Category", "TemporaryTable")]
    public void CreateTemporaryTable_AsSelect_ThenSelect_ShouldReturnProjectedRows()
    {
        var db = new FirebirdDbMock();
        var users = db.AddTable("users");
        users.AddColumn("id", DbType.Int32, false);
        users.AddColumn("name", DbType.String, false);
        users.AddColumn("tenantid", DbType.Int32, false);
        users.Add(new Dictionary<int, object?> { [0] = 1, [1] = "John", [2] = 10 });
        users.Add(new Dictionary<int, object?> { [0] = 2, [1] = "Bob", [2] = 10 });
        users.Add(new Dictionary<int, object?> { [0] = 3, [1] = "Jane", [2] = 20 });

        using var cnn = new FirebirdConnectionMock(db);
        cnn.Open();

        const string sql = @"
CREATE TEMPORARY TABLE tmp_users AS
SELECT id, name FROM users WHERE tenantid = 10;

SELECT id FROM tmp_users ORDER BY id;";

        using var cmd = new FirebirdCommandMock(cnn) { CommandText = sql };
        using var r = cmd.ExecuteReader();

        var ids = new List<int>();
        while (r.Read())
            ids.Add(r.GetInt32(0));

        ids.Should().Equal([1, 2]);
    }

    /// <summary>
    /// EN: Verifies temporary tables stay isolated from secondary connections.
    /// PT: Verifica se tabelas temporarias permanecem isoladas de conexoes secundarias.
    /// </summary>
    [Fact]
    [Trait("Category", "TemporaryTable")]
    public void TemporaryTable_ShouldBeConnectionScoped()
    {
        var db = new FirebirdDbMock();
        var users = db.AddTable("users");
        users.AddColumn("id", DbType.Int32, false);
        users.AddColumn("name", DbType.String, false);
        users.Add(new Dictionary<int, object?> { [0] = 1, [1] = "John" });

        using var connA = new FirebirdConnectionMock(db);
        using var connB = new FirebirdConnectionMock(db);
        connA.Open();
        connB.Open();

        using (var cmd = new FirebirdCommandMock(connA))
        {
            cmd.CommandText = "CREATE TEMPORARY TABLE tmp_users AS SELECT id, name FROM users";
            cmd.ExecuteNonQuery();
        }

        connA.TryGetTemporaryTable("tmp_users", out var tempA).Should().BeTrue();
        connB.TryGetTemporaryTable("tmp_users", out var tempB).Should().BeFalse();
        tempA.Should().ContainSingle();
    }

    /// <summary>
    /// EN: Verifies global temporary table rows stay isolated from secondary connections.
    /// PT: Verifica se as linhas de uma tabela temporaria global permanecem isoladas de conexoes secundarias.
    /// </summary>
    [Fact]
    [Trait("Category", "TemporaryTable")]
    public void GlobalTemporaryTable_ShouldBeSessionScoped()
    {
        var db = new FirebirdDbMock();
        var users = db.AddTable("users");
        users.AddColumn("id", DbType.Int32, false);
        users.AddColumn("name", DbType.String, false);
        users.Add(new Dictionary<int, object?> { [0] = 1, [1] = "John" });

        using var connA = new FirebirdConnectionMock(db);
        using var connB = new FirebirdConnectionMock(db);
        connA.Open();
        connB.Open();

        using (var cmd = new FirebirdCommandMock(connA))
        {
            cmd.CommandText = """
CREATE GLOBAL TEMPORARY TABLE gtmp_users (
    id INTEGER,
    name VARCHAR(100)
) ON COMMIT PRESERVE ROWS
""";
            cmd.ExecuteNonQuery();
        }

        using (var cmd = new FirebirdCommandMock(connA))
        {
            cmd.CommandText = "INSERT INTO gtmp_users (id, name) VALUES (1, 'Alice')";
            cmd.ExecuteNonQuery();
        }

        using (var cmd = new FirebirdCommandMock(connB))
        {
            cmd.CommandText = "SELECT COUNT(*) FROM gtmp_users";
            var count = Convert.ToInt32(cmd.ExecuteScalar(), CultureInfo.InvariantCulture);
            count.Should().Be(0);
        }
    }
}
