namespace DbSqlLikeMem.SqlServer.Test.TemporaryTable;

/// <summary>
/// EN: Covers temporary table execution in the SqlServer mock.
/// PT-br: Cobre a execucao de tabelas temporarias no mock SqlServer.
/// </summary>
public sealed class SqlServerTemporaryTableEngineTests
{
    private static readonly int[] expected = [1, 2];

    /// <summary>
    /// EN: Verifies a temporary table created from SELECT can be queried in the same batch.
    /// PT-br: Verifica se uma tabela temporaria criada a partir de SELECT pode ser consultada no mesmo lote.
    /// </summary>
    [Fact]
    [Trait("Category", "TemporaryTable")]
    public void CreateTemporaryTable_AsSelect_ThenSelect_ShouldReturnProjectedRows()
    {
        var db = new SqlServerDbMock();
        var users = db.AddTable("users");
        users.AddColumn("id", DbType.Int32, false);
        users.AddColumn("name", DbType.String, false);
        users.AddColumn("tenantid", DbType.Int32, false);
        users.Add(new Dictionary<int, object?> { [0] = 1, [1] = "John", [2] = 10 });
        users.Add(new Dictionary<int, object?> { [0] = 2, [1] = "Bob", [2] = 10 });
        users.Add(new Dictionary<int, object?> { [0] = 3, [1] = "Jane", [2] = 20 });

        using var cnn = new SqlServerConnectionMock(db);
        cnn.Open();

        const string sql = @"
CREATE TEMPORARY TABLE tmp_users AS
SELECT id, name FROM users WHERE tenantid = 10;

SELECT id FROM tmp_users ORDER BY id;";

        // TDD contract: engine must execute both statements in order.
        using var cmd = new SqlServerCommandMock(cnn) { CommandText = sql };

        // TDD contract: ExecuteReader should return the last SELECT results.
        using var r = cmd.ExecuteReader();

        var ids = new List<int>();
        while (r.Read()) ids.Add(r.GetInt32(0));

        ids.Should().Equal(expected);
    }

    /// <summary>
    /// EN: Verifies a global temporary table remains visible across connections.
    /// PT-br: Verifica se uma tabela temporaria global permanece visivel entre conexoes.
    /// </summary>
    [Fact]
    [Trait("Category", "TemporaryTable")]
    public void CreateGlobalTemporaryTable_AsSelect_ShouldBeVisibleAcrossConnections()
    {
        var db = new SqlServerDbMock();
        var users = db.AddTable("users");
        users.AddColumn("id", DbType.Int32, false);
        users.AddColumn("name", DbType.String, false);
        users.AddColumn("tenantid", DbType.Int32, false);
        users.Add(new Dictionary<int, object?> { [0] = 1, [1] = "John", [2] = 10 });
        users.Add(new Dictionary<int, object?> { [0] = 2, [1] = "Bob", [2] = 10 });

        using (var creator = new SqlServerConnectionMock(db))
        {
            creator.Open();
            const string createSql = @"
CREATE TABLE ##tmp_users AS
SELECT id, name FROM users WHERE tenantid = 10;

SELECT id FROM ##tmp_users ORDER BY id;";
            using var createCmd = new SqlServerCommandMock(creator) { CommandText = createSql };
            using var reader = createCmd.ExecuteReader();
            while (reader.Read()) { }
        }

        using var consumer = new SqlServerConnectionMock(db);
        consumer.Open();
        using var selectCmd = new SqlServerCommandMock(consumer)
        {
            CommandText = "SELECT id FROM ##tmp_users ORDER BY id;"
        };

        using var r = selectCmd.ExecuteReader();
        var ids = new List<int>();
        while (r.Read()) ids.Add(r.GetInt32(0));

        ids.Should().Equal(expected);
    }
}
