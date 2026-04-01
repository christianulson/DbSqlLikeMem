using FluentAssertions;

namespace DbSqlLikeMem.Sqlite.Test.Strategy;

/// <summary>
/// EN: Covers multi-row and select-based INSERT scenarios in the Sqlite mock.
/// PT: Cobre cenarios de INSERT com multiplas linhas e baseado em SELECT no mock Sqlite.
/// </summary>
public sealed class SqliteInsertStrategyCoverageTests(
        ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Verifies that a multi-row VALUES insert adds every row.
    /// PT: Verifica se um INSERT VALUES com varias linhas adiciona todas as linhas.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void Insert_MultiRowValues_ShouldInsertAllRows()
    {
        var db = new SqliteDbMock();
        var t = db.AddTable("t");
        t.AddColumn("id", DbType.Int32, false, identity: false);
        t.AddColumn("name", DbType.String, false);

        using var cnn = new SqliteConnectionMock(db);
        using var cmd = new SqliteCommandMock(cnn)
        {
            CommandText = "INSERT INTO t (id, name) VALUES (1, 'A'), (2, 'B')"
        };

        var inserted = cmd.ExecuteNonQuery();

        inserted.Should().Be(2);
        t.Count.Should().Be(2);
        t[0][0].Should().Be(1);
        t[0][1].Should().Be("A");
        t[1][0].Should().Be(2);
        t[1][1].Should().Be("B");
    }

    /// <summary>
    /// EN: Verifies that omitting an identity column lets SQLite generate the value.
    /// PT: Verifica se omitir a coluna identity permite ao SQLite gerar o valor.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void Insert_WithIdentityColumnOmitted_ShouldAutoIncrement()
    {
        var db = new SqliteDbMock();
        var t = db.AddTable("t");
        t.AddColumn("id", DbType.Int32, false, identity: true);
        t.AddColumn("name", DbType.String, false);

        t.AddPrimaryKeyIndexes("id");

        using var cnn = new SqliteConnectionMock(db);
        using var cmd = new SqliteCommandMock(cnn)
        {
            CommandText = "INSERT INTO t (name) VALUES ('A'), ('B')"
        };

        var inserted = cmd.ExecuteNonQuery();

        inserted.Should().Be(2);
        t.Count.Should().Be(2);
        t[0][0].Should().Be(1);
        t[0][1].Should().Be("A");
        t[1][0].Should().Be(2);
        t[1][1].Should().Be("B");
    }

    /// <summary>
    /// EN: Verifies that INSERT ... SELECT copies rows from the source query.
    /// PT: Verifica se INSERT ... SELECT copia linhas da consulta de origem.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void InsertSelect_ShouldInsertRowsFromSelect()
    {
        var db = new SqliteDbMock();
        var users = db.AddTable("users");
        users.AddColumn("id", DbType.Int32, false);
        users.AddColumn("tenantid", DbType.Int32, false);
        users.Add(new Dictionary<int, object?> { [0] = 1, [1] = 10 });
        users.Add(new Dictionary<int, object?> { [0] = 2, [1] = 10 });
        users.Add(new Dictionary<int, object?> { [0] = 3, [1] = 20 });

        var t = db.AddTable("t");
        t.AddColumn("id", DbType.Int32, false);

        using var cnn = new SqliteConnectionMock(db);

        using var cmd = new SqliteCommandMock(cnn)
        {
            CommandText = "INSERT INTO t (id) SELECT id FROM users WHERE tenantid = 10"
        };

        var inserted = cmd.ExecuteNonQuery();

        inserted.Should().Be(2);
        t.Count.Should().Be(2);
        t.Select(r => (int)r[0]!).Should().Equal([1, 2]);
    }
}
