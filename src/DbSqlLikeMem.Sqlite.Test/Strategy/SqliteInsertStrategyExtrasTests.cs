namespace DbSqlLikeMem.Sqlite.Test.Strategy;
/// <summary>
/// EN: Defines the class SqliteInsertStrategyExtrasTests.
/// PT: Define a classe SqliteInsertStrategyExtrasTests.
/// </summary>
public sealed class SqliteInsertStrategyExtrasTests(
        ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Tests MultiRowInsertShouldAddAllRows behavior.
    /// PT: Testa o comportamento de MultiRowInsertShouldAddAllRows.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void MultiRowInsertShouldAddAllRows()
    {
        // Arrange
        var db = new SqliteDbMock();
        var table = db.AddTable("t");
        table.AddColumn("id", DbType.Int32, false, identity: false);
        table.AddColumn("val", DbType.String, true);
        using var cnn = new SqliteConnectionMock(db);
        using var cmd = new SqliteCommandMock(cnn)
        {
            CommandText = "INSERT INTO t (id, val) VALUES (1, 'A'), (2, 'B'), (3, 'C')"
        };

        // Act
        var affected = cmd.ExecuteNonQuery();

        // Assert
        Assert.Equal(3, affected);
        Assert.Equal(3, table.Count);
        Assert.Equal("B", table[1][1]);
    }

    /// <summary>
    /// EN: Tests InsertWithDefaultValueAndIdentityShouldApplyDefaults behavior.
    /// PT: Testa o comportamento de InsertWithDefaultValueAndIdentityShouldApplyDefaults.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void InsertWithDefaultValueAndIdentityShouldApplyDefaults()
    {
        // Arrange
        var db = new SqliteDbMock();
        var table = db.AddTable("t");
        table.AddColumn("id", DbType.Int32, false, identity: true);
        table.AddColumn("name", DbType.String, false, defaultValue: "DEF");
        using var cnn = new SqliteConnectionMock(db);
        using var cmd = new SqliteCommandMock(cnn)
        {
            CommandText = "INSERT INTO t () VALUES ()" // no columns specified
        };

        // Act
        var affected1 = cmd.ExecuteNonQuery();
        var affected2 = cmd.ExecuteNonQuery();

        // Assert
        Assert.Equal(1, affected1);
        Assert.Equal(1, affected2);
        Assert.Equal(2, table.Count);
        Assert.Equal(1, table[0][0]);
        Assert.Equal("DEF", table[0][1]);
        Assert.Equal(2, table[1][0]);
    }

    /// <summary>
    /// EN: Tests InsertDuplicatePrimaryKeyShouldThrow behavior.
    /// PT: Testa o comportamento de InsertDuplicatePrimaryKeyShouldThrow.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void InsertDuplicatePrimaryKeyShouldThrow()
    {
        // Arrange
        var db = new SqliteDbMock();
        var table = db.AddTable("t");
        table.AddColumn("id", DbType.Int32, false, identity: false);
        table.AddPrimaryKeyIndexes("id");
        using var cnn = new SqliteConnectionMock(db);
        using var cmd = new SqliteCommandMock(cnn)
        {
            CommandText = "INSERT INTO t (id) VALUES (1)"
        };
        cmd.ExecuteNonQuery();

        // Act & Assert
        var ex = Assert.Throws<SqliteMockException>(() => cmd.ExecuteNonQuery());
        Assert.Contains(SqlExceptionMessages.DuplicateKey(string.Empty, string.Empty).Split('\'')[0].Trim(), ex.Message, StringComparison.OrdinalIgnoreCase);
    }
}

    /// <summary>
    /// EN: Tests delete strategy behavior with foreign keys.
    /// PT: Testes do comportamento da estratégia de delete com chaves estrangeiras.
    /// </summary>
public class SqliteDeleteStrategyForeignKeyTests
{
    /// <summary>
    /// EN: Tests DeleteReferencedRowShouldThrow behavior.
    /// PT: Testa o comportamento de DeleteReferencedRowShouldThrow.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void DeleteReferencedRowShouldThrow()
    {
        // Arrange parent
        var db = new SqliteDbMock();
        var parent = db.AddTable("p");
        parent.AddColumn("id", DbType.Int32, false);
        parent.AddPrimaryKeyIndexes("id");                   // marca 'id' como PK
        parent.Add(new Dictionary<int, object?> { { 0, 42 } });

        // Arrange child
        var child = db.AddTable("c");
        child.AddColumn("pid", DbType.Int32, false);
        child.CreateForeignKey("ix_parent_id", parent.TableName, [("pid", "id")]);    // c(pid) → p(id)
        child.Add(new Dictionary<int, object?> { { 0, 42 } });

        using var cnn = new SqliteConnectionMock(db);

        using var cmd = new SqliteCommandMock(cnn)
        {
            CommandText = "DELETE FROM p WHERE id = 42"
        };

        // Act & Assert
        var ex = Assert.Throws<SqliteMockException>(() => cmd.ExecuteNonQuery());
        Assert.Contains(SqlExceptionMessages.ReferencedRow(string.Empty).Split('(')[0].Trim(), ex.Message, StringComparison.OrdinalIgnoreCase);
    }
}

    /// <summary>
    /// EN: Extra tests for update strategy behavior.
    /// PT: Testes extras do comportamento da estratégia de update.
    /// </summary>
public class SqliteUpdateStrategyExtrasTests
{
    /// <summary>
    /// EN: Tests UpdateMultipleConditionsShouldOnlyAffectMatchingRows behavior.
    /// PT: Testa o comportamento de UpdateMultipleConditionsShouldOnlyAffectMatchingRows.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void UpdateMultipleConditionsShouldOnlyAffectMatchingRows()
    {
        // Arrange
        var db = new SqliteDbMock();
        var table = db.AddTable("t");
        table.AddColumn("id", DbType.Int32, false);
        table.AddColumn("grp", DbType.String, false);
        table.AddColumn("val", DbType.String, false);
        table.Add(new Dictionary<int, object?> { { 0, 1 }, { 1, "X" }, { 2, "A" } });
        table.Add(new Dictionary<int, object?> { { 0, 2 }, { 1, "Y" }, { 2, "A" } });
        using var cnn = new SqliteConnectionMock(db);
        using var cmd = new SqliteCommandMock(cnn)
        {
            CommandText = "UPDATE t SET val = 'Z' WHERE grp = 'X' AND id = 1"
        };

        // Act
        var affected = cmd.ExecuteNonQuery();

        // Assert
        Assert.Equal(1, affected);
        Assert.Equal("Z", table[0][2]);
        Assert.Equal("A", table[1][2]);
    }
}
