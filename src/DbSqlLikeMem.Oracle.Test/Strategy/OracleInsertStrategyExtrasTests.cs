namespace DbSqlLikeMem.Oracle.Test.Strategy;
/// <summary>
/// Auto-generated summary.
/// </summary>
public sealed class OracleInsertStrategyExtrasTests(
        ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Tests MultiRowInsertShouldAddAllRows behavior.
    /// PT: Testa o comportamento de MultiRowInsertShouldAddAllRows.
    /// </summary>
    [Fact]
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public void MultiRowInsertShouldAddAllRows()
    {
        // Arrange
        var db = new OracleDbMock();
        var table = db.AddTable("t");
        table.Columns["id"] = new ColumnDef(0, DbType.Int32, false) { Identity = false };
        table.Columns["val"] = new ColumnDef(1, DbType.String, true);
        using var cnn = new OracleConnectionMock(db);
        using var cmd = new OracleCommandMock(cnn)
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
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public void InsertWithDefaultValueAndIdentityShouldApplyDefaults()
    {
        // Arrange
        var db = new OracleDbMock();
        var table = db.AddTable("t");
        table.Columns["id"] = new ColumnDef(0, DbType.Int32, false) { Identity = true };
        table.Columns["name"] = new ColumnDef(1, DbType.String, false) { DefaultValue = "DEF" };
        using var cnn = new OracleConnectionMock(db);
        using var cmd = new OracleCommandMock(cnn)
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
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public void InsertDuplicatePrimaryKeyShouldThrow()
    {
        // Arrange
        var db = new OracleDbMock();
        var table = db.AddTable("t");
        table.Columns["id"] = new ColumnDef(0, DbType.Int32, false) { Identity = false };
        table.PrimaryKeyIndexes.Add(0);
        using var cnn = new OracleConnectionMock(db);
        using var cmd = new OracleCommandMock(cnn)
        {
            CommandText = "INSERT INTO t (id) VALUES (1)"
        };
        cmd.ExecuteNonQuery();

        // Act & Assert
        var ex = Assert.Throws<OracleMockException>(() => cmd.ExecuteNonQuery());
        Assert.Contains("Duplicate entry", ex.Message, StringComparison.Ordinal);
    }
}

    /// <summary>
    /// EN: Tests delete strategy behavior with foreign keys.
    /// PT: Testes do comportamento da estratégia de delete com chaves estrangeiras.
    /// </summary>
public class OracleDeleteStrategyForeignKeyTests
{
    /// <summary>
    /// EN: Tests DeleteReferencedRowShouldThrow behavior.
    /// PT: Testa o comportamento de DeleteReferencedRowShouldThrow.
    /// </summary>
    [Fact]
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public void DeleteReferencedRowShouldThrow()
    {
        // Arrange parent
        var db = new OracleDbMock();
        var parent = db.AddTable("p");
        parent.Columns["id"] = new ColumnDef(0, DbType.Int32, false);
        parent.PrimaryKeyIndexes.Add(0);                   // marca 'id' como PK
        parent.Add(new Dictionary<int, object?> { { 0, 42 } });

        // Arrange child
        var child = db.AddTable("c");
        child.Columns["pid"] = new ColumnDef(0, DbType.Int32, false);
        child.CreateForeignKey("pid", "p", "id");     // c(pid) → p(id)
        child.Add(new Dictionary<int, object?> { { 0, 42 } });

        using var cnn = new OracleConnectionMock(db);

        using var cmd = new OracleCommandMock(cnn)
        {
            CommandText = "DELETE FROM p WHERE id = 42"
        };

        // Act & Assert
        var ex = Assert.Throws<OracleMockException>(() => cmd.ExecuteNonQuery());
        Assert.Contains("Cannot delete or update a parent row", ex.Message, StringComparison.Ordinal);
    }
}

    /// <summary>
    /// EN: Extra tests for update strategy behavior.
    /// PT: Testes extras do comportamento da estratégia de update.
    /// </summary>
public class OracleUpdateStrategyExtrasTests
{
    /// <summary>
    /// EN: Tests UpdateMultipleConditionsShouldOnlyAffectMatchingRows behavior.
    /// PT: Testa o comportamento de UpdateMultipleConditionsShouldOnlyAffectMatchingRows.
    /// </summary>
    [Fact]
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public void UpdateMultipleConditionsShouldOnlyAffectMatchingRows()
    {
        // Arrange
        var db = new OracleDbMock();
        var table = db.AddTable("t");
        table.Columns["id"] = new ColumnDef(0, DbType.Int32, false);
        table.Columns["grp"] = new ColumnDef(1, DbType.String, false);
        table.Columns["val"] = new ColumnDef(2, DbType.String, false);
        table.Add(new Dictionary<int, object?> { { 0, 1 }, { 1, "X" }, { 2, "A" } });
        table.Add(new Dictionary<int, object?> { { 0, 2 }, { 1, "Y" }, { 2, "A" } });
        using var cnn = new OracleConnectionMock(db);
        using var cmd = new OracleCommandMock(cnn)
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
