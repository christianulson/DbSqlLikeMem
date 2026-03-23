namespace DbSqlLikeMem.MySql.Test.Strategy;
/// <summary>
/// EN: Covers extra INSERT scenarios in the MySql mock.
/// PT: Cobre cenarios extras de INSERT no mock MySql.
/// </summary>
public sealed class MySqlInsertStrategyExtrasTests(
        ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Verifies that a multi-row INSERT adds every row.
    /// PT: Verifica se um INSERT com varias linhas adiciona todas as linhas.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void MultiRowInsertShouldAddAllRows()
    {
        // Arrange
        var db = new MySqlDbMock();
        var table = db.AddTable("t");
        table.AddColumn("id", DbType.Int32, false, identity: false);
        table.AddColumn("val", DbType.String, true);
        using var cnn = new MySqlConnectionMock(db);
        using var cmd = new MySqlCommandMock(cnn)
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
    /// EN: Verifies that default values and identity columns are applied on INSERT.
    /// PT: Verifica se valores padrao e colunas identity sao aplicados no INSERT.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void InsertWithDefaultValueAndIdentityShouldApplyDefaults()
    {
        // Arrange
        var db = new MySqlDbMock();
        var table = db.AddTable("t");
        table.AddColumn("id", DbType.Int32, false, identity: true);
        table.AddColumn("name", DbType.String, false, defaultValue: "DEF");
        using var cnn = new MySqlConnectionMock(db);
        using var cmd = new MySqlCommandMock(cnn)
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
    /// EN: Verifies that duplicate primary keys raise an error.
    /// PT: Verifica se chaves primarias duplicadas geram erro.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void InsertDuplicatePrimaryKeyShouldThrow()
    {
        // Arrange
        var db = new MySqlDbMock();
        var table = db.AddTable("t");
        table.AddColumn("id", DbType.Int32, false, identity: false);
        table.AddPrimaryKeyIndexes("id");
        using var cnn = new MySqlConnectionMock(db);
        using var cmd = new MySqlCommandMock(cnn)
        {
            CommandText = "INSERT INTO t (id) VALUES (1)"
        };
        cmd.ExecuteNonQuery();

        // Act & Assert
        var ex = Assert.Throws<MySqlMockException>(() => cmd.ExecuteNonQuery());
        Assert.Contains(SqlExceptionMessages.DuplicateKey(string.Empty, string.Empty).Split('\'')[0].Trim(), ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Verifies MySQL UPSERT reports two affected rows when an existing row is updated on conflict.
    /// PT: Verifica que o UPSERT do MySQL reporte duas linhas afetadas quando uma linha existente e atualizada em conflito.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void InsertOnDuplicateConflictShouldReportTwoAffectedRows()
    {
        var db = new MySqlDbMock();
        var table = db.AddTable("t");
        table.AddColumn("id", DbType.Int32, false, identity: false);
        table.AddColumn("name", DbType.String, false);
        table.AddPrimaryKeyIndexes("id");
        table.Add(new Dictionary<int, object?> { { 0, 1 }, { 1, "OLD" } });

        using var cnn = new MySqlConnectionMock(db);
        using var cmd = new MySqlCommandMock(cnn)
        {
            CommandText = "INSERT INTO t (id, name) VALUES (1, 'NEW') ON DUPLICATE KEY UPDATE name = VALUES(name)"
        };

        var affected = cmd.ExecuteNonQuery();

        Assert.Equal(2, affected);
        Assert.Single(table);
        Assert.Equal("NEW", table[0][1]);
    }
}

    /// <summary>
    /// EN: Covers delete behavior when foreign keys reference the target row.
    /// PT: Cobre o comportamento de delete quando chaves estrangeiras referenciam a linha alvo.
    /// </summary>
public class MySqlDeleteStrategyForeignKeyTests
{
    /// <summary>
    /// EN: Verifies that deleting a referenced row raises an error.
    /// PT: Verifica se apagar uma linha referenciada gera erro.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void DeleteReferencedRowShouldThrow()
    {
        // Arrange parent
        var db = new MySqlDbMock();
        var parent = db.AddTable("p");
        parent.AddColumn("id", DbType.Int32, false);
        parent.AddPrimaryKeyIndexes("id");                   // marca 'id' como PK
        parent.Add(new Dictionary<int, object?> { { 0, 42 } });

        // Arrange child
        var child = db.AddTable("c");
        child.AddColumn("pid", DbType.Int32, false);
        child.CreateForeignKey("ix_parent_id", parent.TableName, [("pid", "id")]);    // c(pid) → p(id)
        child.Add(new Dictionary<int, object?> { { 0, 42 } });

        using var cnn = new MySqlConnectionMock(db);

        using var cmd = new MySqlCommandMock(cnn)
        {
            CommandText = "DELETE FROM p WHERE id = 42"
        };

        // Act & Assert
        var ex = Assert.Throws<MySqlMockException>(() => cmd.ExecuteNonQuery());
        Assert.Contains(SqlExceptionMessages.ReferencedRow(string.Empty).Split('(')[0].Trim(), ex.Message, StringComparison.OrdinalIgnoreCase);
    }
}

    /// <summary>
    /// EN: Covers extra update scenarios in the MySql mock.
    /// PT: Cobre cenarios extras de update no mock MySql.
    /// </summary>
public class MySqlUpdateStrategyExtrasTests
{
    /// <summary>
    /// EN: Verifies that UPDATE only affects rows matching all conditions.
    /// PT: Verifica se UPDATE afeta apenas linhas que satisfazem todas as condicoes.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void UpdateMultipleConditionsShouldOnlyAffectMatchingRows()
    {
        // Arrange
        var db = new MySqlDbMock();
        var table = db.AddTable("t");
        table.AddColumn("id", DbType.Int32, false);
        table.AddColumn("grp", DbType.String, false);
        table.AddColumn("val", DbType.String, false);
        table.Add(new Dictionary<int, object?> { { 0, 1 }, { 1, "X" }, { 2, "A" } });
        table.Add(new Dictionary<int, object?> { { 0, 2 }, { 1, "Y" }, { 2, "A" } });
        using var cnn = new MySqlConnectionMock(db);
        using var cmd = new MySqlCommandMock(cnn)
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
