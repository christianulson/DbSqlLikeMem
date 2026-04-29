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
        affected.Should().Be(3);
        table.Count.Should().Be(3);
        table[1][1].Should().Be("B");
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
        affected1.Should().Be(1);
        affected2.Should().Be(1);
        table.Count.Should().Be(2);
        table[0][0].Should().Be(1);
        table[0][1].Should().Be("DEF");
        table[1][0].Should().Be(2);
    }

    /// <summary>
    /// EN: Verifies AddItem keeps omitted properties so column defaults are applied.
    /// PT: Verifica se AddItem preserva propriedades omitidas para aplicar defaults de coluna.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void AddItemMissingPropertyShouldUseColumnDefaultValue()
    {
        // Arrange
        var db = new MySqlDbMock();
        var table = db.AddTable("t");
        table.AddColumn("id", DbType.Int32, false, defaultValue: 1);
        table.AddColumn("name", DbType.String, false);
        table.AddColumn("failed", DbType.Int16, false, defaultValue: 0);

        // Act
        table.AddItem(new { name = "Ana" });

        // Assert
        table.Should().ContainSingle();
        table[0][0].Should().Be(1);
        table[0][1].Should().Be("Ana");
        table[0][2].Should().Be(0);
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
        Action act = () => cmd.ExecuteNonQuery();
        act.Should().Throw<MySqlMockException>()
            .Which.Message.Should().Contain(SqlExceptionMessages.DuplicateKey(string.Empty, string.Empty).Split('\'')[0].Trim());
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

        affected.Should().Be(2);
        table.Should().ContainSingle();
        table[0][1].Should().Be("NEW");
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
        Action act = () => cmd.ExecuteNonQuery();
        act.Should().Throw<MySqlMockException>()
            .Which.Message.Should().Contain(SqlExceptionMessages.ReferencedRow(string.Empty).Split('(')[0].Trim());
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
        affected.Should().Be(1);
        table[0][2].Should().Be("Z");
        table[1][2].Should().Be("A");
    }
}
