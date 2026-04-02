namespace DbSqlLikeMem.SqlServer.Test.Strategy;
/// <summary>
/// EN: Covers extra INSERT scenarios in the SqlServer mock.
/// PT: Cobre cenarios extras de INSERT no mock SqlServer.
/// </summary>
public sealed class SqlServerInsertStrategyExtrasTests(
        ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Verifies multi-row INSERT adds every row.
    /// PT: Verifica se INSERT com multiplas linhas adiciona todas as linhas.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void MultiRowInsertShouldAddAllRows()
    {
        // Arrange
        var db = new SqlServerDbMock();
        var table = db.AddTable("t");
        table.AddColumn("id", DbType.Int32, false, identity: false);
        table.AddColumn("val", DbType.String, true);
        using var cnn = new SqlServerConnectionMock(db);
        using var cmd = new SqlServerCommandMock(cnn)
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
    /// EN: Verifies INSERT applies identity and default values when columns are omitted.
    /// PT: Verifica se INSERT aplica identidade e valores padrao quando colunas sao omitidas.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void InsertWithDefaultValueAndIdentityShouldApplyDefaults()
    {
        // Arrange
        var db = new SqlServerDbMock();
        var table = db.AddTable("t");
        table.AddColumn("id", DbType.Int32, false, identity: true);
        table.AddColumn("name", DbType.String, false, defaultValue: "DEF");
        using var cnn = new SqlServerConnectionMock(db);
        using var cmd = new SqlServerCommandMock(cnn)
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
    /// EN: Verifies INSERT throws when the primary key already exists.
    /// PT: Verifica se INSERT dispara erro quando a chave primaria ja existe.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void InsertDuplicatePrimaryKeyShouldThrow()
    {
        // Arrange
        var db = new SqlServerDbMock();
        var table = db.AddTable("t");
        table.AddColumn("id", DbType.Int32, false, identity: false);
        table.AddPrimaryKeyIndexes("id");
        using var cnn = new SqlServerConnectionMock(db);
        using var cmd = new SqlServerCommandMock(cnn)
        {
            CommandText = "INSERT INTO t (id) VALUES (1)"
        };
        cmd.ExecuteNonQuery();

        // Act & Assert
        var ex = Assert.Throws<SqlServerMockException>(() => cmd.ExecuteNonQuery());
        Assert.Contains(SqlExceptionMessages.DuplicateKey(string.Empty, string.Empty).Split('\'')[0].Trim(), ex.Message, StringComparison.OrdinalIgnoreCase);
    }
}

/// <summary>
/// EN: Covers DELETE behavior when foreign keys reference the target row.
/// PT: Cobre o comportamento de DELETE quando chaves estrangeiras referenciam a linha alvo.
/// </summary>
public class SqlServerDeleteStrategyForeignKeyTests
{
    /// <summary>
    /// EN: Verifies DELETE throws when the target row is referenced by a foreign key.
    /// PT: Verifica se DELETE dispara erro quando a linha alvo e referenciada por uma chave estrangeira.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void DeleteReferencedRowShouldThrow()
    {
        // Arrange parent
        var db = new SqlServerDbMock();
        var parent = db.AddTable("p");
        parent.AddColumn("id", DbType.Int32, false);
        parent.AddPrimaryKeyIndexes("id");                   // marca 'id' como PK
        parent.Add(new Dictionary<int, object?> { { 0, 42 } });

        // Arrange child
        var child = db.AddTable("c");
        child.AddColumn("pid", DbType.Int32, false);
        child.CreateForeignKey("ix_parent_id", parent.TableName, [("pid", "id")]);   // c(pid) → p(id)
        child.Add(new Dictionary<int, object?> { { 0, 42 } });

        using var cnn = new SqlServerConnectionMock(db);

        using var cmd = new SqlServerCommandMock(cnn)
        {
            CommandText = "DELETE FROM p WHERE id = 42"
        };

        // Act & Assert
        var ex = Assert.Throws<SqlServerMockException>(() => cmd.ExecuteNonQuery());
        Assert.Contains(SqlExceptionMessages.ReferencedRow(string.Empty).Split('(')[0].Trim(), ex.Message, StringComparison.OrdinalIgnoreCase);
    }
}

/// <summary>
/// EN: Covers extra UPDATE scenarios in the SqlServer mock.
/// PT: Cobre cenarios extras de UPDATE no mock SqlServer.
/// </summary>
public class SqlServerUpdateStrategyExtrasTests
{
    /// <summary>
    /// EN: Verifies UPDATE only affects rows that match every condition.
    /// PT: Verifica se UPDATE afeta apenas as linhas que correspondem a todas as condicoes.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void UpdateMultipleConditionsShouldOnlyAffectMatchingRows()
    {
        // Arrange
        var db = new SqlServerDbMock();
        var table = db.AddTable("t");
        table.AddColumn("id", DbType.Int32, false);
        table.AddColumn("grp", DbType.String, false);
        table.AddColumn("val", DbType.String, false);
        table.Add(new Dictionary<int, object?> { { 0, 1 }, { 1, "X" }, { 2, "A" } });
        table.Add(new Dictionary<int, object?> { { 0, 2 }, { 1, "Y" }, { 2, "A" } });
        using var cnn = new SqlServerConnectionMock(db);
        using var cmd = new SqlServerCommandMock(cnn)
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
