namespace DbSqlLikeMem.SqlAzure.Test;

/// <summary>
/// EN: Covers SQL Azure exception surface on table-level operations.
/// PT: Cobre a superfície de exceções do SQL Azure em operações de nível de tabela.
/// </summary>
public sealed class SqlAzureMockExceptionSurfaceTests
{
    /// <summary>
    /// EN: Ensures unknown column errors use SQL Azure exception type.
    /// PT: Garante que erros de coluna inexistente usem o tipo de exceção SQL Azure.
    /// </summary>
    [Fact]
    public void GetColumn_UnknownColumn_ShouldThrowSqlAzureMockException()
    {
        var db = new SqlAzureDbMock();
        var table = db.AddTable("users");
        table.AddColumn("id", DbType.Int32, false);

        var ex = Assert.Throws<SqlAzureMockException>(() => table.GetColumn("missing_col"));
        ex.ErrorCode.Should().Be(1054);
    }

    /// <summary>
    /// EN: Ensures duplicate key violations throw SQL Azure exception type and code.
    /// PT: Garante que violações de chave duplicada lancem tipo e código de exceção SQL Azure.
    /// </summary>
    [Fact]
    public void Add_DuplicatePrimaryKey_ShouldThrowSqlAzureMockException()
    {
        var db = new SqlAzureDbMock();
        var table = db.AddTable("users");
        table.AddColumn("id", DbType.Int32, false);
        table.AddPrimaryKeyIndexes("id");
        table.Add(new Dictionary<int, object?> { [0] = 1 });

        var ex = Assert.Throws<SqlAzureMockException>(() => table.Add(new Dictionary<int, object?> { [0] = 1 }));
        ex.ErrorCode.Should().Be(1062);
    }

    /// <summary>
    /// EN: Ensures inserting null into non-nullable column throws SQL Azure exception type and code.
    /// PT: Garante que inserir nulo em coluna obrigatória lance tipo e código de exceção SQL Azure.
    /// </summary>
    [Fact]
    public void Add_NullIntoNotNullableColumn_ShouldThrowSqlAzureMockException()
    {
        var db = new SqlAzureDbMock();
        var table = db.AddTable("users");
        table.AddColumn("id", DbType.Int32, false);

        var ex = Assert.Throws<SqlAzureMockException>(() => table.Add([]));
        ex.ErrorCode.Should().Be(1048);
    }

    /// <summary>
    /// EN: Ensures foreign key violations throw SQL Azure exception type and code.
    /// PT: Garante que violações de chave estrangeira lancem tipo e código de exceção SQL Azure.
    /// </summary>
    [Fact]
    public void Add_ForeignKeyViolation_ShouldThrowSqlAzureMockException()
    {
        var db = new SqlAzureDbMock();
        var parents = db.AddTable("parents");
        parents.AddColumn("id", DbType.Int32, false);
        parents.Add(new Dictionary<int, object?> { [0] = 1 });

        var children = db.AddTable("children");
        children.AddColumn("parentid", DbType.Int32, false);
        children.CreateForeignKey(
            "fk_children_parent",
            "parents",
            [("parentid", "id")]);

        var ex = Assert.Throws<SqlAzureMockException>(() => children.Add(new Dictionary<int, object?> { [0] = 999 }));
        ex.ErrorCode.Should().Be(1452);
    }

    /// <summary>
    /// EN: Ensures deleting a referenced parent row throws SQL Azure exception type and referenced-row code.
    /// PT: Garante que excluir linha pai referenciada lance tipo de exceção SQL Azure e código de linha referenciada.
    /// </summary>
    [Fact]
    public void Delete_ReferencedParentRow_ShouldThrowSqlAzureMockException()
    {
        var db = new SqlAzureDbMock();
        var parent = db.AddTable("parents");
        parent.AddColumn("id", DbType.Int32, false);
        parent.AddPrimaryKeyIndexes("id");
        parent.Add(new Dictionary<int, object?> { [0] = 42 });

        var child = db.AddTable("children");
        child.AddColumn("parentid", DbType.Int32, false);
        child.CreateForeignKey(
            "fk_children_parent",
            "parents",
            [("parentid", "id")]);
        child.Add(new Dictionary<int, object?> { [0] = 42 });

        using var connection = new SqlAzureConnectionMock(db);
        using var command = new SqlAzureCommandMock(connection)
        {
            CommandText = "DELETE FROM parents WHERE id = 42"
        };

        var ex = Assert.Throws<SqlAzureMockException>(() => command.ExecuteNonQuery());
        ex.ErrorCode.Should().Be(1451);
    }
}
