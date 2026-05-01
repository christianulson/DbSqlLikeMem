namespace DbSqlLikeMem.Firebird.Test;

/// <summary>
/// EN: Contains tests for Firebird exception surface behavior.
/// PT-br: Contem testes para o comportamento da superficie de excecoes Firebird.
/// </summary>
public sealed class FirebirdExceptionSurfaceTests(
        ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Verifies duplicate primary key violations use the Firebird-specific mock exception type.
    /// PT-br: Verifica se violacoes de chave primaria duplicada usam o tipo de excecao especifico do Firebird.
    /// </summary>
    [Fact]
    public void Add_DuplicatePrimaryKey_ShouldThrowFirebirdMockException()
    {
        var db = new FirebirdDbMock();
        var table = db.AddTable("users");
        table.AddColumn("id", DbType.Int32, false);
        table.AddPrimaryKeyIndexes("id");
        table.Add(new Dictionary<int, object?> { [0] = 1 });

        var ex = Assert.Throws<FirebirdMockException>(() => table.Add(new Dictionary<int, object?> { [0] = 1 }));
        ex.ErrorCode.Should().Be(1062);
    }

    /// <summary>
    /// EN: Verifies nullability violations use the Firebird-specific mock exception type.
    /// PT-br: Verifica se violacoes de nulabilidade usam o tipo de excecao especifico do Firebird.
    /// </summary>
    [Fact]
    public void Add_NullIntoNotNullableColumn_ShouldThrowFirebirdMockException()
    {
        var db = new FirebirdDbMock();
        var table = db.AddTable("users");
        table.AddColumn("id", DbType.Int32, false);

        var ex = Assert.Throws<FirebirdMockException>(() => table.Add([]));
        ex.ErrorCode.Should().Be(1048);
    }

    /// <summary>
    /// EN: Verifies foreign key violations use the Firebird-specific mock exception type.
    /// PT-br: Verifica se violacoes de chave estrangeira usam o tipo de excecao especifico do Firebird.
    /// </summary>
    [Fact]
    public void Add_ForeignKeyViolation_ShouldThrowFirebirdMockException()
    {
        var db = new FirebirdDbMock();
        var parents = db.AddTable("parents");
        parents.AddColumn("id", DbType.Int32, false);
        parents.Add(new Dictionary<int, object?> { [0] = 1 });

        var children = db.AddTable("children");
        children.AddColumn("parentid", DbType.Int32, false);
        children.CreateForeignKey(
            "fk_children_parent",
            "parents",
            [("parentid", "id")]);

        var ex = Assert.Throws<FirebirdMockException>(() => children.Add(new Dictionary<int, object?> { [0] = 999 }));
        ex.ErrorCode.Should().Be(1452);
    }

    /// <summary>
    /// EN: Verifies deleting a referenced parent row uses the Firebird-specific mock exception type.
    /// PT-br: Verifica se excluir uma linha pai referenciada usa o tipo de excecao especifico do Firebird.
    /// </summary>
    [Fact]
    public void Delete_ReferencedParentRow_ShouldThrowFirebirdMockException()
    {
        var db = new FirebirdDbMock();
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

        var ex = Assert.Throws<FirebirdMockException>(() => parent.RemoveAt(0));
        ex.ErrorCode.Should().Be(1451);
    }
}
