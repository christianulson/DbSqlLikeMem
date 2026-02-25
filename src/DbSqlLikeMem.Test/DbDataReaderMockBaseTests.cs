namespace DbSqlLikeMem.Test;

/// <summary>
/// EN: Tests core binary/char/nested-reader APIs of <see cref="DbDataReaderMockBase"/>.
/// PT: Testa APIs centrais de binário/char/leitor aninhado de <see cref="DbDataReaderMockBase"/>.
/// </summary>
public sealed class DbDataReaderMockBaseTests
{
    [Fact]
    [Trait("Category", "Core")]
    public void GetBytes_ShouldReturnLength_WhenBufferIsNull()
    {
        using var reader = CreateReader(new Dictionary<int, object?> { [0] = new byte[] { 1, 2, 3, 4 } }, DbType.Binary);
        Assert.True(reader.Read());

        var total = reader.GetBytes(0, 0, null, 0, 0);

        Assert.Equal(4, total);
    }

    [Fact]
    [Trait("Category", "Core")]
    public void GetBytes_ShouldCopyRequestedSegment()
    {
        using var reader = CreateReader(new Dictionary<int, object?> { [0] = new byte[] { 10, 11, 12, 13 } }, DbType.Binary);
        Assert.True(reader.Read());
        var buffer = new byte[3];

        var copied = reader.GetBytes(0, 1, buffer, 0, 3);

        Assert.Equal(3, copied);
        Assert.Equal(new byte[] { 11, 12, 13 }, buffer);
    }

    [Fact]
    [Trait("Category", "Core")]
    public void GetChars_ShouldCopyRequestedSegment()
    {
        using var reader = CreateReader(new Dictionary<int, object?> { [0] = "abcdef" }, DbType.String);
        Assert.True(reader.Read());
        var buffer = new char[3];

        var copied = reader.GetChars(0, 2, buffer, 0, 3);

        Assert.Equal(3, copied);
        Assert.Equal("cde", new string(buffer));
    }



    [Fact]
    [Trait("Category", "Core")]
    public void GetOrdinal_ShouldResolveBracketQuotedColumnName()
    {
        var table = new TableResultMock
        {
            Columns = [new TableResultColMock("u", "[User Name]", "[User Name]", 0, DbType.String, true)]
        };
        table.Add(new Dictionary<int, object?> { [0] = "Alice" });
        using var reader = new TestDbDataReaderMock([table]);
        Assert.True(reader.Read());

        var ordinal = reader.GetOrdinal("User Name");

        Assert.Equal(0, ordinal);
        Assert.Equal("Alice", reader.GetString(ordinal));
    }

    [Fact]
    [Trait("Category", "Core")]
    public void GetData_ShouldReturnNestedReader_WhenColumnContainsReader()
    {
        using var nested = CreateReader(new Dictionary<int, object?> { [0] = 42 }, DbType.Int32);
        using var outer = CreateReader(new Dictionary<int, object?> { [0] = nested }, DbType.Object);
        Assert.True(outer.Read());

        var read = outer.GetData(0);

        Assert.Same(nested, read);
    }

    [Fact]
    [Trait("Category", "Core")]
    public void GetData_ShouldThrowInvalidCast_WhenColumnDoesNotContainReader()
    {
        using var outer = CreateReader(new Dictionary<int, object?> { [0] = "not a reader" }, DbType.String);
        Assert.True(outer.Read());

        Assert.Throws<InvalidCastException>(() => outer.GetData(0));
    }



    [Fact]
    [Trait("Category", "Core")]
    public void GetValues_ShouldCopyOnlyDestinationLength_WhenArrayIsSmallerThanFieldCount()
    {
        var table = new TableResultMock
        {
            Columns =
            [
                new TableResultColMock("t", "c0", "c0", 0, DbType.Int32, true),
                new TableResultColMock("t", "c1", "c1", 1, DbType.String, true)
            ]
        };
        table.Add(new Dictionary<int, object?> { [0] = 11, [1] = "hello" });
        using var reader = new TestDbDataReaderMock([table]);
        Assert.True(reader.Read());

        var values = new object[1];
        var copied = reader.GetValues(values);

        Assert.Equal(1, copied);
        Assert.Equal(11, values[0]);
    }

    [Fact]
    [Trait("Category", "Core")]
    public void Dispose_ShouldCloseReader_AndDisposeNestedResources()
    {
        var nested = CreateReader(new Dictionary<int, object?> { [0] = 7 }, DbType.Int32);
        var outer = CreateReader(new Dictionary<int, object?> { [0] = nested }, DbType.Object);
        Assert.True(outer.Read());

        outer.Dispose();

        Assert.True(outer.IsClosed);
        Assert.True(nested.IsClosed);
    }


    private static TestDbDataReaderMock CreateReader(Dictionary<int, object?> row, DbType dbType)
    {
        var table = new TableResultMock
        {
            Columns = [new TableResultColMock("t", "c0", "c0", 0, dbType, true)]
        };
        table.Add(row);
        return new TestDbDataReaderMock([table]);
    }

    private sealed class TestDbDataReaderMock(IList<TableResultMock> tables) : DbDataReaderMockBase(tables)
    {
    }
}
