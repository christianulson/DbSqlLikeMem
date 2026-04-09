namespace DbSqlLikeMem.MySql.Test;

/// <summary>
/// EN: Validates collection operations exposed by MySqlBatchCommandCollectionMock.
/// PT: Valida as operacoes de colecao expostas por MySqlBatchCommandCollectionMock.
/// </summary>
public sealed class MySqlBatchCommandCollectionMockTests(
        ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    private sealed class TestableMySqlBatchCommandCollectionMock : MySqlBatchCommandCollectionMock
    {
        public MySqlBatchCommandMock GetAt(int index) => (MySqlBatchCommandMock)GetBatchCommand(index);

        public void SetAt(int index, MySqlBatchCommandMock command) => SetBatchCommand(index, command);
    }

    /// <summary>
    /// EN: Verifies add, insert, remove, clear, and lookup operations preserve the expected command order.
    /// PT: Verifica se as operacoes de adicionar, inserir, remover, limpar e consultar preservam a ordem esperada dos comandos.
    /// </summary>
    [Fact]
    public void CollectionOperations_ShouldTrackCommandsByOrderAndText()
    {
        var collection = new TestableMySqlBatchCommandCollectionMock();
        var first = new MySqlBatchCommandMock("SELECT 1");
        var second = new MySqlBatchCommandMock("SELECT 2");
        var third = new MySqlBatchCommandMock("SELECT 3");

        collection.Add(first);
        collection.Insert(1, third);
        collection.Insert(1, second);

        collection.Count.Should().Be(3);
        collection.IsReadOnly.Should().BeFalse();
        collection.Contains(new MySqlBatchCommandMock("SELECT 2")).Should().BeTrue();
        collection.IndexOf(second).Should().Be(1);
        collection.GetAt(0).CommandText.Should().Be("SELECT 1");
        collection.GetAt(1).CommandText.Should().Be("SELECT 2");
        collection.GetAt(2).CommandText.Should().Be("SELECT 3");

        collection.SetAt(1, new MySqlBatchCommandMock("SELECT 20"));
        collection.GetAt(1).CommandText.Should().Be("SELECT 20");

        collection.RemoveAt(0);
        collection.Remove(third).Should().BeTrue();
        collection.Count.Should().Be(1);
        collection.GetAt(0).CommandText.Should().Be("SELECT 20");

        collection.Clear();
        collection.Count.Should().Be(0);
    }

    /// <summary>
    /// EN: Verifies enumeration and array copy expose the stored commands.
    /// PT: Verifica se a enumeracao e a copia para array expõem os comandos armazenados.
    /// </summary>
    [Fact]
    public void EnumerationAndCopyTo_ShouldExposeStoredCommands()
    {
        var collection = new TestableMySqlBatchCommandCollectionMock();
        collection.Add(new MySqlBatchCommandMock("SELECT 1"));
        collection.Add(new MySqlBatchCommandMock("SELECT 2"));

        var commands = Enumerable.Range(0, collection.Count)
            .Select(collection.GetAt)
            .Select(static c => c.CommandText)
            .ToArray();
        var array = new MySqlBatchCommandMock[2];
        collection.CopyTo(array, 0);

        commands.Should().Equal("SELECT 1", "SELECT 2");
        array.Select(static c => c.CommandText).Should().Equal("SELECT 1", "SELECT 2");
    }
}
