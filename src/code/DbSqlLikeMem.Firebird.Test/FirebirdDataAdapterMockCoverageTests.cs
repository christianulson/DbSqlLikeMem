namespace DbSqlLikeMem.Firebird.Test;

/// <summary>
/// EN: Adds focused coverage for FirebirdDataAdapterMock typed command wiring.
/// PT-br: Adiciona cobertura focada para a vinculação tipada de comandos do FirebirdDataAdapterMock.
/// </summary>
public sealed class FirebirdDataAdapterMockCoverageTests(
        ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Verifies typed command properties keep the underlying adapter command slots synchronized.
    /// PT-br: Verifica se as propriedades tipadas de comando mantem sincronizados os slots de comando do adapter.
    /// </summary>
    [Fact]
    public void TypedCommands_ShouldSynchronizeWithBaseProperties()
    {
        using var connection = new FirebirdConnectionMock(new FirebirdDbMock());
        var adapter = new FirebirdDataAdapterMock();
        var insert = new FirebirdCommandMock(connection) { CommandText = "INSERT INTO Users VALUES (1)" };
        var update = new FirebirdCommandMock(connection) { CommandText = "UPDATE Users SET Id = 1" };
        var delete = new FirebirdCommandMock(connection) { CommandText = "DELETE FROM Users WHERE Id = 1" };

        adapter.InsertCommand = insert;
        adapter.UpdateCommand = update;
        adapter.DeleteCommand = delete;

        ((IDbDataAdapter)adapter).InsertCommand.Should().BeSameAs(insert);
        ((IDbDataAdapter)adapter).UpdateCommand.Should().BeSameAs(update);
        ((IDbDataAdapter)adapter).DeleteCommand.Should().BeSameAs(delete);
    }

    /// <summary>
    /// EN: Verifies the select-command constructors set the provider-specific select command.
    /// PT-br: Verifica se os construtores com select command definem o comando select especifico do provedor.
    /// </summary>
    [Fact]
    public void Constructors_WithSelectCommand_ShouldSetTypedSelectCommand()
    {
        using var connection = new FirebirdConnectionMock(new FirebirdDbMock());

        var adapterFromText = new FirebirdDataAdapterMock("SELECT 1", connection);
        adapterFromText.SelectCommand.Should().NotBeNull();
        adapterFromText.SelectCommand!.CommandText.Should().Be("SELECT 1");

        var selectCommand = new FirebirdCommandMock(connection) { CommandText = "SELECT 2" };
        var adapterFromCommand = new FirebirdDataAdapterMock(selectCommand);
        adapterFromCommand.SelectCommand.Should().BeSameAs(selectCommand);
    }
}
