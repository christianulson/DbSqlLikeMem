namespace DbSqlLikeMem.Firebird.Test;

/// <summary>
/// EN: Contains tests for Firebird connection mock surface behavior.
/// PT: Contem testes para o comportamento de superficie do mock de conexao Firebird.
/// </summary>
public sealed class FirebirdConnectionMockTests(
        ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Verifies the connection opens and closes safely, creates provider-specific commands, and starts provider-specific transactions.
    /// PT: Verifica se a conexao abre e fecha com seguranca, cria comandos especificos do provedor e inicia transacoes especificas do provedor.
    /// </summary>
    [Fact]
    public void OpenCloseAndFactoryMembers_ShouldUseProviderSpecificTypes()
    {
        var connection = new FirebirdConnectionMock(new FirebirdDbMock());

        connection.State.Should().Be(ConnectionState.Closed);
        connection.ServerVersion.Should().Contain("Firebird");

        connection.Open();
        connection.State.Should().Be(ConnectionState.Open);
        connection.CreateCommand().Should().BeOfType<FirebirdCommandMock>();

        using var transaction = connection.BeginTransaction();
        transaction.Should().BeOfType<FirebirdTransactionMock>();

        connection.Close();
        connection.State.Should().Be(ConnectionState.Closed);
    }
}
