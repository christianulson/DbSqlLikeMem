namespace DbSqlLikeMem.Firebird.Test;

/// <summary>
/// EN: Contains tests for the Firebird mock exception surface.
/// PT-br: Contem testes para a superficie da excecao simulada Firebird.
/// </summary>
public sealed class FirebirdMockExceptionTests(
        ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Verifies the message-and-code constructor preserves the provided values.
    /// PT-br: Verifica se o construtor com mensagem e codigo preserva os valores informados.
    /// </summary>
    [Fact]
    public void Constructor_WithMessageAndCode_ShouldPreserveValues()
    {
        var exception = new FirebirdMockException("duplicate key", 1062);

        exception.Message.Should().Be("duplicate key");
        exception.ErrorCode.Should().Be(1062);
    }
}
