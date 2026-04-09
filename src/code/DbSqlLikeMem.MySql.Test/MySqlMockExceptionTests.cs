namespace DbSqlLikeMem.MySql.Test;

/// <summary>
/// EN: Validates MySqlMockException constructors preserve message, code, and inner exception data.
/// PT: Valida se os construtores de MySqlMockException preservam mensagem, codigo e excecao interna.
/// </summary>
public sealed class MySqlMockExceptionTests(
        ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Verifies the error-code constructor keeps the provided message and SQL error code.
    /// PT: Verifica se o construtor com codigo de erro preserva a mensagem informada e o codigo SQL.
    /// </summary>
    [Fact]
    public void Constructor_WithMessageAndCode_ShouldPreserveValues()
    {
        var exception = new MySqlMockException("duplicate key", 1062);

        exception.Message.Should().Be("duplicate key");
        exception.ErrorCode.Should().Be(1062);
    }

    /// <summary>
    /// EN: Verifies the message and inner-exception constructor preserves both values.
    /// PT: Verifica se o construtor com mensagem e excecao interna preserva ambos os valores.
    /// </summary>
    [Fact]
    public void Constructor_WithMessageAndInnerException_ShouldPreserveValues()
    {
        var inner = new InvalidOperationException("inner");

        var exception = new MySqlMockException("outer", inner);

        exception.Message.Should().Be("outer");
        exception.InnerException.Should().BeSameAs(inner);
    }

    /// <summary>
    /// EN: Verifies the parameterless and message-only constructors keep the default code contract.
    /// PT: Verifica se os construtores sem parametros e apenas com mensagem mantem o contrato do codigo padrao.
    /// </summary>
    [Fact]
    public void Constructors_WithoutCode_ShouldKeepDefaultCode()
    {
        var parameterless = new MySqlMockException();
        var messageOnly = new MySqlMockException("only message");

        parameterless.ErrorCode.Should().Be(0);
        messageOnly.ErrorCode.Should().Be(0);
        messageOnly.Message.Should().Be("only message");
    }
}
