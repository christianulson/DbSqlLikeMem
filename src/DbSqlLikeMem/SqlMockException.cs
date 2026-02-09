namespace DbSqlLikeMem;

#pragma warning disable CA1032 // Implement standard exception constructors
/// <summary>
/// EN: Base exception for simulated in-memory DB errors with an error code.
/// PT: Exceção base para erros simulados do banco em memória, com código de erro.
/// </summary>
public class SqlMockException : Exception
#pragma warning restore CA1032 // Implement standard exception constructors
{
    /// <summary>
    /// EN: Gets the error code associated with the failure.
    /// PT: Obtém o código de erro associado à falha.
    /// </summary>
    public int ErrorCode { get; }
    /// <summary>
    /// EN: Initializes the exception with message and error code.
    /// PT: Inicializa a exceção com mensagem e código de erro.
    /// </summary>
    /// <param name="message">EN: Error message. PT: Mensagem do erro.</param>
    /// <param name="code">EN: Error code. PT: Código do erro.</param>
    public SqlMockException(string message, int code)
        : base(message) => ErrorCode = code;

    /// <summary>
    /// EN: Initializes the exception with default values.
    /// PT: Inicializa a exceção com valores padrão.
    /// </summary>
    public SqlMockException() : base()
    {
    }

    /// <summary>
    /// EN: Initializes the exception with an optional message.
    /// PT: Inicializa a exceção com mensagem opcional.
    /// </summary>
    /// <param name="message">EN: Error message. PT: Mensagem do erro.</param>
    public SqlMockException(string? message) : base(message)
    {
    }

    /// <summary>
    /// EN: Initializes the exception with a message and inner exception.
    /// PT: Inicializa a exceção com mensagem e exceção interna.
    /// </summary>
    /// <param name="message">EN: Error message. PT: Mensagem do erro.</param>
    /// <param name="innerException">EN: Inner exception. PT: Exceção interna.</param>
    public SqlMockException(string? message, Exception? innerException) : base(message, innerException)
    {
    }
}
