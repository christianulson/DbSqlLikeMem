namespace DbSqlLikeMem;

#pragma warning disable CA1032 // Implement standard exception constructors
/// <summary>
/// Exceção base para erros simulados do banco em memória, com código de erro.
/// </summary>
public class SqlMockException : Exception
#pragma warning restore CA1032 // Implement standard exception constructors
{
    /// <summary>
    /// Obtém o código de erro associado à falha.
    /// </summary>
    public int ErrorCode { get; }
    /// <summary>
    /// Inicializa a exceção com mensagem e código de erro.
    /// </summary>
    /// <param name="message">Mensagem do erro.</param>
    /// <param name="code">Código do erro.</param>
    public SqlMockException(string message, int code)
        : base(message) => ErrorCode = code;

    /// <summary>
    /// Inicializa a exceção com valores padrão.
    /// </summary>
    public SqlMockException() : base()
    {
    }

    /// <summary>
    /// Inicializa a exceção com mensagem opcional.
    /// </summary>
    /// <param name="message">Mensagem do erro.</param>
    public SqlMockException(string? message) : base(message)
    {
    }

    /// <summary>
    /// Inicializa a exceção com mensagem e exceção interna.
    /// </summary>
    /// <param name="message">Mensagem do erro.</param>
    /// <param name="innerException">Exceção interna.</param>
    public SqlMockException(string? message, Exception? innerException) : base(message, innerException)
    {
    }
}
