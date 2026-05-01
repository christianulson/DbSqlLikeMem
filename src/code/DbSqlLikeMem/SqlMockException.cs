using System.Runtime.Serialization;

namespace DbSqlLikeMem;

#pragma warning disable CA1032 // Implement standard exception constructors
/// <summary>
/// EN: Base exception for simulated in-memory DB errors with an error code.
/// PT-br: Exceção base para erros simulados do banco em memória, com código de erro.
/// </summary>
public class SqlMockException : Exception
#pragma warning restore CA1032 // Implement standard exception constructors
{
    /// <summary>
    /// EN: Gets the error code associated with the failure.
    /// PT-br: Obtém o código de erro associado à falha.
    /// </summary>
    public int ErrorCode { get; }

    /// <summary>
    /// EN: Gets the logical exception name used by WHEN EXCEPTION handlers.
    /// PT-br: Obtém o nome lógico da excecao usado pelos handlers WHEN EXCEPTION.
    /// </summary>
    public string ExceptionName { get; } = "E_FAIL";

    /// <summary>
    /// EN: Gets the SQLSTATE associated with the failure.
    /// PT-br: Obtém o SQLSTATE associado à falha.
    /// </summary>
    public string SqlState { get; protected set; } = "HY000";
    /// <summary>
    /// EN: Initializes the exception with message and error code.
    /// PT-br: Inicializa a exceção com mensagem e código de erro.
    /// </summary>
    /// <param name="message">EN: Error message. PT-br: Mensagem do erro.</param>
    /// <param name="code">EN: Error code. PT-br: Código do erro.</param>
    public SqlMockException(string message, int code)
        : base(message) => ErrorCode = code;

    /// <summary>
    /// EN: Initializes the exception with a message, error code, and SQLSTATE.
    /// PT-br: Inicializa a exceção com mensagem, código de erro e SQLSTATE.
    /// </summary>
    /// <param name="message">EN: Error message. PT-br: Mensagem do erro.</param>
    /// <param name="code">EN: Error code. PT-br: Código do erro.</param>
    /// <param name="sqlState">EN: SQLSTATE value. PT-br: Valor do SQLSTATE.</param>
    public SqlMockException(string message, int code, string sqlState)
        : base(message)
    {
        ErrorCode = code;
        SqlState = string.IsNullOrWhiteSpace(sqlState) ? SqlState : sqlState;
    }

    /// <summary>
    /// EN: Initializes the exception with default values.
    /// PT-br: Inicializa a exceção com valores padrão.
    /// </summary>
    public SqlMockException() : base()
    {
    }

    /// <summary>
    /// EN: Initializes the exception with an optional message.
    /// PT-br: Inicializa a exceção com mensagem opcional.
    /// </summary>
    /// <param name="message">EN: Error message. PT-br: Mensagem do erro.</param>
    public SqlMockException(string? message) : base(message)
    {
    }

    /// <summary>
    /// EN: Initializes the exception with a message and inner exception.
    /// PT-br: Inicializa a exceção com mensagem e exceção interna.
    /// </summary>
    /// <param name="message">EN: Error message. PT-br: Mensagem do erro.</param>
    /// <param name="innerException">EN: Inner exception. PT-br: Exceção interna.</param>
    public SqlMockException(string? message, Exception? innerException) : base(message, innerException)
    {
    }

    /// <summary>
    /// EN: Initializes the exception from serialized data.
    /// PT-br: Inicializa a exceção a partir de dados serializados.
    /// </summary>
    /// <param name="info">EN: Serialization information. PT-br: Informações de serialização.</param>
    /// <param name="context">EN: Streaming context. PT-br: Contexto de streaming.</param>
#pragma warning disable SYSLIB0051 // This API supports obsolete formatter-based serialization.
    protected SqlMockException(SerializationInfo info, StreamingContext context)
        : base(info, context)
    {
    }
#pragma warning restore SYSLIB0051 // This API supports obsolete formatter-based serialization.
}
