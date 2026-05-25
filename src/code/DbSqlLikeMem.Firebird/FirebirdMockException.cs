using System.Runtime.Serialization;

namespace DbSqlLikeMem.Firebird;

/// <summary>
/// EN: Represents Firebird Mock Exception.
/// PT-br: Representa a excecao simulada do Firebird.
/// </summary>
public sealed class FirebirdMockException : SqlMockException
{
    /// <summary>
    /// EN: Initializes the exception with default values.
    /// PT-br: Inicializa a exceção com valores padrão.
    /// </summary>
    public FirebirdMockException()
    {
    }

    /// <summary>
    /// EN: Initializes the exception with a message.
    /// PT-br: Inicializa a exceção com uma mensagem.
    /// </summary>
    /// <param name="message">EN: Error message. PT-br: Mensagem do erro.</param>
    public FirebirdMockException(string? message)
        : base(message)
    {
    }

    /// <summary>
    /// EN: Initializes the exception with a message and inner exception.
    /// PT-br: Inicializa a exceção com uma mensagem e uma exceção interna.
    /// </summary>
    /// <param name="message">EN: Error message. PT-br: Mensagem do erro.</param>
    /// <param name="innerException">EN: Inner exception. PT-br: Exceção interna.</param>
    public FirebirdMockException(string? message, Exception? innerException)
        : base(message, innerException)
    {
    }

    /// <summary>
    /// EN: Initializes the exception with a message and error code.
    /// PT-br: Inicializa a exceção com uma mensagem e um código de erro.
    /// </summary>
    public FirebirdMockException(string message, int code)
        : base(message, code)
    {
    }

    /// <summary>
    /// EN: Initializes the exception with a message, error code, and SQLSTATE.
    /// PT-br: Inicializa a exceção com uma mensagem, um código de erro e um SQLSTATE.
    /// </summary>
    /// <param name="message">EN: Error message. PT-br: Mensagem do erro.</param>
    /// <param name="code">EN: Error code. PT-br: Código do erro.</param>
    /// <param name="sqlState">EN: SQLSTATE value. PT-br: Valor do SQLSTATE.</param>
    public FirebirdMockException(string message, int code, string sqlState)
        : base(message, code)
    {
        SqlState = string.IsNullOrWhiteSpace(sqlState) ? "HY000" : sqlState;
    }

    /// <summary>
    /// EN: Initializes the exception from serialized data.
    /// PT-br: Inicializa a exceção a partir de dados serializados.
    /// </summary>
    /// <param name="info">EN: Serialization information. PT-br: Informações de serialização.</param>
    /// <param name="context">EN: Streaming context. PT-br: Contexto de streaming.</param>
#pragma warning disable SYSLIB0051 // This API supports obsolete formatter-based serialization.
    private FirebirdMockException(SerializationInfo info, StreamingContext context)
        : base(info, context)
    {
    }
#pragma warning restore SYSLIB0051 // This API supports obsolete formatter-based serialization.
}
