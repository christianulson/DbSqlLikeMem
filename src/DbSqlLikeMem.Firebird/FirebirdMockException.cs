using System.Runtime.Serialization;

namespace DbSqlLikeMem.Firebird;

/// <summary>
/// EN: Represents Firebird Mock Exception.
/// PT: Representa a excecao simulada do Firebird.
/// </summary>
public sealed class FirebirdMockException : SqlMockException
{
    /// <summary>
    /// EN: Initializes the exception with default values.
    /// PT: Inicializa a exceção com valores padrão.
    /// </summary>
    public FirebirdMockException()
    {
    }

    /// <summary>
    /// EN: Initializes the exception with a message.
    /// PT: Inicializa a exceção com uma mensagem.
    /// </summary>
    /// <param name="message">EN: Error message. PT: Mensagem do erro.</param>
    public FirebirdMockException(string? message)
        : base(message)
    {
    }

    /// <summary>
    /// EN: Initializes the exception with a message and inner exception.
    /// PT: Inicializa a exceção com uma mensagem e uma exceção interna.
    /// </summary>
    /// <param name="message">EN: Error message. PT: Mensagem do erro.</param>
    /// <param name="innerException">EN: Inner exception. PT: Exceção interna.</param>
    public FirebirdMockException(string? message, Exception? innerException)
        : base(message, innerException)
    {
    }

    /// <summary>
    /// EN: Initializes the exception with a message and error code.
    /// PT: Inicializa a exceção com uma mensagem e um código de erro.
    /// </summary>
    public FirebirdMockException(string message, int code)
        : base(message, code)
    {
    }

    /// <summary>
    /// EN: Initializes the exception from serialized data.
    /// PT: Inicializa a exceção a partir de dados serializados.
    /// </summary>
    /// <param name="info">EN: Serialization information. PT: Informações de serialização.</param>
    /// <param name="context">EN: Streaming context. PT: Contexto de streaming.</param>
#pragma warning disable SYSLIB0051 // This API supports obsolete formatter-based serialization.
    private FirebirdMockException(SerializationInfo info, StreamingContext context)
        : base(info, context)
    {
    }
#pragma warning restore SYSLIB0051 // This API supports obsolete formatter-based serialization.
}

