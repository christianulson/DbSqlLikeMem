namespace DbSqlLikeMem.MySql;

#pragma warning disable CA1032 // Implement standard exception constructors
#pragma warning disable RCS1194
/// <summary>
/// EN: Represents MySQL-specific mock errors.
/// PT-br: Representa erros especificos do mock de MySQL.
/// </summary>
public sealed class MySqlMockException : SqlMockException
#pragma warning restore CA1032 // Implement standard exception constructors
{
    /// <summary>
    /// EN: Creates a MySQL mock exception with a message and error code.
    /// PT-br: Cria uma excecao mock de MySQL com mensagem e codigo de erro.
    /// </summary>
    public MySqlMockException(string message, int code)
        : base(message, code)
    { }

    /// <summary>
    /// EN: Creates a default MySQL mock exception.
    /// PT-br: Cria uma excecao mock de MySQL padrao.
    /// </summary>
    public MySqlMockException() : base()
    {
    }

    /// <summary>
    /// EN: Creates a MySQL mock exception with an optional message.
    /// PT-br: Cria uma excecao mock de MySQL com mensagem opcional.
    /// </summary>
    public MySqlMockException(string? message) : base(message)
    {
    }

    /// <summary>
    /// EN: Creates a MySQL mock exception with an optional message and inner exception.
    /// PT-br: Cria uma excecao mock de MySQL com mensagem e excecao interna opcionais.
    /// </summary>
    public MySqlMockException(string? message, Exception? innerException) : base(message, innerException)
    {
    }
}
#pragma warning restore RCS1194
