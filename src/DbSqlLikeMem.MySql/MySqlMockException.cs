namespace DbSqlLikeMem.MySql;

#pragma warning disable CA1032 // Implement standard exception constructors
/// <summary>
/// EN: Defines the class MySqlMockException.
/// PT: Define a classe MySqlMockException.
/// </summary>
public sealed class MySqlMockException : SqlMockException
#pragma warning restore CA1032 // Implement standard exception constructors
{
    /// <summary>
    /// EN: Implements MySqlMockException.
    /// PT: Implementa MySqlMockException.
    /// </summary>
    public MySqlMockException(string message, int code)
        : base(message, code)
    { }

    /// <summary>
    /// EN: Implements MySqlMockException.
    /// PT: Implementa MySqlMockException.
    /// </summary>
    public MySqlMockException() : base()
    {
    }

    /// <summary>
    /// EN: Implements MySqlMockException.
    /// PT: Implementa MySqlMockException.
    /// </summary>
    public MySqlMockException(string? message) : base(message)
    {
    }

    /// <summary>
    /// EN: Implements MySqlMockException.
    /// PT: Implementa MySqlMockException.
    /// </summary>
    public MySqlMockException(string? message, Exception? innerException) : base(message, innerException)
    {
    }
}
