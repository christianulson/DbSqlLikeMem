namespace DbSqlLikeMem.Sqlite;

#pragma warning disable CA1032 // Implement standard exception constructors
/// <summary>
/// EN: Represents Sqlite Mock Exception.
/// PT: Representa uma exceção simulada do Sqlite.
/// </summary>
public sealed class SqliteMockException : SqlMockException
#pragma warning restore CA1032 // Implement standard exception constructors
{
    /// <summary>
    /// EN: Represents Sqlite Mock Exception.
    /// PT: Representa uma exceção simulada do Sqlite.
    /// </summary>
    public SqliteMockException(string message, int code)
        : base(message, code)
    { }

    /// <summary>
    /// EN: Represents Sqlite Mock Exception.
    /// PT: Representa uma exceção simulada do Sqlite.
    /// </summary>
    public SqliteMockException() : base()
    {
    }

    /// <summary>
    /// EN: Represents Sqlite Mock Exception.
    /// PT: Representa uma exceção simulada do Sqlite.
    /// </summary>
    public SqliteMockException(string? message) : base(message)
    {
    }

    /// <summary>
    /// EN: Represents Sqlite Mock Exception.
    /// PT: Representa uma exceção simulada do Sqlite.
    /// </summary>
    public SqliteMockException(string? message, Exception? innerException) : base(message, innerException)
    {
    }
}
