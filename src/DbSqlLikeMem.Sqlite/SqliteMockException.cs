namespace DbSqlLikeMem.Sqlite;

#pragma warning disable CA1032 // Implement standard exception constructors
/// <summary>
/// EN: Summary for SqliteMockException.
/// PT: Resumo para SqliteMockException.
/// </summary>
public sealed class SqliteMockException : SqlMockException
#pragma warning restore CA1032 // Implement standard exception constructors
{
    /// <summary>
    /// EN: Summary for SqliteMockException.
    /// PT: Resumo para SqliteMockException.
    /// </summary>
    public SqliteMockException(string message, int code)
        : base(message, code)
    { }

    /// <summary>
    /// EN: Summary for SqliteMockException.
    /// PT: Resumo para SqliteMockException.
    /// </summary>
    public SqliteMockException() : base()
    {
    }

    /// <summary>
    /// EN: Summary for SqliteMockException.
    /// PT: Resumo para SqliteMockException.
    /// </summary>
    public SqliteMockException(string? message) : base(message)
    {
    }

    /// <summary>
    /// EN: Summary for SqliteMockException.
    /// PT: Resumo para SqliteMockException.
    /// </summary>
    public SqliteMockException(string? message, Exception? innerException) : base(message, innerException)
    {
    }
}
