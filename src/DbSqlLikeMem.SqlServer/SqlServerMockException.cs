namespace DbSqlLikeMem.SqlServer;

#pragma warning disable CA1032 // Implement standard exception constructors
/// <summary>
/// EN: Summary for SqlServerMockException.
/// PT: Resumo para SqlServerMockException.
/// </summary>
public sealed class SqlServerMockException : SqlMockException
#pragma warning restore CA1032 // Implement standard exception constructors
{
    /// <summary>
    /// EN: Summary for SqlServerMockException.
    /// PT: Resumo para SqlServerMockException.
    /// </summary>
    public SqlServerMockException(string message, int code)
        : base(message, code) { }

    /// <summary>
    /// EN: Summary for SqlServerMockException.
    /// PT: Resumo para SqlServerMockException.
    /// </summary>
    public SqlServerMockException() : base()
    {
    }

    /// <summary>
    /// EN: Summary for SqlServerMockException.
    /// PT: Resumo para SqlServerMockException.
    /// </summary>
    public SqlServerMockException(string? message) : base(message)
    {
    }

    /// <summary>
    /// EN: Summary for SqlServerMockException.
    /// PT: Resumo para SqlServerMockException.
    /// </summary>
    public SqlServerMockException(string? message, Exception? innerException) : base(message, innerException)
    {
    }
}
