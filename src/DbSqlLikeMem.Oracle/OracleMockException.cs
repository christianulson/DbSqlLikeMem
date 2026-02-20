namespace DbSqlLikeMem.Oracle;

#pragma warning disable CA1032 // Implement standard exception constructors
/// <summary>
/// EN: Summary for OracleMockException.
/// PT: Resumo para OracleMockException.
/// </summary>
public sealed class OracleMockException : SqlMockException
#pragma warning restore CA1032 // Implement standard exception constructors
{
    /// <summary>
    /// EN: Summary for OracleMockException.
    /// PT: Resumo para OracleMockException.
    /// </summary>
    public OracleMockException(string message, int code)
        : base(message, code)
    { }

    /// <summary>
    /// EN: Summary for OracleMockException.
    /// PT: Resumo para OracleMockException.
    /// </summary>
    public OracleMockException() : base()
    {
    }

    /// <summary>
    /// EN: Summary for OracleMockException.
    /// PT: Resumo para OracleMockException.
    /// </summary>
    public OracleMockException(string? message) : base(message)
    {
    }

    /// <summary>
    /// EN: Summary for OracleMockException.
    /// PT: Resumo para OracleMockException.
    /// </summary>
    public OracleMockException(string? message, Exception? innerException) : base(message, innerException)
    {
    }
}
