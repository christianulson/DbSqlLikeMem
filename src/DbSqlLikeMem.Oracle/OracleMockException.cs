namespace DbSqlLikeMem.Oracle;

#pragma warning disable CA1032 // Implement standard exception constructors
/// <summary>
/// EN: Represents Oracle Mock Exception.
/// PT: Representa Oracle simulada Exceção.
/// </summary>
public sealed class OracleMockException : SqlMockException
#pragma warning restore CA1032 // Implement standard exception constructors
{
    /// <summary>
    /// EN: Represents Oracle Mock Exception.
    /// PT: Representa Oracle simulada Exceção.
    /// </summary>
    public OracleMockException(string message, int code)
        : base(message, code)
    { }

    /// <summary>
    /// EN: Represents Oracle Mock Exception.
    /// PT: Representa Oracle simulada Exceção.
    /// </summary>
    public OracleMockException() : base()
    {
    }

    /// <summary>
    /// EN: Represents Oracle Mock Exception.
    /// PT: Representa Oracle simulada Exceção.
    /// </summary>
    public OracleMockException(string? message) : base(message)
    {
    }

    /// <summary>
    /// EN: Represents Oracle Mock Exception.
    /// PT: Representa Oracle simulada Exceção.
    /// </summary>
    public OracleMockException(string? message, Exception? innerException) : base(message, innerException)
    {
    }
}
