namespace DbSqlLikeMem.Oracle;

#pragma warning disable CA1032 // Implement standard exception constructors
public sealed class OracleMockException : SqlMockException
#pragma warning restore CA1032 // Implement standard exception constructors
{
    public OracleMockException(string message, int code)
        : base(message, code)
    { }

    public OracleMockException() : base()
    {
    }

    public OracleMockException(string? message) : base(message)
    {
    }

    public OracleMockException(string? message, Exception? innerException) : base(message, innerException)
    {
    }
}