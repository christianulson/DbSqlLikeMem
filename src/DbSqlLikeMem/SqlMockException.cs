namespace DbSqlLikeMem;

#pragma warning disable CA1032 // Implement standard exception constructors
public class SqlMockException : Exception
#pragma warning restore CA1032 // Implement standard exception constructors
{
    public int ErrorCode { get; }
    public SqlMockException(string message, int code)
        : base(message) => ErrorCode = code;

    public SqlMockException() : base()
    {
    }

    public SqlMockException(string? message) : base(message)
    {
    }

    public SqlMockException(string? message, Exception? innerException) : base(message, innerException)
    {
    }
}