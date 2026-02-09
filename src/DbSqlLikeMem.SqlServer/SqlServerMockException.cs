namespace DbSqlLikeMem.SqlServer;

#pragma warning disable CA1032 // Implement standard exception constructors
public sealed class SqlServerMockException : SqlMockException
#pragma warning restore CA1032 // Implement standard exception constructors
{
    public SqlServerMockException(string message, int code)
        : base(message, code) { }

    public SqlServerMockException() : base()
    {
    }

    public SqlServerMockException(string? message) : base(message)
    {
    }

    public SqlServerMockException(string? message, Exception? innerException) : base(message, innerException)
    {
    }
}