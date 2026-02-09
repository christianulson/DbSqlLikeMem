namespace DbSqlLikeMem.MySql;

#pragma warning disable CA1032 // Implement standard exception constructors
public sealed class MySqlMockException : SqlMockException
#pragma warning restore CA1032 // Implement standard exception constructors
{
    public MySqlMockException(string message, int code)
        : base(message, code)
    { }

    public MySqlMockException() : base()
    {
    }

    public MySqlMockException(string? message) : base(message)
    {
    }

    public MySqlMockException(string? message, Exception? innerException) : base(message, innerException)
    {
    }
}