namespace DbSqlLikeMem.Npgsql;

#pragma warning disable CA1032 // Implement standard exception constructors
public sealed class NpgsqlMockException : SqlMockException
#pragma warning restore CA1032 // Implement standard exception constructors
{
    public NpgsqlMockException(string message, int code)
        : base(message, code) 
    { }

    public NpgsqlMockException() : base()
    {
    }

    public NpgsqlMockException(string? message) : base(message)
    {
    }

    public NpgsqlMockException(string? message, Exception? innerException) : base(message, innerException)
    {
    }
}