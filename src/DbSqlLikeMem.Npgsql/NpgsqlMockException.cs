namespace DbSqlLikeMem.Npgsql;

#pragma warning disable CA1032 // Implement standard exception constructors
/// <summary>
/// EN: Summary for NpgsqlMockException.
/// PT: Resumo para NpgsqlMockException.
/// </summary>
public sealed class NpgsqlMockException : SqlMockException
#pragma warning restore CA1032 // Implement standard exception constructors
{
    /// <summary>
    /// EN: Summary for NpgsqlMockException.
    /// PT: Resumo para NpgsqlMockException.
    /// </summary>
    public NpgsqlMockException(string message, int code)
        : base(message, code) 
    { }

    /// <summary>
    /// EN: Summary for NpgsqlMockException.
    /// PT: Resumo para NpgsqlMockException.
    /// </summary>
    public NpgsqlMockException() : base()
    {
    }

    /// <summary>
    /// EN: Summary for NpgsqlMockException.
    /// PT: Resumo para NpgsqlMockException.
    /// </summary>
    public NpgsqlMockException(string? message) : base(message)
    {
    }

    /// <summary>
    /// EN: Summary for NpgsqlMockException.
    /// PT: Resumo para NpgsqlMockException.
    /// </summary>
    public NpgsqlMockException(string? message, Exception? innerException) : base(message, innerException)
    {
    }
}
