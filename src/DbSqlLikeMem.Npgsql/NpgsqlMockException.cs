namespace DbSqlLikeMem.Npgsql;

#pragma warning disable CA1032 // Implement standard exception constructors
/// <summary>
/// EN: Represents Npgsql Mock Exception.
/// PT: Representa uma exceção simulada do Npgsql.
/// </summary>
public sealed class NpgsqlMockException : SqlMockException
#pragma warning restore CA1032 // Implement standard exception constructors
{
    /// <summary>
    /// EN: Represents Npgsql Mock Exception.
    /// PT: Representa uma exceção simulada do Npgsql.
    /// </summary>
    public NpgsqlMockException(string message, int code)
        : base(message, code) 
    { }

    /// <summary>
    /// EN: Represents Npgsql Mock Exception.
    /// PT: Representa uma exceção simulada do Npgsql.
    /// </summary>
    public NpgsqlMockException() : base()
    {
    }

    /// <summary>
    /// EN: Represents Npgsql Mock Exception.
    /// PT: Representa uma exceção simulada do Npgsql.
    /// </summary>
    public NpgsqlMockException(string? message) : base(message)
    {
    }

    /// <summary>
    /// EN: Represents Npgsql Mock Exception.
    /// PT: Representa uma exceção simulada do Npgsql.
    /// </summary>
    public NpgsqlMockException(string? message, Exception? innerException) : base(message, innerException)
    {
    }
}
