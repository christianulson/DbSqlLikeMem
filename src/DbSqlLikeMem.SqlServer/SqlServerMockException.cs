namespace DbSqlLikeMem.SqlServer;

#pragma warning disable CA1032 // Implement standard exception constructors
/// <summary>
/// EN: Represents Sql Server Mock Exception.
/// PT: Representa Sql Server simulada Exceção.
/// </summary>
public sealed class SqlServerMockException : SqlMockException
#pragma warning restore CA1032 // Implement standard exception constructors
{
    /// <summary>
    /// EN: Represents Sql Server Mock Exception.
    /// PT: Representa Sql Server simulada Exceção.
    /// </summary>
    public SqlServerMockException(string message, int code)
        : base(message, code) { }

    /// <summary>
    /// EN: Represents Sql Server Mock Exception.
    /// PT: Representa Sql Server simulada Exceção.
    /// </summary>
    public SqlServerMockException() : base()
    {
    }

    /// <summary>
    /// EN: Represents Sql Server Mock Exception.
    /// PT: Representa Sql Server simulada Exceção.
    /// </summary>
    public SqlServerMockException(string? message) : base(message)
    {
    }

    /// <summary>
    /// EN: Represents Sql Server Mock Exception.
    /// PT: Representa Sql Server simulada Exceção.
    /// </summary>
    public SqlServerMockException(string? message, Exception? innerException) : base(message, innerException)
    {
    }
}
