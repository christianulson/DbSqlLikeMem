namespace DbSqlLikeMem.Db2;

#pragma warning disable CA1032 // Implement standard exception constructors
/// <summary>
/// EN: Summary for Db2MockException.
/// PT: Resumo para Db2MockException.
/// </summary>
public sealed class Db2MockException : SqlMockException
#pragma warning restore CA1032 // Implement standard exception constructors
{
    /// <summary>
    /// EN: Summary for Db2MockException.
    /// PT: Resumo para Db2MockException.
    /// </summary>
    public Db2MockException(string message, int code)
        : base(message, code)
    { }

    /// <summary>
    /// EN: Summary for Db2MockException.
    /// PT: Resumo para Db2MockException.
    /// </summary>
    public Db2MockException() : base()
    {
    }

    /// <summary>
    /// EN: Summary for Db2MockException.
    /// PT: Resumo para Db2MockException.
    /// </summary>
    public Db2MockException(string? message) : base(message)
    {
    }

    /// <summary>
    /// EN: Summary for Db2MockException.
    /// PT: Resumo para Db2MockException.
    /// </summary>
    public Db2MockException(string? message, Exception? innerException) : base(message, innerException)
    {
    }
}
