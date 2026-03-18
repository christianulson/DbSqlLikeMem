using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using DbSqlLikeMem.SqlServer;
namespace DbSqlLikeMem.SqlAzure;

public sealed class SqlAzureMockException : SqlMockException
#pragma warning restore CA1032 // Implement standard exception constructors
{
    /// <summary>
    /// EN: Represents SQL Azure mock exception.
    /// PT: Representa exceção simulada do SQL Azure.
    /// </summary>
    public SqlAzureMockException(string message, int code) : base(message, code)
    {
    }

    /// <summary>
    /// EN: Represents SQL Azure mock exception.
    /// PT: Representa exceção simulada do SQL Azure.
    /// </summary>
    public SqlAzureMockException() : base()
    {
    }

    /// <summary>
    /// EN: Represents SQL Azure mock exception.
    /// PT: Representa exceção simulada do SQL Azure.
    /// </summary>
    public SqlAzureMockException(string? message) : base(message)
    {
    }

    /// <summary>
    /// EN: Represents SQL Azure mock exception.
    /// PT: Representa exceção simulada do SQL Azure.
    /// </summary>
    public SqlAzureMockException(string? message, Exception? innerException) : base(message, innerException)
    {
    }
}