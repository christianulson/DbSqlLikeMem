using System.Data.Common;

using DbConnection = System.Data.Common.DbConnection;
namespace DbSqlLikeMem.Npgsql;

/// <summary>
/// EN: Summary for NpgsqlDataSourceMock.
/// PT: Resumo para NpgsqlDataSourceMock.
/// </summary>
public sealed class NpgsqlDataSourceMock(NpgsqlDbMock? db = null)
#if NET7_0_OR_GREATER
    : DbDataSource
#endif
{
    /// <summary>
    /// EN: Summary for member.
    /// PT: Resumo para member.
    /// </summary>
    public
#if NET7_0_OR_GREATER
    override
#endif
    string ConnectionString => string.Empty;

#if NET7_0_OR_GREATER
    /// <summary>
    /// EN: Summary for CreateDbConnection.
    /// PT: Resumo para CreateDbConnection.
    /// </summary>
    protected override DbConnection CreateDbConnection() => new NpgsqlConnectionMock(db);
#else
    /// <summary>
    /// EN: Summary for CreateDbConnection.
    /// PT: Resumo para CreateDbConnection.
    /// </summary>
    public NpgsqlConnectionMock CreateDbConnection() => new NpgsqlConnectionMock(db);
#endif

    /// <summary>
    /// EN: Summary for CreateConnection.
    /// PT: Resumo para CreateConnection.
    /// </summary>
    public new NpgsqlConnectionMock CreateConnection() => new NpgsqlConnectionMock(db);

}
