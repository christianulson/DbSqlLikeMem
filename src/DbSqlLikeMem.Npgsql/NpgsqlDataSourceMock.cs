using System.Data.Common;

using DbConnection = System.Data.Common.DbConnection;
namespace DbSqlLikeMem.Npgsql;

/// <summary>
/// EN: Summary for NpgsqlDataSourceMock.
/// PT: Resumo para NpgsqlDataSourceMock.
/// </summary>
public sealed class NpgsqlDataSourceMock(NpgsqlDbMock? db = null)
{
    /// <summary>
    /// EN: Summary for member.
    /// PT: Resumo para member.
    /// </summary>
    public string ConnectionString => string.Empty;

    /// <summary>
    /// EN: Summary for CreateDbConnection.
    /// PT: Resumo para CreateDbConnection.
    /// </summary>
    public NpgsqlConnectionMock CreateDbConnection() => new NpgsqlConnectionMock(db);
}
