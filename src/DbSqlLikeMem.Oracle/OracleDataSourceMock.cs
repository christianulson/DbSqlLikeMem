using System.Data.Common;

namespace DbSqlLikeMem.Oracle;

/// <summary>
/// EN: Summary for OracleDataSourceMock.
/// PT: Resumo para OracleDataSourceMock.
/// </summary>
public sealed class OracleDataSourceMock(OracleDbMock? db = null)
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
    public OracleConnectionMock CreateDbConnection() => new OracleConnectionMock(db);
}
