namespace DbSqlLikeMem.SqlServer;

/// <summary>
/// EN: Represents Sql Server Transaction Mock.
/// PT: Representa a transacao simulada do SQL Server.
/// </summary>
public sealed class SqlServerTransactionMock(
    SqlServerConnectionMock cnn,
    IsolationLevel? isolationLevel = null
    ) : DbTransactionMockBase<SqlServerConnectionMock>(cnn, isolationLevel)
{
}
