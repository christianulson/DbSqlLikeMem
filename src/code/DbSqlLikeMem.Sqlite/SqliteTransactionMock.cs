namespace DbSqlLikeMem.Sqlite;

/// <summary>
/// EN: Represents Sqlite Transaction Mock.
/// PT: Representa a transacao simulada do SQLite.
/// </summary>
public class SqliteTransactionMock(
        SqliteConnectionMock cnn,
        IsolationLevel? isolationLevel = null
    ) : DbTransactionMockBase<SqliteConnectionMock>(cnn, isolationLevel)
{
}
