namespace DbSqlLikeMem.MySql;

/// <summary>
/// EN: Mock transaction for MySQL connections.
/// PT-br: Transacao simulada para conexoes MySQL.
/// </summary>
public class MySqlTransactionMock(
        MySqlConnectionMock cnn,
        IsolationLevel? isolationLevel = null
    ) : DbTransactionMockBase<MySqlConnectionMock>(cnn, isolationLevel)
{
}
