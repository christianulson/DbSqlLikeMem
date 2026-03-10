namespace DbSqlLikeMem.Db2;

/// <summary>
/// EN: Represents Db2 Transaction Mock.
/// PT: Representa a transacao simulada do Db2.
/// </summary>
public class Db2TransactionMock(
        Db2ConnectionMock cnn,
        IsolationLevel? isolationLevel = null
    ) : DbTransactionMockBase<Db2ConnectionMock>(cnn, isolationLevel)
{
}
