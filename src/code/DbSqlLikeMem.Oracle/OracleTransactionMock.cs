namespace DbSqlLikeMem.Oracle;

/// <summary>
/// EN: Represents Oracle Transaction Mock.
/// PT-br: Representa a transacao simulada do Oracle.
/// </summary>
public sealed class OracleTransactionMock(
    OracleConnectionMock cnn,
    IsolationLevel? isolationLevel = null
    ) : DbTransactionMockBase<OracleConnectionMock>(cnn, isolationLevel)
{
}
