namespace DbSqlLikeMem.Firebird;

/// <summary>
/// EN: Represents Firebird Transaction Mock.
/// PT-br: Representa a transacao simulada do Firebird.
/// </summary>
public sealed class FirebirdTransactionMock(
    FirebirdConnectionMock cnn,
    IsolationLevel? isolationLevel = null
    ) : DbTransactionMockBase<FirebirdConnectionMock>(cnn, isolationLevel)
{
}

