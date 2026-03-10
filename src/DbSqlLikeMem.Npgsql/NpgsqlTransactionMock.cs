namespace DbSqlLikeMem.Npgsql;

/// <summary>
/// EN: Represents Npgsql Transaction Mock.
/// PT: Representa a transacao simulada do Npgsql.
/// </summary>
public sealed class NpgsqlTransactionMock(
    NpgsqlConnectionMock cnn,
    IsolationLevel? isolationLevel = null
    ) : DbTransactionMockBase<NpgsqlConnectionMock>(cnn, isolationLevel)
{
}
