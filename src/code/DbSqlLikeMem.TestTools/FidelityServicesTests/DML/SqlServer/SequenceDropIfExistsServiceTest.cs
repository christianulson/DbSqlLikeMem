namespace DbSqlLikeMem.SqlServer.Test.Fidelity.DML;

/// <summary>
/// EN: Drops a SQL Server sequence twice with IF EXISTS and verifies the command stays idempotent.
/// PT-br: Remove uma sequence do SQL Server duas vezes com IF EXISTS e verifica se o comando continua idempotente.
/// </summary>
public sealed class SequenceDropIfExistsServiceTest(
    RepoService repo,
    FidelityTestContext context
    ) : BaseServiceTest(repo, context),
        IBaseServiceTest
{
    /// <summary>
    /// EN: Returns the first generated sequence value and whether the second IF EXISTS drop completed.
    /// PT-br: Retorna o primeiro valor gerado da sequence e se a segunda remocao IF EXISTS foi concluida.
    /// </summary>
    public async Task<object?> RunTestAsync(params object[] args)
    {
        _ = args;

        var first = await ExecuteScalarLongAsync(Repo.Dialect.NextSequenceValue(Context));
        await ExecuteNonQueryAsync(Repo.Dialect.DropSequence(Context));
        await ExecuteNonQueryAsync(Repo.Dialect.DropSequence(Context));
        return new[] { first, 1L };
    }

    private async Task ExecuteNonQueryAsync(string sql)
    {
        using var command = Repo.Cnn.CreateCommand();
        command.CommandText = sql;
        await command.ExecuteNonQueryAsync();
    }

    private async Task<long> ExecuteScalarLongAsync(string sql)
    {
        using var command = Repo.Cnn.CreateCommand();
        command.CommandText = sql;
        return Convert.ToInt64(await command.ExecuteScalarAsync(), CultureInfo.InvariantCulture);
    }
}
