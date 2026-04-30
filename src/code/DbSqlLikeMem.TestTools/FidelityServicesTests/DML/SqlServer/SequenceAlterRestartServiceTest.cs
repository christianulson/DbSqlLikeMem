namespace DbSqlLikeMem.SqlServer.Test.Fidelity.DML;

/// <summary>
/// EN: Applies ALTER SEQUENCE RESTART WITH and reads the next values for SQL Server sequence fidelity.
/// PT: Aplica ALTER SEQUENCE RESTART WITH e le os proximos valores para fidelidade de sequence do SQL Server.
/// </summary>
public sealed class SequenceAlterRestartServiceTest(
    RepoService repo,
    FidelityTestContext context
    ) : BaseServiceTest(repo, context),
        IBaseServiceTest
{
    /// <summary>
    /// EN: Resets the current sequence and returns the next generated values after the restart.
    /// PT: Reinicia a sequence atual e retorna os proximos valores gerados depois do restart.
    /// </summary>
    public async Task<object?> RunTestAsync(params object[] args)
    {
        _ = args;

        await ExecuteNonQueryAsync($"ALTER SEQUENCE {Context.Seq} RESTART WITH 40");
        var first = await ExecuteScalarLongAsync(Repo.Dialect.NextSequenceValue(Context));
        var second = await ExecuteScalarLongAsync(Repo.Dialect.NextSequenceValue(Context));
        return new[] { first, second };
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
