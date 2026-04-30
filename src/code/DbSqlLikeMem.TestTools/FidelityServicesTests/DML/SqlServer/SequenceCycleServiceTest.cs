namespace DbSqlLikeMem.SqlServer.Test.Fidelity.DML;

/// <summary>
/// EN: Creates a SQL Server sequence with CYCLE and reads the wrapped values for fidelity coverage.
/// PT: Cria uma sequence SQL Server com CYCLE e le os valores reiniciados para cobertura de fidelidade.
/// </summary>
public sealed class SequenceCycleServiceTest(
    RepoService repo,
    FidelityTestContext context
    ) : BaseServiceTest(repo, context),
        IBaseServiceTest
{
    /// <summary>
    /// EN: Creates a bounded cycle sequence, reads three generated values, and removes the temporary sequence.
    /// PT: Cria uma sequence ciclica limitada, le tres valores gerados e remove a sequence temporaria.
    /// </summary>
    public async Task<object?> RunTestAsync(params object[] args)
    {
        _ = args;

        await ExecuteNonQueryAsync($"CREATE SEQUENCE {Context.Seq} START WITH 1 INCREMENT BY 1 MINVALUE 1 MAXVALUE 2 CYCLE");
        try
        {
            var first = await ExecuteScalarLongAsync(Repo.Dialect.NextSequenceValue(Context));
            var second = await ExecuteScalarLongAsync(Repo.Dialect.NextSequenceValue(Context));
            var third = await ExecuteScalarLongAsync(Repo.Dialect.NextSequenceValue(Context));
            return new[] { first, second, third };
        }
        finally
        {
            await ExecuteNonQueryAsync($"DROP SEQUENCE IF EXISTS {Context.Seq}");
        }
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
