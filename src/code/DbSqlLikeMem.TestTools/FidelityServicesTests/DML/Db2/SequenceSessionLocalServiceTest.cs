namespace DbSqlLikeMem.Db2.Test.Fidelity.DML;

/// <summary>
/// EN: Reads DB2 session-local sequence values from two independent connections.
/// PT: Lê valores de sequence locais da sessao DB2 a partir de duas conexoes independentes.
/// </summary>
public sealed class SequenceSessionLocalServiceTest(
    RepoService repo,
    FidelityTestContext context
    ) : BaseServiceTest(repo, context),
        IBaseServiceTest
{
    /// <summary>
    /// EN: Returns the observed next and previous sequence values across two DB2 sessions.
    /// PT: Retorna os valores observados de next e previous da sequence em duas sessoes DB2.
    /// </summary>
    public async Task<object?> RunTestAsync(params object[] args)
        => await RunSequenceSessionLocalAsync();

    /// <summary>
    /// EN: Returns the observed next and previous sequence values across two DB2 sessions.
    /// PT: Retorna os valores observados de next e previous da sequence em duas sessoes DB2.
    /// </summary>
    public async Task<long[]> RunSequenceSessionLocalAsync()
    {
        await EnsureOpenAsync(Repo);
        using var second = Repo.CloneWithSharedDatabase();
        await EnsureOpenAsync(second);

        return await RunSessionLocalAsync(second);
    }

    private async Task<long[]> RunSessionLocalAsync(RepoService second)
    {
        var firstNext = await ExecuteScalarLongAsync(Repo, Repo.Dialect.NextSequenceValue(Context));
        var firstPrevious = await ExecuteScalarLongAsync(Repo, Repo.Dialect.CurrentSequenceValue(Context));
        var secondNext = await ExecuteScalarLongAsync(second, Repo.Dialect.NextSequenceValue(Context));
        var secondPrevious = await ExecuteScalarLongAsync(second, Repo.Dialect.CurrentSequenceValue(Context));
        var firstPreviousAfterSecond = await ExecuteScalarLongAsync(Repo, Repo.Dialect.CurrentSequenceValue(Context));

        return new[] { firstNext, firstPrevious, secondNext, secondPrevious, firstPreviousAfterSecond };
    }

    private static async Task<long> ExecuteScalarLongAsync(RepoService repo, string sql)
    {
        using var command = repo.Cnn.CreateCommand();
        command.CommandText = sql;
        return Convert.ToInt64(await command.ExecuteScalarAsync(), CultureInfo.InvariantCulture);
    }

    private static async Task EnsureOpenAsync(RepoService repo)
    {
        if (repo.Cnn.State == ConnectionState.Open)
            return;

        await repo.Cnn.OpenAsync();
    }
}
