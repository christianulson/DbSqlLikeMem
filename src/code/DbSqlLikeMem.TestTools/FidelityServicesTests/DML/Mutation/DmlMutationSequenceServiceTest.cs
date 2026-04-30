namespace DbSqlLikeMem.TestTools.DML;

/// <summary>
/// EN: Reads the next sequence value for the configured sequence.
/// PT: Lê o próximo valor da sequência configurada.
/// </summary>
/// <param name="repo"></param>
/// <param name="context"></param>
public class DmlMutationSequenceServiceTest(
        RepoService repo,
        FidelityTestContext context
    ) : BaseServiceTest(repo, context),
        IBaseServiceTest
{
    /// <summary>
    /// EN: Reads the next sequence value for the configured sequence.
    /// PT: Lê o próximo valor da sequência configurada.
    /// </summary>
    public virtual async Task<object?> RunTestAsync(params object[] args)
    {
        if (!Repo.Dialect.SupportsSequence)
            throw new NotSupportedException($"{Repo.Dialect.DisplayName} does not support the sequence benchmark.");

        var first = Convert.ToInt64(await Repo.ExecuteScalarAsync(Repo.Dialect.NextSequenceValue(Context)), CultureInfo.InvariantCulture);
        var second = Convert.ToInt64(await Repo.ExecuteScalarAsync(Repo.Dialect.NextSequenceValue(Context)), CultureInfo.InvariantCulture);
        GC.KeepAlive(first);
        GC.KeepAlive(second);
        return new[] { first, second };
    }
}
