namespace DbSqlLikeMem.TestTools.DML;

/// <summary>
/// EN: Reads the next sequence value for the configured sequence.
/// PT: Lê o próximo valor da sequência configurada.
/// </summary>
/// <param name="repo"></param>
/// <param name="context"></param>
public class DmlMutationSequenceSelectProjectionServiceTest(
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
        var value = Convert.ToInt64(await Repo.ExecuteScalarAsync(Repo.Dialect.SelectNextSequenceValue(Context))!, CultureInfo.InvariantCulture);

        if (value != 10L)
            throw new InvalidOperationException($"Unexpected sequence value for {Repo.Dialect.DisplayName}: {value}.");

        return value;
    }
}
