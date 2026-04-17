namespace DbSqlLikeMem.TestTools.DML;

/// <summary>
/// EN: Reads the next sequence value for the configured sequence.
/// PT: Lê o próximo valor da sequência configurada.
/// </summary>
/// <param name="repo"></param>
/// <param name="context"></param>
public class DmlMutationSequenceCurrentValueServiceTest(
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
        using var transaction = Repo.BeginTransaction();

        var firstValue = Convert.ToInt64(await Repo.ExecuteScalarAsync(Repo.Dialect.NextSequenceValue(Context), transaction), CultureInfo.InvariantCulture);
        var currentValue = Convert.ToInt64(await Repo.ExecuteScalarAsync(Repo.Dialect.CurrentSequenceValue(Context), transaction), CultureInfo.InvariantCulture);
        var secondValue = Convert.ToInt64(await Repo.ExecuteScalarAsync(Repo.Dialect.NextSequenceValue(Context), transaction), CultureInfo.InvariantCulture);
        var currentAfterSecondValue = Convert.ToInt64(await Repo.ExecuteScalarAsync(Repo.Dialect.CurrentSequenceValue(Context), transaction), CultureInfo.InvariantCulture);

        transaction.Commit();

        return new
        {
            firstValue,
            currentValue,
            secondValue,
            currentAfterSecondValue
        };
    }
}
