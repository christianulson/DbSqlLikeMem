namespace DbSqlLikeMem.TestTools.DML;

/// <summary>
/// EN: Service that tests merge operations by inserting a row and then updating it with a second merge.
/// PT-br: Servico que testa operacoes de merge, inserindo uma linha e depois atualizando-a com um segundo merge.
/// </summary>
/// <param name="repo"></param>
/// <param name="context"></param>
public class DmlMutationMergeInsertThenUpdateServiceTest(
        RepoService repo,
        FidelityTestContext context
    ) : BaseServiceTest(repo, context),
        IBaseServiceTest
{
    /// <summary>
    /// EN: Inserts a row with merge and then updates the same row with a second merge.
    /// PT-br: Insere uma linha com merge e depois atualiza a mesma linha com um segundo merge.
    /// </summary>
    public async Task<object?> RunTestAsync(params object[] args)
    {
        if (!Repo.Dialect.SupportsMerge)
        {
            throw new NotSupportedException($"{Repo.Dialect.DisplayName} does not support the merge benchmark.");
        }

        await Repo.ExecuteNonQueryAsync(Repo.Dialect.Merge(Context, 1, "Alice"));

        var inserted = Convert.ToString(await Repo.ExecuteScalarAsync(Repo.Dialect.SelectUserNameById(Context, 1)), CultureInfo.InvariantCulture);
        await Repo.ExecuteNonQueryAsync(Repo.Dialect.Merge(Context, 1, "Alice-v2"));

        var updated = Convert.ToString(await Repo.ExecuteScalarAsync(Repo.Dialect.SelectUserNameById(Context, 1)), CultureInfo.InvariantCulture);
        GC.KeepAlive(inserted);

        return updated;
    }
}
