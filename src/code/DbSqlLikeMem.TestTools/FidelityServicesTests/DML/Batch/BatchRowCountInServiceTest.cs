namespace DbSqlLikeMem.TestTools.DML;

/// <summary>
/// EN: Reads the provider row count after a batch insert workflow.
/// PT: Lê a contagem de linhas do provedor após um fluxo de insert em lote.
/// </summary>
/// <param name="repo"></param>
/// <param name="context"></param>
public class BatchRowCountInServiceTest(
        RepoService repo,
        FidelityTestContext context
    ) : BaseServiceTest(repo, context),
        IBaseServiceTest
{
    /// <summary>
    /// EN: Reads the provider row count after a batch insert workflow.
    /// PT: Lê a contagem de linhas do provedor após um fluxo de insert em lote.
    /// </summary>
    public virtual async Task<object?> RunTestAsync(params object[] args)
    {
        await Repo.ExecuteNonQueryAsync(Repo.Dialect.InsertUser(Context, 1, "Alice"));
        await Repo.ExecuteNonQueryAsync(Repo.Dialect.InsertUser(Context, 2, "Bob"));

        var count = Convert.ToInt32(await Repo.ExecuteScalarAsync(Repo.Dialect.CountRows(Context.TbUsersFullName)), CultureInfo.InvariantCulture);
        if (count != 2)
        {
            throw new InvalidOperationException($"Unexpected batch rowcount for {Repo.Dialect.DisplayName}: {count}.");
        }

        return count;
    }
}
