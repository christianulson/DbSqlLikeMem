namespace DbSqlLikeMem.TestTools.DML;

/// <summary>
/// EN: Reads the provider row count after a batch insert workflow.
/// PT-br: Lê a contagem de linhas do provedor após um fluxo de insert em lote.
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
    /// PT-br: Lê a contagem de linhas do provedor após um fluxo de insert em lote.
    /// </summary>
    /// <param name="args">EN: Optional user ids for the inserted rows. PT-br: Ids opcionais de usuario para as linhas inseridas.</param>
    public virtual async Task<object?> RunTestAsync(params object[] args)
    {
        var firstUserId = args.Length > 0 ? (int)args[0] : 1;
        var secondUserId = args.Length > 1 ? (int)args[1] : 2;

        await Repo.ExecuteNonQueryAsync(Repo.Dialect.InsertUser(Context, firstUserId, "Alice"));
        await Repo.ExecuteNonQueryAsync(Repo.Dialect.InsertUser(Context, secondUserId, "Bob"));

        var count = Convert.ToInt32(await Repo.ExecuteScalarAsync(Repo.Dialect.CountRows(Context.TbUsersFullName)), CultureInfo.InvariantCulture);
        if (count != 2)
        {
            throw new InvalidOperationException($"Unexpected batch rowcount for {Repo.Dialect.DisplayName}: {count}.");
        }

        return count;
    }
}
