namespace DbSqlLikeMem.TestTools.DML;

/// <summary>
/// EN: Reads the provider row count and first value after a batch reader workflow.
/// PT: Lê a contagem de linhas e o primeiro valor do provedor após um fluxo de leitura em lote.
/// </summary>
/// <param name="repo"></param>
/// <param name="context"></param>
public class BatchReaderMultiResultServiceTest(
        RepoService repo,
        FidelityTestContext context
    ) : BaseServiceTest(repo, context),
        IBaseServiceTest
{
    /// <summary>
    /// EN: Reads the provider row count and first value after a batch reader workflow.
    /// PT: Lê a contagem de linhas e o primeiro valor do provedor após um fluxo de leitura em lote.
    /// </summary>
    /// <param name="args">EN: Optional user ids for the inserted rows. PT: Ids opcionais de usuario para as linhas inseridas.</param>
    public async Task<object?> RunTestAsync(params object[] args)
    {
        var firstUserId = args.Length > 0 ? (int)args[0] : 1;
        var secondUserId = args.Length > 1 ? (int)args[1] : 2;

        await Repo.ExecuteNonQueryAsync(Repo.Dialect.InsertUser(Context, firstUserId, "Alice"));
        await Repo.ExecuteNonQueryAsync(Repo.Dialect.InsertUser(Context, secondUserId, "Bob"));
        var first = Convert.ToInt32(await Repo.ExecuteScalarAsync(Repo.Dialect.CountRows(Context.TbUsersFullName)), CultureInfo.InvariantCulture);
        var second = await Repo.ExecuteScalarAsync(Repo.Dialect.SelectUserNameById(Context, firstUserId));

        GC.KeepAlive(first);
        GC.KeepAlive(second);
        return second;
    }
}
