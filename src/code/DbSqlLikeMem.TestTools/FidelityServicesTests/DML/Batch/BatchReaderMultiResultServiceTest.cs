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
    public async Task<object?> RunTestAsync(params object[] args)
    {
        await Repo.ExecuteNonQueryAsync(Repo.Dialect.InsertUser(Context, 1, "Alice"));
        await Repo.ExecuteNonQueryAsync(Repo.Dialect.InsertUser(Context, 2, "Bob"));
        var first = Convert.ToInt32(await Repo.ExecuteScalarAsync(Repo.Dialect.CountRows(Context.TbUsersFullName)), CultureInfo.InvariantCulture);
        var second = await Repo.ExecuteScalarAsync(Repo.Dialect.SelectUserNameById(Context, 1));

        GC.KeepAlive(first);
        GC.KeepAlive(second);
        return second;
    }
}
