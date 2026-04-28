namespace DbSqlLikeMem.TestTools.DML;

/// <summary>
/// EN: Executes a mixed read/write batch and validates the read result.
/// PT: Executa um lote misto de leitura e escrita e valida o resultado lido.
/// </summary>
/// <param name="repo"></param>
/// <param name="context"></param>
public class BatchMixedReadWriteServiceTest(
        RepoService repo,
        FidelityTestContext context
    ) : BaseServiceTest(repo, context),
        IBaseServiceTest
{
    /// <summary>
    /// EN: Executes a mixed read/write batch and validates the read result.
    /// PT: Executa um lote misto de leitura e escrita e valida o resultado lido.
    /// </summary>
    /// <param name="args">EN: Optional user ids for the insert and update rows. PT: Ids opcionais de usuario para as linhas de insert e update.</param>
    public async Task<object?> RunTestAsync(params object[] args)
    {
        var firstUserId = args.Length > 0 ? (int)args[0] : 1;
        var secondUserId = args.Length > 1 ? (int)args[1] : 2;

        using var transaction = Repo.BeginTransaction();
        await Repo.ExecuteNonQueryAsync(Repo.Dialect.InsertUser(Context, firstUserId, "Alice"), transaction);
        await Repo.ExecuteNonQueryAsync(Repo.Dialect.InsertUser(Context, secondUserId, "Bob"), transaction);
        var value = Convert.ToString(await Repo.ExecuteScalarAsync(Repo.Dialect.SelectUserNameById(Context, firstUserId), transaction), CultureInfo.InvariantCulture);
        await Repo.ExecuteNonQueryAsync(Repo.Dialect.UpdateUserNameById(Context, secondUserId, "Bob-v2"), transaction);
        transaction.Commit();

        return value!;
    }
}
