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
    public async Task<object?> RunTestAsync(params object[] args)
    {
        using var transaction = Repo.BeginTransaction();
        await Repo.ExecuteNonQueryAsync(Repo.Dialect.InsertUser(Context, 1, "Alice"), transaction);
        await Repo.ExecuteNonQueryAsync(Repo.Dialect.InsertUser(Context, 2, "Bob"), transaction);
        var value = Convert.ToString(await Repo.ExecuteScalarAsync(Repo.Dialect.SelectUserNameById(Context, 1), transaction), CultureInfo.InvariantCulture);
        await Repo.ExecuteNonQueryAsync(Repo.Dialect.UpdateUserNameById(Context, 2, "Bob-v2"), transaction);
        transaction.Commit();

        return value!;
    }
}
