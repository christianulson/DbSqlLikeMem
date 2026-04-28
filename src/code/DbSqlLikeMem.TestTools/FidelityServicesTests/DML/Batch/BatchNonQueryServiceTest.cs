namespace DbSqlLikeMem.TestTools.DML;

/// <summary>
/// EN: Executes a non-query batch workflow and validates the final row count.
/// PT: Executa um fluxo de lote sem resultado e valida a contagem final de linhas.
/// </summary>
/// <param name="repo"></param>
/// <param name="context"></param>
public class BatchNonQueryServiceTest(
        RepoService repo,
        FidelityTestContext context
    ) : BaseServiceTest(repo, context),
        IBaseServiceTest
{
    /// <summary>
    /// EN: Executes a non-query batch workflow and validates the final row count.
    /// PT: Executa um fluxo de lote sem resultado e valida a contagem final de linhas.
    /// </summary>
    /// <param name="args">EN: Optional user ids for the insert, update, and delete rows. PT: Ids opcionais de usuario para as linhas de insert, update e delete.</param>
    public async Task<object?> RunTestAsync(params object[] args)
    {
        var firstUserId = args.Length > 0 ? (int)args[0] : 1;
        var secondUserId = args.Length > 1 ? (int)args[1] : 2;
        var updateUserId = args.Length > 2 ? (int)args[2] : 2;
        var deleteUserId = args.Length > 3 ? (int)args[3] : 1;

        using var transaction = Repo.BeginTransaction();
        await Repo.ExecuteNonQueryAsync(Repo.Dialect.InsertUser(Context, firstUserId, "Alice"), transaction);
        await Repo.ExecuteNonQueryAsync(Repo.Dialect.InsertUser(Context, secondUserId, "Bob"), transaction);
        await Repo.ExecuteNonQueryAsync(Repo.Dialect.UpdateUserNameById(Context, updateUserId, "Bob-v2"), transaction);
        await Repo.ExecuteNonQueryAsync(Repo.Dialect.DeleteUserById(Context, deleteUserId), transaction);
        var count = Convert.ToInt32(await Repo.ExecuteScalarAsync(Repo.Dialect.CountRows(Context.TbUsersFullName), transaction), CultureInfo.InvariantCulture);
        transaction.Commit();
        if (count != 1)
        {
            throw new InvalidOperationException($"Unexpected non-query batch count for {Repo.Dialect.DisplayName}: {count}.");
        }

        return count;
    }
}
