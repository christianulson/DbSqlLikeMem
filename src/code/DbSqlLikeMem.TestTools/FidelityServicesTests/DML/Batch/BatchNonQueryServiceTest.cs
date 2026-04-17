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
    public async Task<object?> RunTestAsync(params object[] args)
    {
        using var transaction = Repo.BeginTransaction();
        await Repo.ExecuteNonQueryAsync(Repo.Dialect.InsertUser(Context, 1, "Alice"), transaction);
        await Repo.ExecuteNonQueryAsync(Repo.Dialect.InsertUser(Context, 2, "Bob"), transaction);
        await Repo.ExecuteNonQueryAsync(Repo.Dialect.UpdateUserNameById(Context, 2, "Bob-v2"), transaction);
        await Repo.ExecuteNonQueryAsync(Repo.Dialect.DeleteUserById(Context, 1), transaction);
        var count = Convert.ToInt32(await Repo.ExecuteScalarAsync(Repo.Dialect.CountRows(Context.TbUsersFullName), transaction), CultureInfo.InvariantCulture);
        transaction.Commit();
        if (count != 1)
        {
            throw new InvalidOperationException($"Unexpected non-query batch count for {Repo.Dialect.DisplayName}: {count}.");
        }

        return count;
    }
}
