namespace DbSqlLikeMem.TestTools.DML;

/// <summary>
/// EN: Service that tests batch scalar operations by inserting multiple user rows and validating the count and a specific value.
/// PT: Serviço que testa operações escalares em lote, inserindo várias linhas de usuário e validando a contagem e um valor específico.
/// </summary>
/// <param name="repo"></param>
/// <param name="context"></param>
public class BatchScalarServiceTest(
        RepoService repo,
        FidelityTestContext context
    ) : BaseServiceTest(repo, context),
        IBaseServiceTest
{
    /// <summary>
    /// EN: Executes a scalar batch workflow and validates the count and second row value.
    /// PT: Executa um fluxo de lote escalar e valida a contagem e o valor da segunda linha.
    /// </summary>
    /// <param name="args">EN: Optional user ids for the inserted rows. PT: Ids opcionais de usuario para as linhas inseridas.</param>
    public async Task<object?> RunTestAsync(params object[] args)
    {
        var firstUserId = args.Length > 0 ? (int)args[0] : 1;
        var secondUserId = args.Length > 1 ? (int)args[1] : 2;

        using var transaction = Repo.BeginTransaction();
        await Repo.ExecuteNonQueryAsync(Repo.Dialect.InsertUser(Context, firstUserId, "Alice"), transaction);
        await Repo.ExecuteNonQueryAsync(Repo.Dialect.InsertUser(Context, secondUserId, "Bob"), transaction);
        transaction.Commit();

        var count = Convert.ToInt32(await Repo.ExecuteScalarAsync(Repo.Dialect.CountRows(Context.TbUsersFullName)), CultureInfo.InvariantCulture);
        var second = Convert.ToString(await Repo.ExecuteScalarAsync(Repo.Dialect.SelectUserNameById(Context, secondUserId)), CultureInfo.InvariantCulture);
        return new object?[] { count, second };
    }
}
