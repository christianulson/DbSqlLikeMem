namespace DbSqlLikeMem.TestTools.DML;

/// <summary>
/// EN: Service that tests batch insert operations by inserting multiple user rows and validating the count.
/// PT: Serviço que testa operações de inserção em lote, inserindo várias linhas de usuário e validando a contagem.
/// </summary>
/// <param name="repo"></param>
/// <param name="context"></param>
public class BatchInsertServiceTest(
        RepoService repo,
        FidelityTestContext context
    ) : BaseServiceTest(repo, context),
        IBaseServiceTest
{
    /// <summary>
    /// EN: Inserts ten user rows in a batch and validates the final count.
    /// PT: Insere dez linhas de usuario em lote e valida a contagem final.
    /// </summary>
    public virtual async Task<object?> RunTestAsync(params object[] args)
    {
        using var transaction = Repo.BeginTransaction();

        var qtt = (int)args[0];

        var values = new (int id, string name)[qtt];
        for (var i = 1; i <= qtt; i++)
        {
            values[i - 1] = (i, $"User-{i}");
        }

        await Repo.ExecuteNonQueryAsync(Repo.Dialect.InsertUsers(Context, values), transaction);
        transaction.Commit();

        var count = Convert.ToInt32(await Repo.ExecuteScalarAsync(Repo.Dialect.CountRows(Context.TbUsersFullName)), CultureInfo.InvariantCulture);
        if (count != qtt)
        {
            throw new InvalidOperationException($"Expected {qtt} rows for {Repo.Dialect.DisplayName}, got {count}.");
        }

        return count;
    }
}
