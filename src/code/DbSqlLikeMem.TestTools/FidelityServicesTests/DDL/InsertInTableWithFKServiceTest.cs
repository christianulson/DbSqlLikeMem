namespace DbSqlLikeMem.TestTools.DDL;

/// <summary>
/// EN: Creates the orders table with a foreign key and inserts a valid referenced row.
/// PT: Cria a tabela de pedidos com chave estrangeira e insere uma linha referenciada valida.
/// </summary>
public class InsertInTableWithFKServiceTest(
       RepoService repo,
       FidelityTestContext context
    ) : BaseServiceTest(repo, context),
        IBaseServiceTest
{
    /// <summary>
    /// EN: Creates the orders table and inserts a valid row that references the users table.
    /// PT: Cria a tabela de pedidos e insere uma linha valida que referencia a tabela de usuarios.
    /// </summary>
    public async Task<object?> RunTestAsync(params object[] args)
    {
        var d = Repo.Dialect;
        var orderedAt = d.TemporalCurrentTimestampExpression();

        await Repo.ExecuteNonQueryAsync(d.InsertUser(Context, 1, "Ana"));
        await Repo.ExecuteNonQueryAsync(d.InsertOrder(Context, 10, 1, "first", "o-10", 12.34m, 2, true, orderedAt));
        return Convert.ToInt32(await Repo.ExecuteScalarAsync(d.CountJoinForUser(Context, 1)));
    }
}
