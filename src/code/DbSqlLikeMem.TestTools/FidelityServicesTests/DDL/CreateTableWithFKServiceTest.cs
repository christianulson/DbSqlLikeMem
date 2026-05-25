namespace DbSqlLikeMem.TestTools.DDL;

/// <summary>
/// EN: Executes the orders-table creation command for the foreign-key scenario.
/// PT-br: Executa o comando de criacao da tabela de pedidos para o cenario de chave estrangeira.
/// </summary>
public class CreateTableWithFKServiceTest(
       RepoService repo,
       FidelityTestContext context
    ) : BaseServiceTest(repo, context),
        IBaseServiceTest
{
    /// <summary>
    /// EN: Creates the orders table that references the users table.
    /// PT-br: Cria a tabela de pedidos que referencia a tabela de usuarios.
    /// </summary>
    public async Task<object?> RunTestAsync(params object[] args)
        => await Repo.ExecuteNonQueryStatementsAsync(Repo.Dialect.CreateOrdersTable(Context));
}
