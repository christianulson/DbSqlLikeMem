namespace DbSqlLikeMem.TestTools.DDL;

/// <summary>
/// EN: Creates the users table before creating the related orders table with a foreign key.
/// PT-br: Cria a tabela de usuarios antes de criar a tabela de pedidos relacionada com chave estrangeira.
/// </summary>
public class TableWithFKScenario(
        RepoService repo,
       FidelityTestContext context
    ) : BaseScenario(repo, context), 
        ITestScenario
{
    /// <summary>
    /// EN: Seeds the parent users table required by the foreign-key scenario.
    /// PT-br: Preenche a tabela pai de usuarios exigida pelo cenario de chave estrangeira.
    /// </summary>
    public async  Task CreateScenarioAsync()
    {
        await  Repo.ExecuteNonQueryAsync(Repo.Dialect.CreateUsersTable(Context));
        await Repo.ExecuteNonQueryStatementsAsync(Repo.Dialect.CreateOrdersTable(Context));
    }

    /// <summary>
    /// EN: Drops the related orders table before removing the parent users table.
    /// PT-br: Remove a tabela relacionada de pedidos antes de remover a tabela pai de usuarios.
    /// </summary>
    /// <exception cref="NotImplementedException"></exception>
    public async Task DropScenarioAsync()
    {
        try
        {
            await Repo.ExecuteNonQueryAsync(Repo.Dialect.DropTable(Context.TbOrdersFullName));
        }
        catch (Exception ex) when (IsMissingTableException(ex))
        {
        }

        try
        {
            await Repo.ExecuteNonQueryAsync(Repo.Dialect.DropTable(Context.TbUsersFullName));
        }
        catch (Exception ex) when (IsMissingTableException(ex))
        {
        }
    }

    private static bool IsMissingTableException(Exception ex)
    {
        var message = ex.GetBaseException().Message;
        return message.Contains("does not exist", StringComparison.OrdinalIgnoreCase)
            || message.Contains("doesn't exist", StringComparison.OrdinalIgnoreCase)
            || message.Contains("doesnt exist", StringComparison.OrdinalIgnoreCase)
            || message.Contains("not exist", StringComparison.OrdinalIgnoreCase)
            || message.Contains("undefined name", StringComparison.OrdinalIgnoreCase)
            || message.Contains("not found", StringComparison.OrdinalIgnoreCase)
            || message.Contains("ora-00942", StringComparison.OrdinalIgnoreCase)
            || message.Contains("tabela ou view", StringComparison.OrdinalIgnoreCase)
            || message.Contains("given key was not present", StringComparison.OrdinalIgnoreCase);
    }
}
