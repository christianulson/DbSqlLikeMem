namespace DbSqlLikeMem.TestTools.DDL;

/// <summary>
/// EN: Creates the users table for DDL scenarios without an associated foreign key.
/// PT: Cria a tabela de usuarios para cenarios DDL sem chave estrangeira associada.
/// </summary>
public class CreateTableScenario(
    RepoService repo,
    FidelityTestContext context
    ) : BaseScenario(repo, context),
        ITestScenario
{
    /// <summary>
    /// EN: Keeps the create-table scenario focused on the users table definition.
    /// PT: Mantem o cenario de create-table focado na definicao da tabela de usuarios.
    /// </summary>
    public Task CreateScenarioAsync()
    {
        return Task.CompletedTask;
    }

    /// <summary>
    /// EN: Drops the users table created by the scenario.
    /// PT: Remove a tabela de usuarios criada pelo cenario.
    /// </summary>
    /// <exception cref="NotImplementedException"></exception>
    public virtual async Task DropScenarioAsync()
    {
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
