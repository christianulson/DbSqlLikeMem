namespace DbSqlLikeMem.TestTools.DML;

/// <summary>
/// EN: Creates and drops the contract table used to verify default and null insert behavior.
/// PT: Cria e remove a tabela de contrato usada para verificar o comportamento de defaults e nulos no insert.
/// </summary>
public class InsertNullabilityScenario(
    RepoService repo,
    FidelityTestContext context
) : BaseScenario(repo, context),
    ITestScenario
{
    /// <summary>
    /// EN: Creates the contract table used to validate nullable and non-nullable insert behavior.
    /// PT: Cria a tabela de contrato usada para validar o comportamento de insert com colunas anulaveis e nao anulaveis.
    /// </summary>
    public Task CreateScenarioAsync()
        => Repo.ExecuteNonQueryAsync($"""
CREATE TABLE {Context.TbUsersFullName} (
    Id INT NOT NULL PRIMARY KEY,
    RequiredNoDefault INT NOT NULL,
    NullableWithDefault INT DEFAULT 7,
    NullableNoDefault INT
)
""");

    /// <summary>
    /// EN: Drops the contract table created for the insert fidelity checks.
    /// PT: Remove a tabela de contrato criada para as verificacoes de fidelidade do insert.
    /// </summary>
    public async Task DropScenarioAsync()
    {
        try
        {
            await Repo.ExecuteNonQueryAsync(Repo.Dialect.DropTable(Context.TbUsersFullName));
        }
        catch (Exception ex) when (ShouldIgnoreDropException(ex))
        {
        }
    }

    private static bool ShouldIgnoreDropException(Exception ex)
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
            || message.Contains("given key was not present", StringComparison.OrdinalIgnoreCase)
            || message.Contains("lock conflict on no wait transaction", StringComparison.OrdinalIgnoreCase)
            || message.Contains("is in use", StringComparison.OrdinalIgnoreCase);
    }
}
