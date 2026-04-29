namespace DbSqlLikeMem.TestTools.DML;

/// <summary>
/// EN: Creates and drops the contract table used to verify check-constraint insert and update behavior.
/// PT: Cria e remove a tabela de contrato usada para verificar o comportamento de insert e update com restricoes check.
/// </summary>
public class CheckConstraintsScenario(
    RepoService repo,
    FidelityTestContext context
) : BaseScenario(repo, context),
    ITestScenario
{
    /// <summary>
    /// EN: Creates the contract table used to validate check constraints together with defaults and nullable columns.
    /// PT: Cria a tabela de contrato usada para validar restricoes check junto com defaults e colunas anulaveis.
    /// </summary>
    public Task CreateScenarioAsync()
        => Repo.ExecuteNonQueryAsync($"""
CREATE TABLE {Context.TbUsersFullName} (
    Id INT NOT NULL PRIMARY KEY,
    RequiredNoDefault INT NOT NULL,
    NullableWithDefault INT NULL DEFAULT 7 CHECK (NullableWithDefault > 0),
    NullableNoDefault INT NULL,
    CheckedRequired INT NOT NULL CHECK (CheckedRequired > 0),
    CheckedNullable INT NULL CHECK (CheckedNullable > 0)
)
""");

    /// <summary>
    /// EN: Drops the contract table created for the check-constraint fidelity checks.
    /// PT: Remove a tabela de contrato criada para as verificacoes de fidelidade de restricoes check.
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
