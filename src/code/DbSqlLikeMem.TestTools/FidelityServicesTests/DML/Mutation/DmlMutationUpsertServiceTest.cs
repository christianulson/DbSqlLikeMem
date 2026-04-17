namespace DbSqlLikeMem.TestTools.DML;

/// <summary>
/// EN: Service that tests upsert operations by executing a provider-specific upsert path and validating the updated value.
/// PT: Serviço que testa operações de upsert, executando um caminho de upsert específico do provedor e validando o valor atualizado.
/// </summary>
/// <param name="repo"></param>
/// <param name="context"></param>
public class DmlMutationUpsertServiceTest(
        RepoService repo,
        FidelityTestContext context
    ) : BaseServiceTest(repo, context),
        IBaseServiceTest
{
    /// <summary>
    /// EN: Executes the provider-specific upsert path and validates the updated value.
    /// PT: Executa o caminho de upsert específico do provedor e valida o valor atualizado.
    /// </summary>
    public async Task<object?> RunTestAsync(params object[] args)
    {
        if (!Repo.Dialect.SupportsUpsert)
        {
            throw new NotSupportedException($"{Repo.Dialect.DisplayName} does not support the upsert benchmark.");
        }

        await Repo.ExecuteNonQueryAsync(Repo.Dialect.Upsert(Context, 1, "Alice-v2"));
        var value = Convert.ToString(await Repo.ExecuteScalarAsync(Repo.Dialect.SelectUserNameById(Context, 1)), CultureInfo.InvariantCulture);
        if (!string.Equals(value, "Alice-v2", StringComparison.Ordinal))
        {
            throw new InvalidOperationException($"Unexpected upsert result for {Repo.Dialect.DisplayName}: {value ?? "<null>"}.");
        }

        return value!;
    }
}
