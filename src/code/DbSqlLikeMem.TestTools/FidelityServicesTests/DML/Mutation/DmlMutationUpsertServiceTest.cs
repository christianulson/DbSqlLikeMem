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
    /// <param name="args">EN: Optional primary user id for the upsert flow. PT: Id principal opcional do usuario para o fluxo de upsert.</param>
    public async Task<object?> RunTestAsync(params object[] args)
    {
        if (!Repo.Dialect.SupportsUpsert)
        {
            throw new NotSupportedException($"{Repo.Dialect.DisplayName} does not support the upsert benchmark.");
        }

        var userId = args.Length > 0 ? (int)args[0] : 1;

        await Repo.ExecuteNonQueryAsync(Repo.Dialect.Upsert(Context, userId, "Alice-v2"));
        var value = Convert.ToString(await Repo.ExecuteScalarAsync(Repo.Dialect.SelectUserNameById(Context, userId)), CultureInfo.InvariantCulture);
        if (!string.Equals(value, "Alice-v2", StringComparison.Ordinal))
        {
            throw new InvalidOperationException($"Unexpected upsert result for {Repo.Dialect.DisplayName}: {value ?? "<null>"}.");
        }

        return value!;
    }
}
