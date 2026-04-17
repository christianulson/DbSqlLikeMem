namespace DbSqlLikeMem.TestTools.Query;

/// <summary>
/// EN: Executes the primary-key select command for the shared query scenario.
/// PT: Executa o comando de selecao por chave primaria para o cenario de consulta compartilhado.
/// </summary>
public class SelectByPKServiceTest(
        RepoService repo,
        FidelityTestContext context
    ) : BaseServiceTest(repo, context),
        IBaseServiceTest
{
    /// <summary>
    /// EN: Reads the seeded row by primary key and validates the returned value.
    /// PT: Lê a linha inserida pela chave primaria e valida o valor retornado.
    /// </summary>
    /// <param name="args">EN: Scenario arguments that include the users table name. PT: Argumentos do cenario que incluem o nome da tabela de usuarios.</param>
    public async Task<object?> RunTestAsync(params object[] args)
    {
        string? value;

        var sql = Repo.Dialect.SelectUserNameById(Context, 1);
        var rawValue = await Repo. ExecuteScalarAsync(sql);
        value = Convert.ToString(rawValue);

        if (!string.Equals(value, "Alice", StringComparison.Ordinal)
            && Repo.Dialect.Provider == ProviderId.Oracle)
        {
            using var command = Repo.Cnn.CreateCommand();
            command.CommandText = sql;

            using var reader = await command.ExecuteReaderAsync();
            if (await reader.ReadAsync())
            {
                var nameValue = reader.GetValue(0);
                value = Convert.ToString(nameValue);
            }
        }

        if (!string.Equals(value, "Alice", StringComparison.Ordinal))
            throw new InvalidOperationException($"Unexpected select result for {Repo.Dialect.DisplayName}: {value ?? "<null>"}.");
        return value!;
    }
}
