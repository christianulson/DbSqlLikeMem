using System.Text.Json;

namespace DbSqlLikeMem.TestTools.DML;

/// <summary>
/// EN: Inserts user rows starting from a custom id and validates the persisted key range and names.
/// PT: Insere linhas de usuario a partir de um id customizado e valida a faixa de chaves e os nomes persistidos.
/// </summary>
/// <param name="repo">EN: Repository used to execute SQL commands. PT: Repositorio usado para executar comandos SQL.</param>
/// <param name="context">EN: Scenario context with the current table names. PT: Contexto do cenario com os nomes atuais das tabelas.</param>
public class InsertCustomStartUsersServiceTest(
        RepoService repo,
        FidelityTestContext context
    ) : BaseServiceTest(repo, context),
        IBaseServiceTest
{
    /// <summary>
    /// EN: Inserts three user rows starting at id 10 and validates the first and last persisted names.
    /// PT: Insere tres linhas de usuario a partir do id 10 e valida os primeiros e ultimos nomes persistidos.
    /// </summary>
    /// <param name="args">EN: Optional starting user id for the insert sequence. PT: Id inicial opcional do usuario para a sequencia de insert.</param>
    public virtual async Task<object?> RunTestAsync(params object[] args)
    {
        var startId = args.Length > 0 ? (int)args[0] : 10;
        var middleId = startId + 1;
        var lastId = startId + 2;

        await Repo.ExecuteNonQueryAsync(Repo.Dialect.InsertUser(Context, startId, $"User-{startId}"));
        await Repo.ExecuteNonQueryAsync(Repo.Dialect.InsertUser(Context, middleId, $"User-{middleId}"));
        await Repo.ExecuteNonQueryAsync(Repo.Dialect.InsertUser(Context, lastId, $"User-{lastId}"));

        var firstName = Convert.ToString(
            await Repo.ExecuteScalarAsync(Repo.Dialect.SelectUserNameById(Context, startId)),
            CultureInfo.InvariantCulture) ?? string.Empty;
        var lastName = Convert.ToString(
            await Repo.ExecuteScalarAsync(Repo.Dialect.SelectUserNameById(Context, lastId)),
            CultureInfo.InvariantCulture) ?? string.Empty;

        if (!string.Equals(firstName, $"User-{startId}", StringComparison.Ordinal)
            || !string.Equals(lastName, $"User-{lastId}", StringComparison.Ordinal))
        {
            throw new InvalidOperationException($"Unexpected custom-start insert result for {Repo.Dialect.DisplayName}: {JsonSerializer.Serialize(new { firstName, lastName })}.");
        }

        return (firstName, lastName);
    }
}
