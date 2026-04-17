namespace DbSqlLikeMem.TestTools.DML;

/// <summary>
/// EN: Executes shared insert workflows and validates row counts for the current provider.
/// PT: Executa fluxos compartilhados de insert e valida contagens de linhas para o provedor atual.
/// </summary>
public class InsertParameterUsersServiceTest(
        RepoService repo,
        FidelityTestContext context
    ) : BaseServiceTest(repo, context),
        IBaseServiceTest
{
    /// <summary>
    /// EN: Inserts the requested number of user rows and validates the final count.
    /// PT: Insere a quantidade solicitada de linhas de usuario e valida a contagem final.
    /// </summary>
    /// <returns>EN: The final row count. PT: A contagem final de linhas.</returns>
    public virtual async Task<object?> RunTestAsync(params object[] args)
    {
        var id = 1;
        var name = $"User {id}";

        var affected = await Repo.ExecuteNonQueryAsync($"""
INSERT INTO {Context.TbUsersFullName} (
    Id,
    Name
)
VALUES (
    {Repo.Dialect.Parameter("id")},
    {Repo.Dialect.Parameter("name")}
)
""", addParameters: command =>
        {
            AddParameter(command, "id", DbType.Int32, id);
            AddParameter(command, "name", DbType.String, name);
        });

        if (affected < 1)
        {
            throw new InvalidOperationException($"Unexpected parameter insert rowcount for {Repo.Dialect.DisplayName}: {affected}.");
        }

        var result = Convert.ToString(await Repo.ExecuteScalarAsync($"""
SELECT Name
FROM {Context.TbUsersFullName}
WHERE Id = {Repo.Dialect.Parameter("id")}
""", addParameters: command =>
            AddParameter(command, "id", DbType.Int32, id)
        ), CultureInfo.InvariantCulture);
        result.Should().Be(name);

        GC.KeepAlive(id);
        GC.KeepAlive(name);
        return affected;
    }
}
