namespace DbSqlLikeMem.TestTools.DML;

/// <summary>
/// EN: Attempts to insert a row without a required NOT NULL column and verifies that the provider rejects it.
/// PT: Tenta inserir uma linha sem uma coluna NOT NULL obrigatoria e verifica se o provedor rejeita a operacao.
/// </summary>
/// <param name="repo">EN: Repository used to execute SQL commands. PT: Repositorio usado para executar comandos SQL.</param>
/// <param name="context">EN: Scenario context with the current table names. PT: Contexto do cenario com os nomes atuais das tabelas.</param>
public class InsertNotNullWithoutDefaultServiceTest(
        RepoService repo,
        FidelityTestContext context
    ) : BaseServiceTest(repo, context),
        IBaseServiceTest
{
    /// <summary>
    /// EN: Inserts a row that omits a required NOT NULL column and returns true when the provider rejects the insert.
    /// PT: Insere uma linha que omite uma coluna NOT NULL obrigatoria e retorna true quando o provedor rejeita o insert.
    /// </summary>
    /// <param name="args">EN: Optional row id. PT: Id da linha opcional.</param>
    /// <returns>EN: True when the insert fails as expected. PT: True quando o insert falha como esperado.</returns>
    public virtual async Task<object?> RunTestAsync(params object[] args)
    {
        var id = args.Length > 0 ? (int)args[0] : 2;

        try
        {
            await Repo.ExecuteNonQueryAsync($"""
INSERT INTO {Context.TbUsersFullName} (
    Id
) VALUES (
    {Repo.Dialect.Parameter("id")}
)
""", addParameters: command =>
            {
                AddParameter(command, "id", DbType.Int32, id);
            });
        }
        catch
        {
            return true;
        }

        throw new InvalidOperationException($"Expected {Repo.Dialect.DisplayName} to reject INSERT without NOT NULL column values.");
    }
}
