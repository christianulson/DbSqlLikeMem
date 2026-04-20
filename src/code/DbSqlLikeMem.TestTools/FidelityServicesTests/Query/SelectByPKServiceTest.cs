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
    /// EN: Reads the seeded row by primary key and returns the full result snapshot.
    /// PT: Lê a linha inserida pela chave primaria e retorna o snapshot completo do resultado.
    /// </summary>
    /// <param name="args">EN: Scenario arguments that include the users table name. PT: Argumentos do cenario que incluem o nome da tabela de usuarios.</param>
    public async Task<object?> RunTestAsync(params object[] args)
    {
        var sql = Repo.Dialect.SelectUserNameById(Context, 1);
        using var command = Repo.Cnn.CreateCommand();
        command.CommandText = sql;

        using var reader = await command.ExecuteReaderAsync();
        var snapshot = QueryResultSnapshotReader.Capture(reader);
        if (Repo.Dialect.Provider == ProviderId.Oracle)
        {
            var columnNames = new string[snapshot.ColumnNames.Count];
            for (var i = 0; i < snapshot.ColumnNames.Count; i++)
                columnNames[i] = snapshot.ColumnNames[i].ToUpperInvariant();

            return snapshot with { ColumnNames = columnNames };
        }

        return snapshot;
    }
}
