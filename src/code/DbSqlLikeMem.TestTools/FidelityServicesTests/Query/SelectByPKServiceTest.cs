namespace DbSqlLikeMem.TestTools.Query;

/// <summary>
/// EN: Executes the primary-key select command for the shared query scenario.
/// PT-br: Executa o comando de selecao por chave primaria para o cenario de consulta compartilhado.
/// </summary>
public class SelectByPKServiceTest(
        RepoService repo,
        FidelityTestContext context
    ) : BaseServiceTest(repo, context),
        IBaseServiceTest
{
    /// <summary>
    /// EN: Reads the seeded row by primary key and returns the full result snapshot.
    /// PT-br: Lê a linha inserida pela chave primaria e retorna o snapshot completo do resultado.
    /// </summary>
    /// <param name="args">EN: Optional primary user id for the lookup. PT-br: Id principal opcional do usuario para a consulta.</param>
    public async Task<object?> RunTestAsync(params object[] args)
        => await RunSelectByPkAsync(args);

    /// <summary>
    /// EN: Reads the seeded row by primary key and returns the full result snapshot.
    /// PT-br: Lê a linha inserida pela chave primaria e retorna o snapshot completo do resultado.
    /// </summary>
    /// <param name="args">EN: Optional primary user id for the lookup. PT-br: Id principal opcional do usuario para a consulta.</param>
    public async Task<QueryResultSnapshot> RunSelectByPkAsync(params object[] args)
    {
        var userId = args.Length > 0 ? (int)args[0] : 1;
        var sql = Repo.Dialect.SelectUserNameById(Context, userId);
        using var command = Repo.Cnn.CreateCommand();
        command.CommandText = sql;

        using var reader = await command.ExecuteReaderAsync();
        var snapshot = QueryResultSnapshotReader.Capture(reader);
        if (Repo.Dialect.Provider is ProviderId.Oracle or ProviderId.Db2)
        {
            var columnNames = new string[snapshot.ColumnNames.Count];
            for (var i = 0; i < snapshot.ColumnNames.Count; i++)
                columnNames[i] = snapshot.ColumnNames[i].ToUpperInvariant();

            return snapshot with { ColumnNames = columnNames };
        }

        return snapshot;
    }
}
