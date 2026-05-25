namespace DbSqlLikeMem.TestTools.DML;

/// <summary>
/// EN: Executes the update that matches rows through an IN subquery with a table alias and returns the persisted rowset.
/// PT-br: Executa o update que casa linhas por meio de uma subquery IN com alias de tabela e retorna o conjunto persistido de linhas.
/// </summary>
/// <param name="repo">EN: The repository used to execute SQL. PT-br: O repositório usado para executar SQL.</param>
/// <param name="context">EN: The active fidelity-test context. PT-br: O contexto ativo do teste de fidelidade.</param>
public sealed class UpdateWithInSubqueryServiceTest(
    RepoService repo,
    FidelityTestContext context
) : BaseServiceTest(repo, context), IBaseServiceTest
{
    /// <summary>
    /// EN: Updates the matching row through an IN subquery with a table alias and returns the affected row count and final rowset.
    /// PT-br: Atualiza a linha correspondente por meio de uma subquery IN com alias de tabela e retorna a contagem afetada e o conjunto final de linhas.
    /// </summary>
    public async Task<object?> RunTestAsync(params object[] args)
    {
        var affected = await Repo.ExecuteNonQueryAsync(
            """
UPDATE WALLETHOTLIST
   SET WLTHOT_DTDELETED = :dateTime
     , WLTHOT_Status = 'I'
 WHERE WLT_ID IN (
    SELECT W.WLT_ID
      FROM WALLET W
     WHERE W.WLT_DEVICEID = :deviceId
       AND W.USR_ID = :userId
       AND W.WLT_STATUS = 'A'
)
   AND WLTHOT_DTDELETED IS NULL
""",
            addParameters: command =>
            {
                Repo.Dialect.AddParameter(command, "dateTime", DbType.DateTime, new DateTime(2026, 4, 30, 12, 0, 0, DateTimeKind.Utc));
                Repo.Dialect.AddParameter(command, "deviceId", DbType.String, "DEVICE-1");
                Repo.Dialect.AddParameter(command, "userId", DbType.Int64, 1L);
            });

        var rows = await Repo.ExecuteReaderAsync("""
SELECT WLTHOT_ID, WLT_ID, WLTHOT_STATUS, WLTHOT_DTDELETED
  FROM WALLETHOTLIST
 ORDER BY WLTHOT_ID
""");

        if (rows.Count != 1)
        {
            throw new InvalidOperationException($"Unexpected result set count for {Repo.Dialect.DisplayName}: {rows.Count}.");
        }

        var resultSet = rows[0];
        return new
        {
            affected,
            rows = resultSet
        };
    }
}
