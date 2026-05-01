namespace DbSqlLikeMem.TestTools.DML;

/// <summary>
/// EN: Creates and drops the rows needed for the update-with-IN-subquery fidelity test.
/// PT-br: Cria e remove as linhas necessarias para o teste de fidelidade de update com subquery IN.
/// </summary>
public sealed class UpdateWithInSubqueryScenario(
    RepoService repo,
    FidelityTestContext context
) : BaseScenario(repo, context), ITestScenario
{
    /// <summary>
    /// EN: Creates the tables and seeds the rows needed by the fidelity check.
    /// PT-br: Cria as tabelas e preenche as linhas necessarias para a validacao de fidelidade.
    /// </summary>
    public async Task CreateScenarioAsync()
    {
        await Repo.ExecuteNonQueryAsync(@"
CREATE TABLE WALLET (
    WLT_ID NUMBER(10,0) NOT NULL,
    VEN_ID NUMBER(5,0) NOT NULL,
    USR_ID NUMBER(10,0) NOT NULL,
    WLT_DEVICEID VARCHAR2(50) NOT NULL,
    WLT_STATUS CHAR(1) NOT NULL,
    CONSTRAINT PK_WALLET PRIMARY KEY (WLT_ID)
)");

        await Repo.ExecuteNonQueryAsync(@"
CREATE TABLE WALLETHOTLIST (
    WLTHOT_ID NUMBER(10,0) NOT NULL,
    WLT_ID NUMBER(10,0) NOT NULL,
    WLTHOT_STATUS CHAR(1) NOT NULL,
    WLTHOT_DTCREATED TIMESTAMP(6) NOT NULL,
    WLTHOT_DTDELETED TIMESTAMP(6),
    CONSTRAINT PK_WALLETHOTLIST PRIMARY KEY (WLTHOT_ID)
)");

        await Repo.ExecuteNonQueryAsync(@"
INSERT INTO WALLET (WLT_ID, VEN_ID, USR_ID, WLT_DEVICEID, WLT_STATUS)
VALUES (101, 7, 1, 'DEVICE-1', 'A')");

        await Repo.ExecuteNonQueryAsync(@"
INSERT INTO WALLET (WLT_ID, VEN_ID, USR_ID, WLT_DEVICEID, WLT_STATUS)
VALUES (202, 9, 1, 'DEVICE-2', 'A')");

        await Repo.ExecuteNonQueryAsync(@"
INSERT INTO WALLETHOTLIST (WLTHOT_ID, WLT_ID, WLTHOT_STATUS, WLTHOT_DTCREATED, WLTHOT_DTDELETED)
VALUES (1, 101, 'A', :createdAt, NULL)",
            addParameters: command => Repo.Dialect.AddParameter(command, "createdAt", DbType.DateTime, new DateTime(2026, 4, 30, 10, 0, 0, DateTimeKind.Utc)));

        await Repo.ExecuteNonQueryAsync(@"
INSERT INTO WALLETHOTLIST (WLTHOT_ID, WLT_ID, WLTHOT_STATUS, WLTHOT_DTCREATED, WLTHOT_DTDELETED)
VALUES (2, 202, 'A', :createdAt, NULL)",
            addParameters: command => Repo.Dialect.AddParameter(command, "createdAt", DbType.DateTime, new DateTime(2026, 4, 30, 11, 0, 0, DateTimeKind.Utc)));
    }

    /// <summary>
    /// EN: Drops the tables used by the fidelity fixture.
    /// PT-br: Remove as tabelas usadas pela fixture de fidelidade.
    /// </summary>
    public async Task DropScenarioAsync()
    {
        try
        {
            await Repo.ExecuteNonQueryAsync(Repo.Dialect.DropTable("WALLETHOTLIST"));
        }
        catch (Exception ex) when (IsMissingTableException(ex))
        {
        }

        try
        {
            await Repo.ExecuteNonQueryAsync(Repo.Dialect.DropTable("WALLET"));
        }
        catch (Exception ex) when (IsMissingTableException(ex))
        {
        }
    }

    private static bool IsMissingTableException(Exception ex)
    {
        var message = ex.GetBaseException().Message;
        return message.Contains("does not exist", StringComparison.OrdinalIgnoreCase)
            || message.Contains("doesn't exist", StringComparison.OrdinalIgnoreCase)
            || message.Contains("doesnt exist", StringComparison.OrdinalIgnoreCase)
            || message.Contains("not exist", StringComparison.OrdinalIgnoreCase)
            || message.Contains("undefined name", StringComparison.OrdinalIgnoreCase)
            || message.Contains("not found", StringComparison.OrdinalIgnoreCase)
            || message.Contains("ora-00942", StringComparison.OrdinalIgnoreCase)
            || message.Contains("given key was not present", StringComparison.OrdinalIgnoreCase);
    }
}
