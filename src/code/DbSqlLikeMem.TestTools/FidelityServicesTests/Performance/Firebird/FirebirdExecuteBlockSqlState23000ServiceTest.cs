namespace DbSqlLikeMem.TestTools.Performance;

/// <summary>
/// EN: Executes the Firebird EXECUTE BLOCK benchmark that traps SQLSTATE 23000.
/// PT: Executa o benchmark Firebird de EXECUTE BLOCK que trata SQLSTATE 23000.
/// </summary>
/// <param name="repo">EN: Repository used to execute SQL commands. PT: Repositorio usado para executar comandos SQL.</param>
/// <param name="context">EN: Scenario context for the current benchmark run. PT: Contexto do cenario para a execucao atual do benchmark.</param>
public class FirebirdExecuteBlockSqlState23000ServiceTest(
        RepoService repo,
        FidelityTestContext context
    ) : BaseServiceTest(repo, context),
        IBaseServiceTest
{
    private const string UsersTable = "FB_EXEC_BLOCK_SQLSTATE_23000";

    /// <summary>
    /// EN: Creates the table, seeds one row, and executes an EXECUTE BLOCK that handles SQLSTATE 23000.
    /// PT: Cria a tabela, insere uma linha inicial e executa um EXECUTE BLOCK que trata SQLSTATE 23000.
    /// </summary>
    public async Task<object?> RunTestAsync(params object[] args)
    {
        await TryDropTableAsync();

        try
        {
            await Repo.ExecuteNonQueryAsync($"""
CREATE TABLE {UsersTable} (
    Id INTEGER NOT NULL,
    Name VARCHAR(100) NOT NULL,
    CONSTRAINT PK_{UsersTable} PRIMARY KEY (Id)
)
""");

            await Repo.ExecuteNonQueryAsync($"""INSERT INTO {UsersTable} (Id, Name) VALUES (1, 'Seed')""");

            using var transaction = Repo.BeginTransaction();
            try
            {
                await Repo.ExecuteNonQueryAsync($"""
EXECUTE BLOCK AS
BEGIN
    EXECUTE STATEMENT 'INSERT INTO {UsersTable} (Id, Name) VALUES (1, ''Fail'')';
    WHEN SQLSTATE '23000' DO
    BEGIN
        INSERT INTO {UsersTable} (Id, Name) VALUES (2, 'SqlState23000');
    END
END
""", transaction);
            }
            finally
            {
                try
                {
                    transaction.Rollback();
                }
                catch
                {
                    // Ignore rollback failures during benchmark cleanup.
                }
            }

            return null;
        }
        finally
        {
            await TryDropTableAsync();
        }
    }

    private async Task TryDropTableAsync()
    {
        try
        {
            await Repo.ExecuteNonQueryAsync(Repo.Dialect.DropTable(UsersTable));
        }
        catch
        {
            // Ignore cleanup failures during benchmark teardown.
        }
    }
}
