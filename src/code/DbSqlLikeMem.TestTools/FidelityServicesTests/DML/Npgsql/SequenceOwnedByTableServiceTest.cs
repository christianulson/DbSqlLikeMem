namespace DbSqlLikeMem.Npgsql.Test.Fidelity.DML;

/// <summary>
/// EN: Attaches a PostgreSQL sequence to a table column and verifies the sequence is dropped with the table.
/// PT: Anexa uma sequence PostgreSQL a uma coluna de tabela e verifica se a sequence e removida junto com a tabela.
/// </summary>
public sealed class SequenceOwnedByTableServiceTest(
    RepoService repo,
    FidelityTestContext context
    ) : BaseServiceTest(repo, context),
        IBaseServiceTest
{
    /// <summary>
    /// EN: Returns the initial sequence value and whether the sequence became unavailable after the table drop.
    /// PT: Retorna o valor inicial da sequence e se ela ficou indisponivel depois da remocao da tabela.
    /// </summary>
    public async Task<object?> RunTestAsync(params object[] args)
    {
        _ = args;

        await ExecuteNonQueryAsync($"CREATE TABLE {Context.TbUsersFullName} (Id BIGINT NOT NULL PRIMARY KEY)");
        await ExecuteNonQueryAsync($"CREATE SEQUENCE {Context.Seq} START WITH 1 INCREMENT BY 1");
        await ExecuteNonQueryAsync($"ALTER SEQUENCE {Context.Seq} OWNED BY {Context.TbUsersFullName}.Id");

        var first = await ExecuteScalarLongAsync($"SELECT nextval('{Context.Seq}')");

        await ExecuteNonQueryAsync($"DROP TABLE {Context.TbUsersFullName}");

        var missing = false;
        try
        {
            await ExecuteScalarLongAsync($"SELECT nextval('{Context.Seq}')");
        }
        catch (Exception ex) when (IsMissingSequenceException(ex))
        {
            missing = true;
        }

        return new[] { first, missing ? 1L : 0L };
    }

    private async Task ExecuteNonQueryAsync(string sql)
    {
        using var command = Repo.Cnn.CreateCommand();
        command.CommandText = sql;
        await command.ExecuteNonQueryAsync();
    }

    private async Task<long> ExecuteScalarLongAsync(string sql)
    {
        using var command = Repo.Cnn.CreateCommand();
        command.CommandText = sql;
        return Convert.ToInt64(await command.ExecuteScalarAsync(), CultureInfo.InvariantCulture);
    }

    private static bool IsMissingSequenceException(Exception ex)
    {
        var message = ex.GetBaseException().Message;
        return message.Contains("does not exist", StringComparison.OrdinalIgnoreCase)
            || message.Contains("undefined table", StringComparison.OrdinalIgnoreCase)
            || message.Contains("invalid object name", StringComparison.OrdinalIgnoreCase)
            || message.Contains("no such table", StringComparison.OrdinalIgnoreCase);
    }
}
