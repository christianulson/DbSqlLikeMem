namespace DbSqlLikeMem.Npgsql.Test.Fidelity.DML;

/// <summary>
/// EN: Drops a sequence inside a transaction and restores it through rollback for PostgreSQL fidelity.
/// PT: Remove uma sequence dentro de uma transacao e a restaura por rollback para fidelidade do PostgreSQL.
/// </summary>
public sealed class SequenceDropRollbackServiceTest(
    RepoService repo,
    FidelityTestContext context
    ) : BaseServiceTest(repo, context),
        IBaseServiceTest
{
    /// <summary>
    /// EN: Returns the observed sequence values and whether the dropped sequence stayed unavailable until rollback.
    /// PT: Retorna os valores observados da sequence e se a sequence removida permaneceu indisponivel ate o rollback.
    /// </summary>
    public async Task<object?> RunTestAsync(params object[] args)
    {
        var initial = await ExecuteScalarLongAsync($"SELECT nextval('{Context.Seq}')");

        using var transaction = await Repo.Cnn.BeginTransactionAsync();

        await ExecuteNonQueryAsync($"DROP SEQUENCE {Context.Seq}", transaction);

        var droppedMissing = false;
        try
        {
            await ExecuteScalarLongAsync($"SELECT nextval('{Context.Seq}')", transaction);
        }
        catch (InvalidOperationException ex) when (ex.Message.Contains("does not exist", StringComparison.OrdinalIgnoreCase))
        {
            droppedMissing = true;
        }

        await transaction.RollbackAsync();

        var afterRollback = await ExecuteScalarLongAsync($"SELECT nextval('{Context.Seq}')");
        return new[] { initial, droppedMissing ? 1L : 0L, afterRollback };

    }

    private async Task ExecuteNonQueryAsync(string sql, DbTransaction? transaction = null)
    {
        using var command = Repo.Cnn.CreateCommand();
        command.CommandText = sql;
        command.Transaction = transaction;
        await command.ExecuteNonQueryAsync();
    }

    private async Task<long> ExecuteScalarLongAsync(string sql, DbTransaction? transaction = null)
    {
        using var command = Repo.Cnn.CreateCommand();
        command.CommandText = sql;
        command.Transaction = transaction;
        return Convert.ToInt64(await command.ExecuteScalarAsync(), CultureInfo.InvariantCulture);
    }
}
