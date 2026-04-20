namespace DbSqlLikeMem.Npgsql.Test.Fidelity.DML;

/// <summary>
/// EN: Applies ALTER SEQUENCE RESTART WITH and reads the next values for PostgreSQL sequence fidelity.
/// PT: Aplica ALTER SEQUENCE RESTART WITH e le os proximos valores para fidelidade de sequence do PostgreSQL.
/// </summary>
public sealed class SequenceAlterRestartServiceTest(
    RepoService repo,
    FidelityTestContext context
    ) : BaseServiceTest(repo, context),
        IBaseServiceTest
{
    /// <summary>
    /// EN: Recreates the next sequence values after a restart on the current sequence and can also expose session-local values.
    /// PT: Recria os proximos valores da sequence apos um restart na sequence atual e tambem pode expor valores locais da sessao.
    /// </summary>
    public async Task<object?> RunTestAsync(params object[] args)
    {
        var includeSessionValues = args.Length > 0 && args[0] is bool b && b;

        using (var alter = Repo.Cnn.CreateCommand())
        {
            alter.CommandText = $"ALTER SEQUENCE {Context.Seq} RESTART WITH 10";
            await alter.ExecuteNonQueryAsync();
        }

        if (!includeSessionValues)
        {
            var first = await ExecuteScalarLongAsync($"SELECT nextval('{Context.Seq}')");
            var second = await ExecuteScalarLongAsync($"SELECT nextval('{Context.Seq}')");
            return new[] { first, second };
        }

        var firstValue = await ExecuteScalarLongAsync($"SELECT nextval('{Context.Seq}')");
        var firstCurr = await ExecuteScalarLongAsync($"SELECT currval('{Context.Seq}')");
        var firstLast = await ExecuteScalarLongAsync("SELECT lastval()");
        var secondValue = await ExecuteScalarLongAsync($"SELECT nextval('{Context.Seq}')");
        var secondCurr = await ExecuteScalarLongAsync($"SELECT currval('{Context.Seq}')");
        var secondLast = await ExecuteScalarLongAsync("SELECT lastval()");
        return new[] { firstValue, firstCurr, firstLast, secondValue, secondCurr, secondLast };
    }

    private async Task<long> ExecuteScalarLongAsync(string sql)
    {
        using var command = Repo.Cnn.CreateCommand();
        command.CommandText = sql;
        return Convert.ToInt64(await command.ExecuteScalarAsync(), CultureInfo.InvariantCulture);
    }
}
