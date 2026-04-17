using System.Text.Json;

namespace DbSqlLikeMem.TestTools.DML;

/// <summary>
/// EN: Reads the next sequence value for the configured sequence.
/// PT: Lê o próximo valor da sequência configurada.
/// </summary>
/// <param name="repo"></param>
/// <param name="context"></param>
public class DmlMutationSequenceInsertRoundTripServiceTest(
       RepoService repo,
       FidelityTestContext context
    ) : BaseServiceTest(repo, context),
        IBaseServiceTest
{
    /// <summary>
    /// EN: Reads the next sequence value for the configured sequence.
    /// PT: Lê o próximo valor da sequência configurada.
    /// </summary>
    public virtual async Task<object?> RunTestAsync(params object[] args)
    {
        if (!Repo.Dialect.SupportsSequence)
            throw new NotSupportedException($"{Repo.Dialect.DisplayName} does not support the sequence benchmark.");

        var first = Convert.ToInt64(await Repo.ExecuteScalarAsync(Repo.Dialect.NextSequenceValue(Context)), CultureInfo.InvariantCulture);
        await Repo.ExecuteNonQueryAsync(Repo.Dialect.InsertUser(Context, (int)first, $"Seq-{first}"));
        var second = Convert.ToInt64(await Repo.ExecuteScalarAsync(Repo.Dialect.NextSequenceValue(Context)), CultureInfo.InvariantCulture);
        await Repo.ExecuteNonQueryAsync(Repo.Dialect.InsertUser(Context, (int)second, $"Seq-{second}"));
        GC.KeepAlive(first);
        GC.KeepAlive(second);

        var lst = await Repo.ExecuteReaderAsync($"SELECT MIN(Id), MAX(Id), COUNT(*) FROM {Context.TbUsersFullName}");
        if (lst?.Count != 1
            || lst[0].Count != 1
            || lst[0][0].Length != 3
            || Convert.ToInt64(lst[0][0][0], CultureInfo.InvariantCulture) != first
            || Convert.ToInt64(lst[0][0][1], CultureInfo.InvariantCulture) != second
            || Convert.ToInt64(lst[0][0][2], CultureInfo.InvariantCulture) != 2)
            throw new InvalidOperationException($"Unexpected sequence insert results for {Repo.Dialect.DisplayName}: {JsonSerializer.Serialize(lst)}.");

        return new
        {
            first,
            second,
            minId = Convert.ToInt64(lst[0][0][0], CultureInfo.InvariantCulture),
            maxId = Convert.ToInt64(lst[0][0][1], CultureInfo.InvariantCulture),
            rowCount = Convert.ToInt64(lst[0][0][2], CultureInfo.InvariantCulture)
        };
    }
}
