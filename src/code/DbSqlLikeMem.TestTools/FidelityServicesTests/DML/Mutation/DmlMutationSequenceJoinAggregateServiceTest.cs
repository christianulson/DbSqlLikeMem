namespace DbSqlLikeMem.TestTools.DML;

/// <summary>
/// EN: Reads the next sequence value for the configured sequence.
/// PT-br: Lê o próximo valor da sequência configurada.
/// </summary>
/// <param name="repo"></param>
/// <param name="context"></param>
public class DmlMutationSequenceJoinAggregateServiceTest(
       RepoService repo,
       FidelityTestContext context
    ) : BaseServiceTest(repo, context),
        IBaseServiceTest
{
    /// <summary>
    /// EN: Reads the next sequence value for the configured sequence.
    /// PT-br: Lê o próximo valor da sequência configurada.
    /// </summary>
    public virtual async Task<object?> RunTestAsync(params object[] args)
    {
        if (!Repo.Dialect.SupportsSequence)
            throw new NotSupportedException($"{Repo.Dialect.DisplayName} does not support the sequence benchmark.");

        var firstUserId = Convert.ToInt64(await Repo.ExecuteScalarAsync(Repo.Dialect.NextSequenceValue(Context)), CultureInfo.InvariantCulture);
        var secondUserId = Convert.ToInt64(await Repo.ExecuteScalarAsync(Repo.Dialect.NextSequenceValue(Context)), CultureInfo.InvariantCulture);

        await Repo.ExecuteNonQueryAsync(Repo.Dialect.InsertUser(Context, (int)firstUserId, $"Seq-{firstUserId}"));
        await Repo.ExecuteNonQueryAsync(Repo.Dialect.InsertUser(Context, (int)secondUserId, $"Seq-{secondUserId}"));
        await Repo.ExecuteNonQueryAsync(Repo.Dialect.InsertOrder(Context, 100, (int)firstUserId, "A", "o-100", 1.25m, 1, false, Repo.Dialect.TemporalCurrentTimestampExpression()));
        await Repo.ExecuteNonQueryAsync(Repo.Dialect.InsertOrder(Context, 101, (int)firstUserId, "B", "o-101", 2.75m, 2, true, Repo.Dialect.TemporalCurrentTimestampExpression()));
        await Repo.ExecuteNonQueryAsync(Repo.Dialect.InsertOrder(Context, 102, (int)secondUserId, "C", "o-102", 5.50m, 4, false, Repo.Dialect.TemporalCurrentTimestampExpression()));


        var lst = await Repo.ExecuteReaderAsync($"""
SELECT
    u.Id,
    COUNT(o.Note) AS OrderCount,
    SUM(o.Quantity) AS TotalQuantity,
    ROUND(SUM(o.Amount), 2) AS TotalAmount
FROM {Context.TbUsersFullName} u
INNER JOIN {Context.TbOrdersFullName} o ON o.{Context.TbUsers}Id = u.Id
GROUP BY u.Id
ORDER BY u.Id
""");
        lst.Should().BeEquivalentTo(new List<List<object?[]>>
        {
            new()
            {
                new object?[] { 1L, 2L, 2L, 0.00m },
                new object?[] { firstUserId, 2L, 3L, 4.00m },
                new object?[] { secondUserId, 1L, 4L, 5.50m }
            }
        });

        return lst;
    }
}
