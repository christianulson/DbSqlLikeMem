using System.Text.Json;

namespace DbSqlLikeMem.TestTools.DML;

/// <summary>
/// EN: Reads the next sequence value for the configured sequence.
/// PT: Lê o próximo valor da sequência configurada.
/// </summary>
/// <param name="repo"></param>
/// <param name="context"></param>
public class DmlMutationSequenceTemporalMatrixServiceTest(
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

        if (Repo.Dialect.Provider == ProviderId.Oracle)
            return await RunOracleMatrixAsync();

        var nowExpr = Repo.Dialect.TemporalCurrentTimestampExpression();
        var nextDayExpr = Repo.Dialect.TemporalDateAddExpression();

        var seqFirstCte = BuildSingleRowSequenceCte("seq_first");
        var seqSecondCte = BuildSingleRowSequenceCte("seq_second");

        var lst = await Repo.ExecuteReaderAsync($"""
WITH {seqFirstCte},
{seqSecondCte}
SELECT
    s1.SeqValue,
    s2.SeqValue,
    CASE WHEN s1.SeqValue = 10 THEN 1 ELSE 0 END AS FirstIsTen,
    CASE WHEN s2.SeqValue = 11 THEN 1 ELSE 0 END AS SecondIsEleven,
    CASE WHEN {nowExpr} IS NOT NULL THEN 1 ELSE 0 END AS NowPresent,
    CASE WHEN {nextDayExpr} > {nowExpr} THEN 1 ELSE 0 END AS NextDayAfterNow,
    CASE WHEN s1.SeqValue < s2.SeqValue THEN 1 ELSE 0 END AS IsAscending
FROM seq_first s1
CROSS JOIN seq_second s2
WHERE s1.SeqValue BETWEEN 10 AND 10
  AND s2.SeqValue BETWEEN 11 AND 11
""");
        if (lst?.Count != 1
            || lst[0].Count != 1
            || lst[0][0].Length != 7
            || Convert.ToInt64(lst[0][0][0], CultureInfo.InvariantCulture) != 10L
            || Convert.ToInt64(lst[0][0][1], CultureInfo.InvariantCulture) != 11L
            || Convert.ToInt32(lst[0][0][2], CultureInfo.InvariantCulture) != 1
            || Convert.ToInt32(lst[0][0][3], CultureInfo.InvariantCulture) != 1
            || Convert.ToInt32(lst[0][0][4], CultureInfo.InvariantCulture) != 1
            || Convert.ToInt32(lst[0][0][5], CultureInfo.InvariantCulture) != 1
            || Convert.ToInt32(lst[0][0][6], CultureInfo.InvariantCulture) != 1
            )
            throw new InvalidOperationException($"Unexpected sequence insert results for {Repo.Dialect.DisplayName}: {JsonSerializer.Serialize(lst)}.");

        return lst[0];
    }

    private async Task<object?> RunOracleMatrixAsync()
    {
        var nowExpr = Repo.Dialect.TemporalCurrentTimestampExpression();
        var nextDayExpr = Repo.Dialect.TemporalDateAddExpression();
        var firstValue = Convert.ToInt64(await Repo.ExecuteScalarAsync(Repo.Dialect.NextSequenceValue(Context))!, CultureInfo.InvariantCulture);
        var secondValue = Convert.ToInt64(await Repo.ExecuteScalarAsync(Repo.Dialect.NextSequenceValue(Context))!, CultureInfo.InvariantCulture);

        var lst = await Repo.ExecuteReaderAsync($"""
SELECT
    s1.SeqValue,
    s2.SeqValue,
    CASE WHEN s1.SeqValue = 10 THEN 1 ELSE 0 END AS FirstIsTen,
    CASE WHEN s2.SeqValue = 11 THEN 1 ELSE 0 END AS SecondIsEleven,
    CASE WHEN {nowExpr} IS NOT NULL THEN 1 ELSE 0 END AS NowPresent,
    CASE WHEN {nextDayExpr} > {nowExpr} THEN 1 ELSE 0 END AS NextDayAfterNow,
    CASE WHEN s1.SeqValue < s2.SeqValue THEN 1 ELSE 0 END AS IsAscending
FROM (SELECT {firstValue} AS SeqValue FROM DUAL) s1
CROSS JOIN (SELECT {secondValue} AS SeqValue FROM DUAL) s2
WHERE s1.SeqValue BETWEEN 10 AND 10
  AND s2.SeqValue BETWEEN 11 AND 11
""");
        if (lst?.Count != 1
            || lst[0].Count != 1
            || lst[0][0].Length != 7
            || Convert.ToInt64(lst[0][0][0], CultureInfo.InvariantCulture) != 10L
            || Convert.ToInt64(lst[0][0][1], CultureInfo.InvariantCulture) != 11L
            || Convert.ToInt32(lst[0][0][2], CultureInfo.InvariantCulture) != 1
            || Convert.ToInt32(lst[0][0][3], CultureInfo.InvariantCulture) != 1
            || Convert.ToInt32(lst[0][0][4], CultureInfo.InvariantCulture) != 1
            || Convert.ToInt32(lst[0][0][5], CultureInfo.InvariantCulture) != 1
            || Convert.ToInt32(lst[0][0][6], CultureInfo.InvariantCulture) != 1
            )
            throw new InvalidOperationException($"Unexpected sequence insert results for {Repo.Dialect.DisplayName}: {JsonSerializer.Serialize(lst)}.");

        return lst[0];
    }

    private string BuildSingleRowSequenceCte(string alias)
    {
        return $"{alias} (SeqValue) AS ({Repo.Dialect.SelectNextSequenceValue(Context)})";
    }
}
