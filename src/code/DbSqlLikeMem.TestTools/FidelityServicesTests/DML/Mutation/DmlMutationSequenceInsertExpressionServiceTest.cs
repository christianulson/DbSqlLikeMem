using System.Text.Json;

namespace DbSqlLikeMem.TestTools.DML;

/// <summary>
/// EN: Reads the next sequence value for the configured sequence.
/// PT-br: Lê o próximo valor da sequência configurada.
/// </summary>
/// <param name="repo"></param>
/// <param name="context"></param>
public class DmlMutationSequenceInsertExpressionServiceTest(
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

        await Repo.ExecuteNonQueryAsync($"INSERT INTO {Context.TbUsersFullName} (Id, Name) VALUES ({Repo.Dialect.NextSequenceValueExpression(Context)}, 'Seq-A')");
        await Repo.ExecuteNonQueryAsync($"INSERT INTO {Context.TbUsersFullName} (Id, Name) VALUES ({Repo.Dialect.NextSequenceValueExpression(Context)}, 'Seq-B')");

        var lst = await Repo.ExecuteReaderAsync($"SELECT MIN(Id), MAX(Id), COUNT(*) FROM {Context.TbUsersFullName}");
        if (lst?.Count != 1
            || lst[0].Count != 1
            || lst[0][0].Length != 3
            || Convert.ToInt64(lst[0][0][0], CultureInfo.InvariantCulture) != 10L
            || Convert.ToInt64(lst[0][0][1], CultureInfo.InvariantCulture) != 11L
            || Convert.ToInt64(lst[0][0][2], CultureInfo.InvariantCulture) != 2)
            throw new InvalidOperationException($"Unexpected sequence insert results for {Repo.Dialect.DisplayName}: {JsonSerializer.Serialize(lst)}.");

        return new
        {
            minId = Convert.ToInt64(lst[0][0][0], CultureInfo.InvariantCulture),
            maxId = Convert.ToInt64(lst[0][0][1], CultureInfo.InvariantCulture),
            rowCount = Convert.ToInt64(lst[0][0][2], CultureInfo.InvariantCulture)
        };
    }
}
