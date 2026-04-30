using System.Collections;

namespace DbSqlLikeMem.TestTools.DML;

/// <summary>
/// EN: Creates and drops a users table with optional seed rows for DML mutation workflows.
/// PT: Cria e remove uma tabela de usuarios com linhas iniciais opcionais para fluxos de mutacao DML.
/// </summary>
public sealed class UsersScenario(
        RepoService repo,
        FidelityTestContext context,
        params object?[][] seedRows
    ) : BaseScenario(repo, context),
        ITestScenario
{

    /// <summary>
    /// EN: Creates the users table and seeds the configured rows.
    /// PT: Cria a tabela de usuarios e preenche as linhas configuradas.
    /// </summary>
    public async Task CreateScenarioAsync()
    {

        await Repo.ExecuteNonQueryAsync(Repo.Dialect.CreateUsersTable(Context));
        if (seedRows != null)
            foreach (var (id, name) in ConvertSeedRows(seedRows))
            {
                await Repo.ExecuteNonQueryAsync(Repo.Dialect.InsertUser(Context, id, name));
            }
    }

    /// <summary>
    /// EN: Drops the users table created for the workflow.
    /// PT: Remove a tabela de usuarios criada para o fluxo.
    /// </summary>
    public Task DropScenarioAsync()
    {
        return DropTableIfPresentAsync();
    }

    private static (int id, string name)[] ConvertSeedRows(object?[][] seedRows)
    {
        var rows = new List<(int id, string name)>();
        foreach (var seedRow in seedRows)
        {
            AddSeedRow(seedRow, rows);
        }

        return [.. rows];
    }

    private static void AddSeedRow(object? value, ICollection<(int id, string name)> rows)
    {
        if (value is null)
            throw new ArgumentNullException(nameof(value));

        if (value is ValueTuple<int, string> tuple)
        {
            rows.Add(tuple);
            return;
        }

        if (value is object?[] array)
        {
            if (array.Length == 2
                && array[0] is int id
                && array[1] is string name)
            {
                rows.Add((id, name));
                return;
            }

            foreach (var item in array)
            {
                AddSeedRow(item, rows);
            }

            return;
        }

        if (value is IEnumerable enumerable && value is not string)
        {
            var items = enumerable.Cast<object?>().ToArray();
            if (items.Length == 2
                && items[0] is int id
                && items[1] is string name)
            {
                rows.Add((id, name));
                return;
            }

            foreach (var item in items)
            {
                AddSeedRow(item, rows);
            }

            return;
        }

        throw new ArgumentException($"Unsupported seed row value type: {value.GetType().FullName}.", nameof(value));
    }

    private async Task DropTableIfPresentAsync()
    {
        try
        {
            await Repo.ExecuteNonQueryAsync(Repo.Dialect.DropTable(Context.TbUsersFullName));
        }
        catch (Exception ex) when (ShouldIgnoreDropException(ex))
        {
        }
    }

    private static bool ShouldIgnoreDropException(Exception ex)
    {
        var message = ex.GetBaseException().Message;
        return message.Contains("does not exist", StringComparison.OrdinalIgnoreCase)
            || message.Contains("doesn't exist", StringComparison.OrdinalIgnoreCase)
            || message.Contains("doesnt exist", StringComparison.OrdinalIgnoreCase)
            || message.Contains("not exist", StringComparison.OrdinalIgnoreCase)
            || message.Contains("undefined name", StringComparison.OrdinalIgnoreCase)
            || message.Contains("not found", StringComparison.OrdinalIgnoreCase)
            || message.Contains("ora-00942", StringComparison.OrdinalIgnoreCase)
            || message.Contains("tabela ou view", StringComparison.OrdinalIgnoreCase)
            || message.Contains("given key was not present", StringComparison.OrdinalIgnoreCase)
            || message.Contains("lock conflict on no wait transaction", StringComparison.OrdinalIgnoreCase)
            || message.Contains("is in use", StringComparison.OrdinalIgnoreCase);
    }
}

