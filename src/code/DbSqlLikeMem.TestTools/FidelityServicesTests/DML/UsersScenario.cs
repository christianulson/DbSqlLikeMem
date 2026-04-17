using System.Collections;

namespace DbSqlLikeMem.TestTools.DML;

/// <summary>
/// EN: Creates and drops a users table with optional seed rows for DML mutation workflows.
/// PT: Cria e remove uma tabela de usuarios com linhas iniciais opcionais para fluxos de mutacao DML.
/// </summary>
public sealed class UsersScenario(
        RepoService repo,
        FidelityTestContext context,
        params (int id, string name)[] seedRows
    ) : BaseScenario(repo, context),
        ITestScenario
{
    /// <summary>
    /// EN: Creates the users table scenario from generic seed rows supplied by reflection-based test helpers.
    /// PT: Cria o cenario da tabela de usuarios a partir de linhas iniciais genericas fornecidas por helpers de teste baseados em reflexao.
    /// </summary>
    /// <param name="repo">EN: The repository used by the scenario. PT: O repositório usado pelo cenário.</param>
    /// <param name="context">EN: The fidelity context used by the scenario. PT: O contexto de fidelidade usado pelo cenário.</param>
    /// <param name="seedRows">EN: The seed rows to convert into typed user tuples. PT: As linhas iniciais a converter em tuplas tipadas de usuario.</param>
    public UsersScenario(
        RepoService repo,
        FidelityTestContext context,
        params object?[][] seedRows
    ) : this(repo, context, ConvertSeedRows(seedRows))
    {
    }

    /// <summary>
    /// EN: Creates the users table and seeds the configured rows.
    /// PT: Cria a tabela de usuarios e preenche as linhas configuradas.
    /// </summary>
    public async Task CreateScenarioAsync()
    {

        await Repo.ExecuteNonQueryAsync(Repo.Dialect.CreateUsersTable(Context));
        foreach (var (id, name) in seedRows)
        {
            await Repo.ExecuteNonQueryAsync(Repo.Dialect.InsertUser(Context, id, name));
        }
    }

    /// <summary>
    /// EN: Drops the users table created for the workflow.
    /// PT: Remove a tabela de usuarios criada para o fluxo.
    /// </summary>
    public Task DropScenarioAsync()
    => Repo.ExecuteNonQueryAsync(Repo.Dialect.DropTable(Context.TbUsersFullName));

    private static (int id, string name)[] ConvertSeedRows(object?[][] seedRows)
    {
        var rows = new List<(int id, string name)>();
        foreach (var seedRow in seedRows)
        {
            AddSeedRow(seedRow, rows);
        }

        return rows.ToArray();
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
}

