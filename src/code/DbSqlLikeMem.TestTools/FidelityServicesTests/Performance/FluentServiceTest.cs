namespace DbSqlLikeMem.TestTools.Performance;

/// <summary>
/// EN: Executes fluent-style benchmark workflows that do not require database state.
/// PT: Executa fluxos de benchmark no estilo fluent que nao exigem estado de banco de dados.
/// </summary>
public class FluentServiceTest<T>(
    T connection,
    ITestScenario<T> testScenario,
    ProviderSqlDialect dialect
    ) : BaseServiceTest<T>(connection, testScenario, dialect)
    where T : DbConnection
{
    /// <summary>
    /// EN: Builds a representative fluent schema model.
    /// PT: Monta um modelo de schema fluent representativo.
    /// </summary>
    public object RunFluentSchemaBuild()
    {
        var model = BuildFluentSchemaBuild();
        GC.KeepAlive(model);
        return model;
    }

    /// <summary>
    /// EN: Builds a representative fluent schema model without database access.
    /// PT: Monta um modelo de schema fluent representativo sem acesso a banco de dados.
    /// </summary>
    public static object BuildFluentSchemaBuild()
    {
        return new
        {
            Tables = new[]
            {
                new { Name = "Users", Columns = new[] { "Id", "Name" } },
                new { Name = "Orders", Columns = new[] { "Id", "UsersId", "Note" } }
            }
        };
    }

    /// <summary>
    /// EN: Builds a representative fluent seed payload with one hundred rows.
    /// PT: Monta um payload fluent de seed com cem linhas.
    /// </summary>
    public object RunFluentSeed100()
    {
        var rows = BuildFluentSeed100();
        GC.KeepAlive(rows);
        return rows;
    }

    /// <summary>
    /// EN: Builds a representative fluent seed payload with one hundred rows without database access.
    /// PT: Monta um payload fluent de seed com cem linhas sem acesso a banco de dados.
    /// </summary>
    public static object BuildFluentSeed100()
    {
        return Enumerable.Range(1, 100).Select(i => new { Id = i, Name = $"User{i}" }).ToArray();
    }

    /// <summary>
    /// EN: Builds a representative fluent seed payload with one thousand rows.
    /// PT: Monta um payload fluent de seed com mil linhas.
    /// </summary>
    public object RunFluentSeed1000()
    {
        var rows = BuildFluentSeed1000();
        GC.KeepAlive(rows);
        return rows;
    }

    /// <summary>
    /// EN: Builds a representative fluent seed payload with one thousand rows without database access.
    /// PT: Monta um payload fluent de seed com mil linhas sem acesso a banco de dados.
    /// </summary>
    public static object BuildFluentSeed1000()
    {
        return Enumerable.Range(1, 1000).Select(i => new { Id = i, Name = $"User{i}" }).ToArray();
    }

    /// <summary>
    /// EN: Builds a representative fluent scenario composition payload.
    /// PT: Monta um payload representativo de composição de cenário fluent.
    /// </summary>
    public object RunFluentScenarioCompose()
    {
        var scenario = BuildFluentScenarioCompose();
        GC.KeepAlive(scenario);
        return scenario;
    }

    /// <summary>
    /// EN: Builds a representative fluent scenario composition payload without database access.
    /// PT: Monta um payload representativo de composicao de cenario fluent sem acesso a banco de dados.
    /// </summary>
    public static object BuildFluentScenarioCompose()
    {
        return new
        {
            Schema = new[] { "Users", "Orders" },
            Seed = Enumerable.Range(1, 25).Select(i => $"User{i}").ToArray(),
            Query = "SELECT COUNT(*) FROM Users"
        };
    }
}
