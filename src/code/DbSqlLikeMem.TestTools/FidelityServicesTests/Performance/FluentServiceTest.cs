namespace DbSqlLikeMem.TestTools.Performance;

/// <summary>
/// EN: Executes fluent-style benchmark workflows that do not require database state.
/// PT-br: Executa fluxos de benchmark no estilo fluent que nao exigem estado de banco de dados.
/// </summary>
public class FluentServiceTest(
        RepoService repo,
        FidelityTestContext context
    ) : BaseServiceTest(repo, context)
{
    /// <summary>
    /// EN: Builds a representative fluent schema model.
    /// PT-br: Monta um modelo de schema fluent representativo.
    /// </summary>
    /// <param name="args">EN: Unused benchmark arguments kept for signature consistency. PT-br: Argumentos de benchmark nao utilizados, mantidos para consistencia da assinatura.</param>
    public Task<object?> RunFluentSchemaBuildAsync(params object[] args)
    {
        var model = BuildFluentSchemaBuild();
        GC.KeepAlive(model);
        return Task.FromResult<object?>(model);
    }

    /// <summary>
    /// EN: Builds a representative fluent schema model without database access.
    /// PT-br: Monta um modelo de schema fluent representativo sem acesso a banco de dados.
    /// </summary>
    public static object BuildFluentSchemaBuild()
    => new
    {
        Tables = new[]
            {
                new { Name = "Users", Columns = new[] { "Id", "Name" } },
                new { Name = "Orders", Columns = new[] { "Id", "UsersId", "Note" } }
            }
    };

    /// <summary>
    /// EN: Builds a representative fluent seed payload with one hundred rows.
    /// PT-br: Monta um payload fluent de seed com cem linhas.
    /// </summary>
    /// <param name="args">EN: Unused benchmark arguments kept for signature consistency. PT-br: Argumentos de benchmark nao utilizados, mantidos para consistencia da assinatura.</param>
    public Task<object?> RunFluentSeed100Async(params object[] args)
    {
        var rows = BuildFluentSeed100();
        GC.KeepAlive(rows);
        return Task.FromResult<object?>(rows);
    }

    /// <summary>
    /// EN: Builds a representative fluent seed payload with one hundred rows without database access.
    /// PT-br: Monta um payload fluent de seed com cem linhas sem acesso a banco de dados.
    /// </summary>
    public static object BuildFluentSeed100()
    {
        return Enumerable.Range(1, 100).Select(i => new { Id = i, Name = $"User{i}" }).ToArray();
    }

    /// <summary>
    /// EN: Builds a representative fluent seed payload with one thousand rows.
    /// PT-br: Monta um payload fluent de seed com mil linhas.
    /// </summary>
    /// <param name="args">EN: Unused benchmark arguments kept for signature consistency. PT-br: Argumentos de benchmark nao utilizados, mantidos para consistencia da assinatura.</param>
    public Task<object?> RunFluentSeed1000Async(params object[] args)
    {
        var rows = BuildFluentSeed1000();
        GC.KeepAlive(rows);
        return Task.FromResult<object?>(rows);
    }

    /// <summary>
    /// EN: Builds a representative fluent seed payload with one thousand rows without database access.
    /// PT-br: Monta um payload fluent de seed com mil linhas sem acesso a banco de dados.
    /// </summary>
    public static object BuildFluentSeed1000()
    {
        return Enumerable.Range(1, 1000).Select(i => new { Id = i, Name = $"User{i}" }).ToArray();
    }

    /// <summary>
    /// EN: Builds a representative fluent scenario composition payload.
    /// PT-br: Monta um payload representativo de composição de cenário fluent.
    /// </summary>
    /// <param name="args">EN: Unused benchmark arguments kept for signature consistency. PT-br: Argumentos de benchmark nao utilizados, mantidos para consistencia da assinatura.</param>
    public Task<object?> RunFluentScenarioComposeAsync(params object[] args)
    {
        var scenario = BuildFluentScenarioCompose();
        GC.KeepAlive(scenario);
        return Task.FromResult<object?>(scenario);
    }

    /// <summary>
    /// EN: Builds a representative fluent scenario composition payload without database access.
    /// PT-br: Monta um payload representativo de composicao de cenario fluent sem acesso a banco de dados.
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
