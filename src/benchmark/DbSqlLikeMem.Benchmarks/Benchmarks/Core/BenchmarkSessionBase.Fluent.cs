namespace DbSqlLikeMem.Benchmarks.Core;

public abstract partial class BenchmarkSessionBase
{
    /// <summary>
    /// EN: Builds the fluent schema and keeps the generated model alive.
    /// PT: Constrói o schema fluent e mantém o modelo gerado ativo.
    /// </summary>
    protected virtual void RunFluentSchemaBuild()
    {
        var model = FluentServiceTest.BuildFluentSchemaBuild();
        GC.KeepAlive(model);
    }

    /// <summary>
    /// EN: Builds the fluent seed set with 100 rows and keeps the generated rows alive.
    /// PT: Constrói o conjunto fluent com 100 linhas e mantém as linhas geradas ativas.
    /// </summary>
    protected virtual void RunFluentSeed100()
    {
        var rows = FluentServiceTest.BuildFluentSeed100();
        GC.KeepAlive(rows);
    }

    /// <summary>
    /// EN: Builds the fluent seed set with 1000 rows and keeps the generated rows alive.
    /// PT: Constrói o conjunto fluent com 1000 linhas e mantém as linhas geradas ativas.
    /// </summary>
    protected virtual void RunFluentSeed1000()
    {
        var rows = FluentServiceTest.BuildFluentSeed1000();
        GC.KeepAlive(rows);
    }

    /// <summary>
    /// EN: Builds the fluent compose scenario and keeps the generated scenario alive.
    /// PT: Constrói o cenario compose fluent e mantém o cenario gerado ativo.
    /// </summary>
    protected virtual void RunFluentScenarioCompose()
    {
        var scenario = FluentServiceTest.BuildFluentScenarioCompose();
        GC.KeepAlive(scenario);
    }
}
