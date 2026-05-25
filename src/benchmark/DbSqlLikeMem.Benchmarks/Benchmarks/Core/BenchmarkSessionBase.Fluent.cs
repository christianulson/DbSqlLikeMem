using DbSqlLikeMem.TestTools.Performance;

namespace DbSqlLikeMem.Benchmarks.Core;

public abstract partial class BenchmarkSessionBase
{
    /// <summary>
    /// EN: Builds the fluent schema and keeps the generated model alive.
    /// PT-br: Constrói o schema fluent e mantém o modelo gerado ativo.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.FluentSchemaBuild)]
    protected virtual void RunFluentSchemaBuild()
    {
        var model = FluentServiceTest.BuildFluentSchemaBuild();
        GC.KeepAlive(model);
    }

    /// <summary>
    /// EN: Builds the fluent seed set with 100 rows and keeps the generated rows alive.
    /// PT-br: Constrói o conjunto fluent com 100 linhas e mantém as linhas geradas ativas.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.FluentSeed100)]
    protected virtual void RunFluentSeed100()
    {
        var rows = FluentServiceTest.BuildFluentSeed100();
        GC.KeepAlive(rows);
    }

    /// <summary>
    /// EN: Builds the fluent seed set with 1000 rows and keeps the generated rows alive.
    /// PT-br: Constrói o conjunto fluent com 1000 linhas e mantém as linhas geradas ativas.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.FluentSeed1000)]
    protected virtual void RunFluentSeed1000()
    {
        var rows = FluentServiceTest.BuildFluentSeed1000();
        GC.KeepAlive(rows);
    }

    /// <summary>
    /// EN: Builds the fluent compose scenario and keeps the generated scenario alive.
    /// PT-br: Constrói o cenario compose fluent e mantém o cenario gerado ativo.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.FluentScenarioCompose)]
    protected virtual void RunFluentScenarioCompose()
    {
        var scenario = FluentServiceTest.BuildFluentScenarioCompose();
        GC.KeepAlive(scenario);
    }
}
