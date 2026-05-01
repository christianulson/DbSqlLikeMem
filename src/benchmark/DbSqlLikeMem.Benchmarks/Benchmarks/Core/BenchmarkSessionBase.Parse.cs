using DbSqlLikeMem.TestTools.Query;

namespace DbSqlLikeMem.Benchmarks.Core;

public abstract partial class BenchmarkSessionBase
{
    /// <summary>
    /// EN: Executes the simple SELECT parser benchmark and keeps the tokens alive.
    /// PT-br: Executa o benchmark do parser para um SELECT simples e mantem os tokens vivos.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.ParseSimpleSelect)]
    protected virtual void RunParseSimpleSelect()
    {
        var tokens = ParseServiceTest.RunParseSimpleSelect();
        GC.KeepAlive(tokens);
    }

    /// <summary>
    /// EN: Executes the complex join parser benchmark and keeps the tokens alive.
    /// PT-br: Executa o benchmark do parser para um join complexo e mantem os tokens vivos.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.ParseComplexJoin)]
    protected virtual void RunParseComplexJoin()
    {
        var tokens = ParseServiceTest.RunParseComplexJoin();
        GC.KeepAlive(tokens);
    }

    /// <summary>
    /// EN: Executes the INSERT RETURNING parser benchmark and keeps the tokens alive.
    /// PT-br: Executa o benchmark do parser para INSERT RETURNING e mantem os tokens vivos.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.ParseInsertReturning)]
    protected virtual void RunParseInsertReturning()
    {
        var tokens = ParseServiceTest.RunParseInsertReturning();
        GC.KeepAlive(tokens);
    }

    /// <summary>
    /// EN: Executes the ON CONFLICT DO UPDATE parser benchmark and keeps the tokens alive.
    /// PT-br: Executa o benchmark do parser para ON CONFLICT DO UPDATE e mantem os tokens vivos.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.ParseOnConflictDoUpdate)]
    protected virtual void RunParseOnConflictDoUpdate()
    {
        var tokens = ParseServiceTest.RunParseOnConflictDoUpdate();
        GC.KeepAlive(tokens);
    }

    /// <summary>
    /// EN: Executes the JSON extract parser benchmark and keeps the tokens alive.
    /// PT-br: Executa o benchmark do parser para extracao JSON e mantem os tokens vivos.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.ParseJsonExtract)]
    protected virtual void RunParseJsonExtract()
    {
        var tokens = ParseServiceTest.RunParseJsonExtract();
        GC.KeepAlive(tokens);
    }

    /// <summary>
    /// EN: Executes the string-aggregate WITHIN GROUP parser benchmark and keeps the tokens alive.
    /// PT-br: Executa o benchmark do parser para string aggregate WITHIN GROUP e mantem os tokens vivos.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.ParseStringAggregateWithinGroup)]
    protected virtual void RunParseStringAggregateWithinGroup()
    {
        var tokens = ParseServiceTest.RunParseStringAggregateWithinGroup();
        GC.KeepAlive(tokens);
    }

    /// <summary>
    /// EN: Executes the auto-dialect TOP/LIMIT/FETCH parser benchmark and keeps the tokens alive.
    /// PT-br: Executa o benchmark do parser para TOP/LIMIT/FETCH com autodialeto e mantem os tokens vivos.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.ParseAutoDialectTopLimitFetch)]
    protected virtual void RunParseAutoDialectTopLimitFetch()
    {
        var tokens = ParseServiceTest.RunParseAutoDialectTopLimitFetch();
        GC.KeepAlive(tokens);
    }

    /// <summary>
    /// EN: Executes the multi-statement batch parser benchmark and keeps the tokens alive.
    /// PT-br: Executa o benchmark do parser para lote com multiplas instrucoes e mantem os tokens vivos.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.ParseMultiStatementBatch)]
    protected virtual void RunParseMultiStatementBatch()
    {
        var tokens = ParseServiceTest.RunParseMultiStatementBatch();
        GC.KeepAlive(tokens);
    }
}
