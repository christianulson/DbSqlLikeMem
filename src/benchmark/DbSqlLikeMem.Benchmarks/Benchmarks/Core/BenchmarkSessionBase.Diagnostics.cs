using DbSqlLikeMem.TestTools.Performance;

namespace DbSqlLikeMem.Benchmarks.Core;

public abstract partial class BenchmarkSessionBase
{
    /// <summary>
    /// EN: Executes the execution-plan benchmark and keeps the generated plan alive.
    /// PT-br: Executa o benchmark de execution plan e mantem o plano gerado ativo.
    /// </summary>
    protected virtual void RunExecutionPlan()
    {
        (int id, string name)[] seedRows = [(1, "Alice")];
        var state = GetPreparedExecutionPlanState("ExecutionPlan", seedRows);
        var plan = state.Service.RunTestAsync(seedRows[0].id).GetAwaiter().GetResult();
        GC.KeepAlive(plan);
    }

    /// <summary>
    /// EN: Executes the execution-plan benchmark for SELECT flows.
    /// PT-br: Executa o benchmark de execution plan para fluxos SELECT.
    /// </summary>
    protected virtual void RunExecutionPlanSelect()
    {
        RunExecutionPlan();
    }

    /// <summary>
    /// EN: Executes the execution-plan benchmark for join flows and keeps the generated plan alive.
    /// PT-br: Executa o benchmark de execution plan para fluxos de join e mantem o plano gerado ativo.
    /// </summary>
    protected virtual void RunExecutionPlanJoin()
    {
        (int id, string name)[] seedUsers = [(1, "Alice")];
        (int id, int userId, string order)[] seedOrders = [(1, 1, "order-1")];
        var state = GetPreparedExecutionPlanJoinState(
            "ExecutionPlanJoin",
            seedUsers,
            seedOrders);
        var plan = state.Service.RunTestAsync(seedUsers[0].id).GetAwaiter().GetResult();
        GC.KeepAlive(plan);
    }

    /// <summary>
    /// EN: Executes the execution-plan benchmark for DML flows and keeps the generated plan alive.
    /// PT-br: Executa o benchmark de execution plan para fluxos DML e mantem o plano gerado ativo.
    /// </summary>
    protected virtual void RunExecutionPlanDml()
    {
        var state = GetPreparedExecutionPlanDmlState("ExecutionPlanDml");
        var plan = state.RunExecutionPlanDml();
        GC.KeepAlive(plan);
    }

    /// <summary>
    /// EN: Executes the debug-trace benchmark for SELECT flows and keeps the generated trace alive.
    /// PT-br: Executa o benchmark de debug trace para fluxos SELECT e mantem o trace gerado ativo.
    /// </summary>
    protected virtual void RunDebugTraceSelect()
    {
        (int id, string name)[] seedRows = [(1, "Alice")];
        var state = GetPreparedDebugTraceSelectState("DebugTraceSelect", seedRows);
        var trace = state.Service.RunTestAsync(seedRows[0].id).GetAwaiter().GetResult();
        GC.KeepAlive(trace);
    }

    /// <summary>
    /// EN: Executes the debug-trace benchmark for batch flows and keeps the generated trace alive.
    /// PT-br: Executa o benchmark de debug trace para fluxos em lote e mantem o trace gerado ativo.
    /// </summary>
    protected virtual void RunDebugTraceBatch()
    {
        var state = GetPreparedDebugTraceBatchState("DebugTraceBatch");
        var trace = state.Service.RunTestAsync(2, 3).GetAwaiter().GetResult();
        GC.KeepAlive(trace);
    }

    /// <summary>
    /// EN: Executes the debug-trace JSON benchmark and keeps the generated payload alive.
    /// PT-br: Executa o benchmark de debug trace em JSON e mantem o payload gerado ativo.
    /// </summary>
    protected virtual void RunDebugTraceJson()
    {
        var json = DebugTraceJsonServiceTest.RunDebugTraceJson(Dialect.DisplayName, Engine.ToString());
        GC.KeepAlive(json);
    }

    /// <summary>
    /// EN: Executes the last-execution-plans history benchmark and keeps the collected plans alive.
    /// PT-br: Executa o benchmark de historico de ultimos planos de execucao e mantem os planos coletados ativos.
    /// </summary>
    protected virtual void RunLastExecutionPlansHistory()
    {
        (int id, string name)[] seedRows = [(1, "Alice")];
        var state = GetPreparedLastExecutionPlansHistoryState("LastExecutionPlansHistory", seedRows);
        var plans = state.Service.RunTestAsync(seedRows[0].id).GetAwaiter().GetResult();
        GC.KeepAlive(plans);
    }
}
