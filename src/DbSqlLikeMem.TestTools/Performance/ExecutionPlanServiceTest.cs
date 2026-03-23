namespace DbSqlLikeMem.TestTools.Performance;

/// <summary>
/// EN: Executes execution-plan benchmark workflows and validates the observed provider diagnostics.
/// PT: Executa fluxos de benchmark de plano de execucao e valida os diagnosticos observados do provedor.
/// </summary>
public class ExecutionPlanServiceTest<T>(
    T connection,
    ITestScenario<T> testScenario,
    ProviderSqlDialect dialect
    ) : PerformanceServiceBase<T>(connection, testScenario, dialect)
    where T : DbConnection
{
    /// <summary>
    /// EN: Executes a select and reads the provider execution plan diagnostic.
    /// PT: Executa um select e lê o diagnostico de plano de execucao do provedor.
    /// </summary>
    public object? RunExecutionPlan(params object[] pars)
    {
        var users = (string)pars[0];
        _ = ExecuteScalar(Dialect.SelectUserNameById(users, 1));
        var plan = TryReadDiagnosticValue(Connection, "LastExecutionPlan");
        GC.KeepAlive(plan);
        return plan;
    }

    /// <summary>
    /// EN: Executes the select execution-plan benchmark alias.
    /// PT: Executa o alias do benchmark de plano de execucao para select.
    /// </summary>
    public object? RunExecutionPlanSelect(params object[] pars)
        => RunExecutionPlan(pars);

    /// <summary>
    /// EN: Executes a join and reads the provider execution plan diagnostic.
    /// PT: Executa uma junção e lê o diagnostico de plano de execucao do provedor.
    /// </summary>
    public object? RunExecutionPlanJoin(params object[] pars)
    {
        var users = (string)pars[0];
        var orders = (string)pars[1];
        _ = ExecuteScalar(Dialect.CountJoinForUser(users, orders, 1));
        var plan = TryReadDiagnosticValue(Connection, "LastExecutionPlan");
        GC.KeepAlive(plan);
        return plan;
    }

    /// <summary>
    /// EN: Executes an insert and reads the provider execution plan diagnostic.
    /// PT: Executa um insert e lê o diagnostico de plano de execucao do provedor.
    /// </summary>
    public object? RunExecutionPlanDml(params object[] pars)
    {
        var users = (string)pars[0];
        ExecuteNonQuery(Dialect.InsertUser(users, 1, "Alice"));
        var plan = TryReadDiagnosticValue(Connection, "LastExecutionPlan");
        GC.KeepAlive(plan);
        return plan;
    }

    /// <summary>
    /// EN: Executes several statements and reads the provider execution-plan history.
    /// PT: Executa varias instrucoes e lê o historico de planos de execucao do provedor.
    /// </summary>
    public object? RunLastExecutionPlansHistory(params object[] pars)
    {
        var users = (string)pars[0];
        _ = ExecuteScalar(Dialect.SelectUserNameById(users, 1));
        _ = ExecuteScalar(Dialect.CountRows(users));
        var plans = TryReadDiagnosticValue(Connection, "LastExecutionPlans");
        GC.KeepAlive(plans);
        return plans;
    }
}
