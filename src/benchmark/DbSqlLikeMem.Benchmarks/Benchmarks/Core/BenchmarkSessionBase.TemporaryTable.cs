namespace DbSqlLikeMem.Benchmarks.Core;

public abstract partial class BenchmarkSessionBase
{
    /// <summary>
    /// EN: Executes the temporary-table create-and-use benchmark and keeps the projected rows alive.
    /// PT-br: Executa o benchmark de criar e usar tabela temporaria e mantem as linhas projetadas ativas.
    /// </summary>
    protected virtual void RunTempTableCreateAndUse()
    {
        var state = GetPreparedTemporaryTableSourceState("TempTableSource");
        var rows = state.Service.RunCreateTemporaryTableAsSelectThenSelectAsync().GetAwaiter().GetResult();
        GC.KeepAlive(rows);
    }

    /// <summary>
    /// EN: Executes the temporary-table rollback benchmark and keeps the provider result alive.
    /// PT-br: Executa o benchmark de rollback com tabela temporaria e mantem o resultado do provedor vivo.
    /// </summary>
    protected virtual void RunTempTableRollback()
    {
        var state = GetPreparedTemporaryUsersState("TempUsers");
        state.Service.RunTempTableRollback().GetAwaiter().GetResult();
    }

    /// <summary>
    /// EN: Executes the temporary-table cross-connection isolation benchmark and keeps the result alive.
    /// PT-br: Executa o benchmark de isolamento de tabela temporaria entre conexoes e mantem o resultado ativo.
    /// </summary>
    protected virtual void RunTempTableCrossConnectionIsolation()
    {
        var state = GetPreparedTemporaryUsersState("TempUsersIsolation");
        var value = state.Service.RunTemporaryTableCrossConnectionIsolation().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }
}
