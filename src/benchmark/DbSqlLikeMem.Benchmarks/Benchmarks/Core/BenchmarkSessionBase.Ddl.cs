namespace DbSqlLikeMem.Benchmarks.Core;

public abstract partial class BenchmarkSessionBase
{
    /// <summary>
    /// EN: Creates the benchmark schema through the shared CreateTable service and then removes it.
    /// PT-br: Cria o esquema de benchmark pelo service compartilhado de CreateTable e depois o remove.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.CreateSchema)]
    protected virtual void RunCreateSchema()
    {
        var state = GetPreparedCreateSchemaState();
        state.RunCreateSchema();
    }

    /// <summary>
    /// EN: Creates the benchmark table through the shared CreateTable workflow.
    /// PT-br: Cria a tabela de benchmark pelo fluxo compartilhado de CreateTable.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.CreateTable)]
    protected virtual void RunCreateTable()
    {
        RunCreateSchema();
    }

    /// <summary>
    /// EN: Creates the benchmark users and orders tables with a foreign key and removes them after the run.
    /// PT-br: Cria as tabelas de usuarios e pedidos do benchmark com chave estrangeira e as remove apos a execucao.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.CreateTableWithFK)]
    protected virtual void RunCreateTableWithFK()
    {
        var state = GetPreparedCreateTableWithFkState();
        state.RunCreateTableWithFk();
    }

    /// <summary>
    /// EN: Creates the benchmark foreign-key tables and inserts a referenced row.
    /// PT-br: Cria as tabelas com chave estrangeira do benchmark e insere uma linha referenciada.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.CreateTableWithFKInsert)]
    protected virtual void RunCreateTableWithFKInsert()
    {
        var state = GetPreparedCreateTableWithFkState();
        var count = state.RunCreateTableWithFkInsert(1, 10);
        GC.KeepAlive(count);
    }

    /// <summary>
    /// EN: Executes the insert-in-table-with-FK benchmark and keeps the provider result alive.
    /// PT-br: Executa o benchmark de insert na tabela com chave estrangeira e mantem o resultado do provedor vivo.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.InsertInTableWithFK)]
    protected virtual void RunInsertInTableWithFK()
        => RunCreateTableWithFKInsert();

    /// <summary>
    /// EN: Creates and drops the benchmark users table through the shared DDL drop workflow.
    /// PT-br: Cria e remove a tabela de usuarios do benchmark pelo fluxo compartilhado de remocao DDL.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.DropTable)]
    protected virtual void RunDropTable()
    {
        var state = GetPreparedDropTableState();
        state.RunDropTable();
    }
}
