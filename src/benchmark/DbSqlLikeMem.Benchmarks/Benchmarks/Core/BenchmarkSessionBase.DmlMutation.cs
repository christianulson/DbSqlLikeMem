namespace DbSqlLikeMem.Benchmarks.Core;

public abstract partial class BenchmarkSessionBase
{
    /// <summary>
    /// EN: Updates a user row by primary key and validates the stored value.
    /// PT-br: Atualiza uma linha de usuário pela chave primária e valida o valor armazenado.
    /// </summary>
    /// <exception cref="InvalidOperationException"></exception>
    [BenchmarkFeature(BenchmarkFeatureId.UpdateByPk)]
    protected virtual void RunUpdateByPk()
    {
        var state = GetPreparedCrudUsersState("CrudUsers");
        var value = state.RunUpdateByPk(1);
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Deletes a user row by primary key and validates the remaining row count.
    /// PT-br: Exclui uma linha de usuário pela chave primária e valida a contagem de linhas restante.
    /// </summary>
    /// <exception cref="InvalidOperationException"></exception>
    [BenchmarkFeature(BenchmarkFeatureId.DeleteByPk)]
    protected virtual void RunDeleteByPk()
    {
        var state = GetPreparedCrudUsersState("CrudUsers");
        var count = state.RunDeleteByPk(1);
        GC.KeepAlive(count);
    }

    /// <summary>
    /// EN: Executes an update/delete round trip and validates the remaining row count.
    /// PT-br: Executa um ciclo de update/delete e valida a contagem de linhas restante.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.UpdateDeleteRoundTrip)]
    protected virtual void RunUpdateDeleteRoundTrip()
    {
        var state = GetPreparedCrudUsersState("UpdateDeleteRoundTrip");
        var count = state.RunUpdateDeleteRoundTrip(1, 2);
        GC.KeepAlive(count);
    }

    /// <summary>
    /// EN: Executes the parameter update/delete round-trip benchmark and keeps the provider result alive.
    /// PT-br: Executa o benchmark de roundtrip de update/delete com parametros e mantem o resultado do provedor vivo.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.ParameterUpdateDeleteRoundTrip)]
    protected virtual void RunParameterUpdateDeleteRoundTrip()
        => RunUpdateDeleteRoundTrip();

    /// <summary>
    /// EN: Executes an insert inside a transaction, commits it, and validates the committed result.
    /// PT-br: Executa uma inserção dentro de uma transação, confirma a operação e valida o resultado confirmado.
    /// </summary>
    /// <exception cref="InvalidOperationException"></exception>
    [BenchmarkFeature(BenchmarkFeatureId.TransactionCommit)]
    protected virtual void RunTransactionCommit()
    {
        var state = GetPreparedTransactionUsersState("TransactionUsers");
        var count = state.RunTransactionCommit();
        GC.KeepAlive(count);
    }

    /// <summary>
    /// EN: Executes an insert inside a transaction, rolls it back, and validates that no rows remain.
    /// PT-br: Executa uma inserção dentro de uma transação, desfaz a operação e valida que nenhuma linha permaneceu.
    /// </summary>
    /// <exception cref="InvalidOperationException"></exception>
    [BenchmarkFeature(BenchmarkFeatureId.TransactionRollback)]
    protected virtual void RunTransactionRollback()
    {
        var state = GetPreparedTransactionUsersState("TransactionUsers");
        var count = state.RunTransactionRollback();
        GC.KeepAlive(count);
    }

    /// <summary>
    /// EN: Executes an update/delete workflow inside a transaction and validates the committed result.
    /// PT-br: Executa um fluxo de update/delete dentro de uma transação e valida o resultado confirmado.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.TransactionalUpdateDeleteCommit)]
    protected virtual void RunTransactionalUpdateDeleteCommit()
    {
        var state = GetPreparedCrudUsersState("TransactionalUpdateDeleteCommit");
        var count = state.RunTransactionalUpdateDeleteCommit(1, 2);
        GC.KeepAlive(count);
    }

    /// <summary>
    /// EN: Executes the row-count-after-update benchmark and keeps the count alive.
    /// PT-br: Executa o benchmark de contagem de linhas apos update e mantem a contagem viva.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.RowCountAfterUpdate)]
    protected virtual void RunRowCountAfterUpdate()
    {
        var state = GetPreparedCrudUsersState("CrudUsers");
        var affected = state.RunRowCountAfterUpdate();
        GC.KeepAlive(affected);
    }

    /// <summary>
    /// EN: Executes the savepoint creation benchmark.
    /// PT-br: Executa o benchmark de criacao de savepoint.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.SavepointCreate)]
    protected virtual void RunSavepointCreate()
    {
        var state = GetPreparedNoopMutationState("NoopMutation");
        state.Service.RunSavepointCreate();
    }

    /// <summary>
    /// EN: Executes the rollback-to-savepoint benchmark and keeps the provider result alive.
    /// PT-br: Executa o benchmark de rollback ate o savepoint e mantem o resultado do provedor vivo.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.RollbackToSavepoint)]
    protected virtual void RunRollbackToSavepoint()
    {
        var state = GetPreparedTransactionUsersState("TransactionUsers");
        var count = state.RunRollbackToSavepoint();
        GC.KeepAlive(count);
    }

    /// <summary>
    /// EN: Executes the savepoint release benchmark when the provider supports it.
    /// PT-br: Executa o benchmark de liberacao de savepoint quando o provedor oferece suporte.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.ReleaseSavepoint)]
    protected virtual void RunReleaseSavepoint()
    {
        if (!Dialect.SupportsReleaseSavepoints)
        {
            return;
        }

        var state = GetPreparedNoopMutationState("NoopMutation");
        state.Service.RunReleaseSavepoint();
    }

    /// <summary>
    /// EN: Executes the nested savepoint flow benchmark and keeps the result alive.
    /// PT-br: Executa o benchmark de fluxo aninhado de savepoint e mantem o resultado vivo.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.NestedSavepointFlow)]
    protected virtual void RunNestedSavepointFlow()
    {
        var state = GetPreparedTransactionUsersState("TransactionUsers");
        var count = state.RunNestedSavepointFlow();
        GC.KeepAlive(count);
    }

    /// <summary>
    /// EN: Executes the provider-specific upsert path and validates the updated value.
    /// PT-br: Executa o caminho de upsert específico do provedor e valida o valor atualizado.
    /// </summary>
    /// <exception cref="NotSupportedException"></exception>
    /// <exception cref="InvalidOperationException"></exception>
    [BenchmarkFeature(BenchmarkFeatureId.Upsert)]
    protected virtual void RunUpsert()
    {
        var state = GetPreparedCrudUsersState("CrudUsers");
        var value = state.RunUpsert(1);
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the merge insert-then-update benchmark and keeps the provider result alive.
    /// PT-br: Executa o benchmark de merge de inserir e depois atualizar e mantem o resultado do provedor vivo.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.MergeInsertThenUpdate)]
    protected virtual void RunMergeInsertThenUpdate()
    {
        if (!Dialect.SupportsMerge)
        {
            return;
        }

        var state = GetPreparedMergeUsersState("MergeInsertThenUpdate");
        var value = state.RunMergeInsertThenUpdate();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the upsert insert-then-update benchmark and keeps the provider result alive.
    /// PT-br: Executa o benchmark de upsert de inserir e depois atualizar e mantem o resultado do provedor vivo.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.UpsertInsertThenUpdate)]
    protected virtual void RunUpsertInsertThenUpdate()
    {
        if (!Dialect.SupportsUpsert)
        {
            return;
        }

        var state = GetPreparedMergeUsersState("UpsertInsertThenUpdate");
        var value = state.RunUpsertInsertThenUpdate();
        GC.KeepAlive(value);
    }
}
