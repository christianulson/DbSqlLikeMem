namespace DbSqlLikeMem.Benchmarks.Core;

public abstract partial class BenchmarkSessionBase
{
    /// <summary>
    /// EN: Inserts a single user row and validates that the row was persisted.
    /// PT-br: Insere uma única linha de usuário e valida que a linha foi persistida.
    /// </summary>
    /// <exception cref="InvalidOperationException"></exception>
    protected virtual void RunInsertSingle()
    {
        var state = GetPreparedInsertUsersState("InsertSingle");
        var count = state.RunSequentialInsert(1);
        GC.KeepAlive(count);
    }

    /// <summary>
    /// EN: Inserts three rows starting from a custom id and validates the persisted names.
    /// PT-br: Insere tres linhas iniciando em um id customizado e valida os nomes persistidos.
    /// </summary>
    protected virtual void RunInsertCustomStartId()
    {
        var state = GetPreparedInsertUsersState("InsertCustomStartId");
        var result = state.RunInsertCustomStartId();
        GC.KeepAlive(result);
    }

    /// <summary>
    /// EN: Executes the default-columns insert benchmark and keeps the provider result alive.
    /// PT-br: Executa o benchmark de insert com colunas default e mantem o resultado do provedor vivo.
    /// </summary>
    protected virtual void RunInsertDefaultColumns()
    {
        var state = GetPreparedInsertUsersState("InsertDefaultColumns");
        var result = state.RunInsertDefaultColumns();
        GC.KeepAlive(result);
    }

    /// <summary>
    /// EN: Executes the nullable-columns insert benchmark and keeps the provider result alive.
    /// PT-br: Executa o benchmark de insert com colunas anulaveis e mantem o resultado do provedor vivo.
    /// </summary>
    protected virtual void RunInsertNullableColumns()
    {
        var state = GetPreparedInsertUsersState("InsertNullableColumns");
        var result = state.RunInsertNullableColumns();
        GC.KeepAlive(result);
    }

    /// <summary>
    /// EN: Executes the NOT NULL without default insert benchmark and keeps the provider result alive.
    /// PT-br: Executa o benchmark de insert com NOT NULL sem default e mantem o resultado do provedor vivo.
    /// </summary>
    protected virtual void RunInsertNotNullWithoutDefault()
    {
        var state = GetPreparedInsertUsersState("InsertNotNullWithoutDefault");
        var result = state.RunInsertNotNullWithoutDefault();
        GC.KeepAlive(result);
    }

    /// <summary>
    /// EN: Executes the valid CHECK-constraints insert benchmark and keeps the provider result alive.
    /// PT-br: Executa o benchmark de insert valido para restricoes CHECK e mantem o resultado do provedor vivo.
    /// </summary>
    protected virtual void RunCheckConstraintsValidInsert()
    {
        var state = GetPreparedCheckConstraintsState("CheckConstraintsValidInsert");
        var result = state.RunCheckConstraintsValidInsert();
        GC.KeepAlive(result);
    }

    /// <summary>
    /// EN: Executes the invalid CHECK-constraints insert benchmark and keeps the provider result alive.
    /// PT-br: Executa o benchmark de insert invalido para restricoes CHECK e mantem o resultado do provedor vivo.
    /// </summary>
    protected virtual void RunCheckConstraintsInvalidInsert()
    {
        var state = GetPreparedCheckConstraintsState("CheckConstraintsInvalidInsert");
        var result = state.RunCheckConstraintsInvalidInsert();
        GC.KeepAlive(result);
    }

    /// <summary>
    /// EN: Executes the invalid CHECK-constraints update benchmark and keeps the provider result alive.
    /// PT-br: Executa o benchmark de update invalido para restricoes CHECK e mantem o resultado do provedor vivo.
    /// </summary>
    protected virtual void RunCheckConstraintsInvalidUpdate()
    {
        var state = GetPreparedCheckConstraintsState("CheckConstraintsInvalidUpdate");
        var result = state.RunCheckConstraintsInvalidUpdate();
        GC.KeepAlive(result);
    }

    /// <summary>
    /// EN: Inserts ten user rows and keeps the returned count alive.
    /// PT-br: Insere dez linhas de usuario e mantem viva a contagem retornada.
    /// </summary>
    protected virtual void RunInsertBatch10()
    {
        var state = GetPreparedInsertUsersState("InsertBatch10");
        var count = state.RunSequentialInsert(10);
        GC.KeepAlive(count);
    }

    /// <summary>
    /// EN: Inserts one hundred user rows and validates the final row count.
    /// PT-br: Insere cem linhas de usuário e valida a contagem final de linhas.
    /// </summary>
    /// <exception cref="InvalidOperationException"></exception>
    protected virtual void RunInsertBatch100()
    {
        var state = GetPreparedInsertUsersState("InsertBatch100");
        var count = state.RunSequentialInsert(100);
        GC.KeepAlive(count);
    }

    /// <summary>
    /// EN: Inserts one hundred user rows in parallel and validates the final row count.
    /// PT-br: Insere cem linhas de usuário em paralelo e valida a contagem final de linhas.
    /// </summary>
    /// <exception cref="InvalidOperationException"></exception>
    protected virtual void RunInsertBatch100Parallel()
    {
        var state = GetPreparedInsertUsersState("InsertBatch100Parallel");
        var count = state.RunParallelInsert(100);
        GC.KeepAlive(count);
    }
}
