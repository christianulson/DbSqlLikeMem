namespace DbSqlLikeMem.Benchmarks.Core;

public abstract partial class BenchmarkSessionBase
{
    /// <summary>
    /// EN: Executes a parameter binding benchmark using a scalar projection query.
    /// PT-br: Executa um benchmark de binding de parametros usando uma consulta de projeção escalar.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.ParameterProjection)]
    protected virtual void RunParameterProjection()
    {
        var state = GetPreparedParameterProjectionState("ParameterProjection");
        var value = state.RunParameterProjection();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes a parameter binding benchmark using a single-row INSERT statement.
    /// PT-br: Executa um benchmark de binding de parametros usando uma instrucao INSERT de uma linha.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.ParameterInsertSingle)]
    protected virtual void RunParameterInsertSingle()
    {
        var state = GetPreparedInsertUsersState("ParameterInsertSingle");
        var count = state.RunParameterInsertSingle();
        GC.KeepAlive(count);
    }

    /// <summary>
    /// EN: Executes the parameter insert round-trip benchmark and keeps the provider result alive.
    /// PT-br: Executa o benchmark de roundtrip de insert com parametros e mantem o resultado do provedor vivo.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.ParameterInsertRoundTrip)]
    protected virtual void RunParameterInsertRoundTrip()
    {
        var state = GetPreparedParameterInsertUsersState("ParameterInsertRoundTrip");
        var count = state.RunParameterInsertRoundTrip();
        GC.KeepAlive(count);
    }

    /// <summary>
    /// EN: Executes the parameter insert round-trip benchmark with null values and keeps the provider result alive.
    /// PT-br: Executa o benchmark de roundtrip de insert com parametros e valores nulos e mantem o resultado do provedor vivo.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.ParameterInsertNullRoundTrip)]
    protected virtual void RunParameterInsertNullRoundTrip()
    {
        var state = GetPreparedParameterInsertUsersState("ParameterInsertNullRoundTrip");
        var count = state.RunParameterInsertNullRoundTrip();
        GC.KeepAlive(count);
    }

    /// <summary>
    /// EN: Executes a parameterized lookup by name benchmark.
    /// PT-br: Executa um benchmark de consulta parametrizada por nome.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.ParameterSelectByNameMatrix)]
    protected virtual void RunParameterSelectByNameMatrix()
    {
        var state = GetPreparedUsersQueryState(
            "ParameterSelectByNameMatrix",
            (1, "Alice"),
            (2, "Bob"),
            (3, "Charlie"),
            (4, "Delta"),
            (5, "Echo"));
        var value = state.Service.RunParameterSelectByNameMatrixAsync("Bob").GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes a parameterized lookup by id benchmark.
    /// PT-br: Executa um benchmark de consulta parametrizada por id.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.ParameterSelectByIdMatrix)]
    protected virtual void RunParameterSelectByIdMatrix()
    {
        var state = GetPreparedUsersQueryState(
            "ParameterSelectByIdMatrix",
            (1, "Alice"),
            (2, "Bob"),
            (3, "Charlie"),
            (4, "Delta"),
            (5, "Echo"));
        var value = state.Service.RunParameterSelectByIdMatrixAsync(2, "Bob").GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes a typed parameter round-trip benchmark.
    /// PT-br: Executa um benchmark de roundtrip de parametros tipados.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.ParameterRoundTripMatrix)]
    protected virtual void RunParameterRoundTripMatrix()
    {
        var state = GetPreparedTypedFieldStorageMatrixState("ParameterRoundTripMatrix");
        var value = state.RunParameterRoundTripMatrix();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes a typed parameter projection benchmark.
    /// PT-br: Executa um benchmark de projeção de parametros tipados.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.ParameterTypeMatrix)]
    protected virtual void RunParameterTypeMatrix()
    {
        var state = GetPreparedParameterMatrixState("ParameterTypeMatrix");
        var value = state.RunParameterTypeMatrix();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes a typed date and currency parameter benchmark.
    /// PT-br: Executa um benchmark de data e moeda com parametros tipados.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.ParameterDateCurrencyMatrix)]
    protected virtual void RunParameterDateCurrencyMatrix()
    {
        var state = GetPreparedParameterMatrixState("ParameterDateCurrencyMatrix");
        var value = state.RunParameterDateCurrencyMatrix();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the typed field storage matrix benchmark and keeps the validated snapshot alive.
    /// PT-br: Executa o benchmark da matriz de armazenamento tipado e mantem o snapshot validado ativo.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.TypedFieldStorageMatrix)]
    protected virtual void RunTypedFieldStorageMatrix()
    {
        var state = GetPreparedTypedFieldStorageMatrixState("TypedFieldStorageMatrix");
        var snapshot = state.RunTypedFieldStorageMatrix();
        GC.KeepAlive(snapshot);
    }

    /// <summary>
    /// EN: Executes the typed field function matrix benchmark and keeps the validated snapshot alive.
    /// PT-br: Executa o benchmark da matriz de funcoes tipadas e mantem o snapshot validado ativo.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.TypedFieldFunctionMatrix)]
    protected virtual void RunTypedFieldFunctionMatrix()
    {
        var state = GetPreparedTypedFieldStorageMatrixState("TypedFieldFunctionMatrix");
        var snapshot = state.RunTypedFieldFunctionMatrix();
        GC.KeepAlive(snapshot);
    }

    /// <summary>
    /// EN: Executes the typed field calculation matrix benchmark and keeps the validated snapshot alive.
    /// PT-br: Executa o benchmark da matriz de calculo tipado e mantem o snapshot validado ativo.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.TypedFieldCalculationMatrix)]
    protected virtual void RunTypedFieldCalculationMatrix()
    {
        var state = GetPreparedTypedFieldStorageMatrixState("TypedFieldCalculationMatrix");
        var snapshot = state.RunTypedFieldCalculationMatrix();
        GC.KeepAlive(snapshot);
    }

    /// <summary>
    /// EN: Executes the typed field and function blend benchmark and keeps the validated count alive.
    /// PT-br: Executa o benchmark de mistura de campos tipados e funcoes e mantem a contagem validada ativa.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.TypedFieldAndFunctionBlend)]
    protected virtual void RunTypedFieldAndFunctionBlend()
    {
        var state = GetPreparedTypedFieldStorageMatrixState("TypedFieldAndFunctionBlend");
        var value = state.RunTypedFieldAndFunctionBlend();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the typed field compound predicate matrix benchmark and keeps the validated count alive.
    /// PT-br: Executa o benchmark da matriz de predicados compostos com campos tipados e mantem a contagem validada ativa.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.TypedFieldCompoundPredicateMatrix)]
    protected virtual void RunTypedFieldCompoundPredicateMatrix()
    {
        var state = GetPreparedTypedFieldStorageMatrixState("TypedFieldCompoundPredicateMatrix");
        var value = state.RunTypedFieldCompoundPredicateMatrix();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the typed field cast calculation matrix benchmark and keeps the validated snapshot alive.
    /// PT-br: Executa o benchmark da matriz de calculo com casts em campos tipados e mantem o snapshot validado ativo.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.TypedFieldCastCalculationMatrix)]
    protected virtual void RunTypedFieldCastCalculationMatrix()
    {
        var state = GetPreparedTypedFieldStorageMatrixState("TypedFieldCastCalculationMatrix");
        var snapshot = state.RunCastCalculationMatrix();
        GC.KeepAlive(snapshot);
    }

    /// <summary>
    /// EN: Executes the typed field null comparison matrix benchmark and keeps the validated snapshot alive.
    /// PT-br: Executa o benchmark da matriz de comparacao com null em campos tipados e mantem o snapshot validado ativo.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.TypedFieldNullComparisonMatrix)]
    protected virtual void RunTypedFieldNullComparisonMatrix()
    {
        var state = GetPreparedTypedFieldStorageMatrixState("TypedFieldNullComparisonMatrix");
        var snapshot = state.RunNullComparisonMatrix();
        GC.KeepAlive(snapshot);
    }

    /// <summary>
    /// EN: Executes the typed field text length matrix benchmark and keeps the validated snapshot alive.
    /// PT-br: Executa o benchmark da matriz de comprimento de texto em campos tipados e mantem o snapshot validado ativo.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.TypedFieldTextLengthMatrix)]
    protected virtual void RunTypedFieldTextLengthMatrix()
    {
        var state = GetPreparedTypedFieldStorageMatrixState("TypedFieldTextLengthMatrix");
        var snapshot = state.RunTextLengthMatrix();
        GC.KeepAlive(snapshot);
    }

    /// <summary>
    /// EN: Executes the typed field text case matrix benchmark and keeps the validated snapshot alive.
    /// PT-br: Executa o benchmark da matriz de caixa de texto em campos tipados e mantem o snapshot validado ativo.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.TypedFieldTextCaseMatrix)]
    protected virtual void RunTypedFieldTextCaseMatrix()
    {
        var state = GetPreparedTypedFieldStorageMatrixState("TypedFieldTextCaseMatrix");
        var snapshot = state.RunTextCaseMatrix();
        GC.KeepAlive(snapshot);
    }

    /// <summary>
    /// EN: Executes the typed field predicate matrix benchmark and keeps the validated snapshot alive.
    /// PT-br: Executa o benchmark da matriz de predicados em campos tipados e mantem o snapshot validado ativo.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.TypedFieldPredicateMatrix)]
    protected virtual void RunTypedFieldPredicateMatrix()
    {
        var state = GetPreparedTypedFieldStorageMatrixState("TypedFieldPredicateMatrix");
        var snapshot = state.RunTypedFieldPredicateMatrix();
        GC.KeepAlive(snapshot);
    }

    /// <summary>
    /// EN: Executes typed parameter inserts inside a committed transaction and validates the persisted rows.
    /// PT-br: Executa inserts tipados com parametros dentro de uma transação confirmada e valida as linhas persistidas.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.ParameterTransactionCommit)]
    protected virtual void RunParameterTransactionCommit()
    {
        var state = GetPreparedParameterTransactionUsersState("ParameterTransactionCommit");
        var count = state.RunParameterTransactionCommit();
        GC.KeepAlive(count);
    }

    /// <summary>
    /// EN: Executes typed parameter inserts inside a rolled-back transaction and validates that no rows remain.
    /// PT-br: Executa inserts tipados com parametros dentro de uma transação revertida e valida que nenhuma linha permaneceu.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.ParameterTransactionRollback)]
    protected virtual void RunParameterTransactionRollback()
    {
        var state = GetPreparedParameterTransactionUsersState("ParameterTransactionRollback");
        var count = state.RunParameterTransactionRollback();
        GC.KeepAlive(count);
    }
}
