namespace DbSqlLikeMem.Benchmarks.Core;

public abstract partial class BenchmarkSessionBase
{
    /// <summary>
    /// EN: Executes the shared math functions benchmark and keeps the provider result alive.
    /// PT-br: Executa o benchmark compartilhado de funcoes matematicas e mantem o resultado do provedor vivo.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.MathFunctions)]
    protected virtual void RunMathFunctions()
    {
        if (!Dialect.SupportsMathFunctions)
        {
            return;
        }

        var state = GetPreparedNoopQueryState("NoopQuery");
        var value = state.Service.RunMathFunctionsAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the shared explicit-base math LOG benchmark and keeps the provider result alive.
    /// PT-br: Executa o benchmark compartilhado de logaritmo com base explicita e mantem o resultado do provedor vivo.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.MathLogBaseFunction)]
    protected virtual void RunMathLogBaseFunction()
    {
        if (!Dialect.SupportsMathLogBaseFunction)
        {
            return;
        }

        var state = GetPreparedNoopQueryState("NoopQuery");
        var value = state.Service.RunMathLogBaseFunctionAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the shared LOG2 benchmark and keeps the provider result alive.
    /// PT-br: Executa o benchmark compartilhado de LOG2 e mantem o resultado do provedor vivo.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.MathLog2Function)]
    protected virtual void RunMathLog2Function()
    {
        if (!Dialect.SupportsMathLog2Function)
        {
            return;
        }

        var state = GetPreparedNoopQueryState("NoopQuery");
        var value = state.Service.RunMathLog2FunctionAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the shared PI benchmark and keeps the provider result alive.
    /// PT-br: Executa o benchmark compartilhado de PI e mantem o resultado do provedor vivo.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.MathPiFunction)]
    protected virtual void RunMathPiFunction()
    {
        if (!Dialect.SupportsMathPiFunction)
        {
            return;
        }

        var state = GetPreparedNoopQueryState("NoopQuery");
        var value = state.Service.RunMathPiFunctionAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the shared RAND benchmark and keeps the provider result alive.
    /// PT-br: Executa o benchmark compartilhado de RAND e mantem o resultado do provedor vivo.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.MathRandFunction)]
    protected virtual void RunMathRandFunction()
    {
        if (!Dialect.SupportsMathRandFunction)
        {
            return;
        }

        var state = GetPreparedNoopQueryState("NoopQuery");
        var value = state.Service.RunMathRandFunctionAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the shared remainder benchmark and keeps the provider result alive.
    /// PT-br: Executa o benchmark compartilhado de resto e mantem o resultado do provedor vivo.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.MathRemainderFunction)]
    protected virtual void RunMathRemainderFunction()
    {
        if (!Dialect.SupportsMathRemainderFunction)
        {
            return;
        }

        var state = GetPreparedNoopQueryState("NoopQuery");
        var value = state.Service.RunMathRemainderFunctionAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the shared math truncation benchmark and keeps the provider result alive.
    /// PT-br: Executa o benchmark compartilhado de truncamento numerico e mantem o resultado do provedor vivo.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.MathTruncFunction)]
    protected virtual void RunMathTruncFunction()
    {
        if (!Dialect.SupportsMathTruncFunction)
        {
            return;
        }

        var state = GetPreparedNoopQueryState("NoopQuery");
        var value = state.Service.RunMathTruncFunctionAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the shared cotangent benchmark and keeps the provider result alive.
    /// PT-br: Executa o benchmark compartilhado de cotangente e mantem o resultado do provedor vivo.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.MathCotFunction)]
    protected virtual void RunMathCotFunction()
    {
        if (!Dialect.SupportsMathCotFunction)
        {
            return;
        }

        var state = GetPreparedNoopQueryState("NoopQuery");
        var value = state.Service.RunMathCotFunctionAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the MySQL utility math benchmark and keeps the provider result alive.
    /// PT-br: Executa o benchmark de utilitarios matematicos da familia MySQL e mantem o resultado do provedor vivo.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.MySqlUtilityMathFunctions)]
    protected virtual void RunMySqlUtilityMathFunctions()
    {
        if (!Dialect.SupportsMySqlUtilityMathFunctions)
        {
            return;
        }

        var state = GetPreparedNoopQueryState("MySqlUtilityMathFunctions");
        var value = state.Service.RunMySqlUtilityMathFunctionsAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the shared greatest/least/mod benchmark and keeps the provider result alive.
    /// PT-br: Executa o benchmark compartilhado de greatest/least/mod e mantem o resultado do provedor vivo.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.GreatestLeastModFunctions)]
    protected virtual void RunGreatestLeastModFunctions()
    {
        if (!Dialect.SupportsGreatestLeastModFunctions)
        {
            return;
        }

        var state = GetPreparedNoopQueryState("GreatestLeastModFunctions");
        var value = state.Service.RunGreatestLeastModFunctionsAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the DB2 alias math benchmark and keeps the provider result alive.
    /// PT-br: Executa o benchmark de aliases matematicos do DB2 e mantem o resultado do provedor vivo.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.Db2AliasMathFunctions)]
    protected virtual void RunDb2AliasMathFunctions()
    {
        if (!Dialect.SupportsDb2AliasMathFunctions)
        {
            return;
        }

        var state = GetPreparedNoopQueryState("Db2AliasMathFunctions");
        var value = state.Service.RunDb2AliasMathFunctionsAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the Firebird alias math benchmark and keeps the provider result alive.
    /// PT-br: Executa o benchmark de aliases matematicos do Firebird e mantem o resultado do provedor vivo.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.FirebirdAliasMathFunctions)]
    protected virtual void RunFirebirdAliasMathFunctions()
    {
        if (!Dialect.SupportsFirebirdAliasMathFunctions)
        {
            return;
        }

        var state = GetPreparedNoopQueryState("FirebirdAliasMathFunctions");
        var value = state.Service.RunFirebirdAliasMathFunctionsAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the shared transcendental math benchmark and keeps the provider result alive.
    /// PT-br: Executa o benchmark compartilhado de matematica transcendental e mantem o resultado do provedor vivo.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.MathTranscendentalFunctions)]
    protected virtual void RunMathTranscendentalFunctions()
    {
        if (!Dialect.SupportsMathTranscendentalFunctions)
        {
            return;
        }

        var state = GetPreparedNoopQueryState("MathTranscendentalFunctions");
        var value = state.Service.RunMathTranscendentalFunctionsAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }
}
