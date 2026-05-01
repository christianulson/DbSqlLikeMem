namespace DbSqlLikeMem.Benchmarks.Core;

public abstract partial class BenchmarkSessionBase
{
    /// <summary>
    /// EN: Executes the JSON scalar read benchmark and keeps the provider result alive.
    /// PT-br: Executa o benchmark de leitura escalar JSON e mantem o resultado do provedor vivo.
    /// </summary>
    protected virtual void RunJsonScalarRead()
    {
        if (!Dialect.SupportsJsonScalarRead)
        {
            return;
        }

        var state = GetPreparedNoopQueryState("NoopQuery");
        var service = state.Service;
        var value = service.RunJsonScalarReadAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the JSON path read benchmark and keeps the provider result alive.
    /// PT-br: Executa o benchmark de leitura por caminho JSON e mantem o resultado do provedor vivo.
    /// </summary>
    protected virtual void RunJsonPathRead()
    {
        if (!Dialect.SupportsJsonScalarRead)
        {
            return;
        }

        var state = GetPreparedNoopQueryState("NoopQuery");
        var service = state.Service;
        var value = service.RunJsonPathReadAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the JSON missing-path benchmark and keeps the provider result alive.
    /// PT-br: Executa o benchmark de caminho JSON ausente e mantem o resultado do provedor vivo.
    /// </summary>
    protected virtual void RunJsonMissingPathRead()
    {
        if (!Dialect.SupportsJsonScalarRead)
        {
            return;
        }

        var state = GetPreparedNoopQueryState("NoopQuery");
        var value = state.Service.RunJsonMissingPathReadAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the missing JSON path benchmark and keeps the provider result alive when it is null.
    /// PT: Executa o benchmark de caminho JSON ausente e mantem o resultado do provedor vivo quando ele eh nulo.
    /// </summary>
    protected virtual void RunJsonMissingPathReturnsNull()
        => RunJsonMissingPathRead();

    /// <summary>
    /// EN: Executes the JSON_QUERY root-fragment benchmark and keeps the provider result alive.
    /// PT-br: Executa o benchmark de fragmento raiz JSON_QUERY e mantem o resultado do provedor vivo.
    /// </summary>
    protected virtual void RunJsonQueryRootFragment()
    {
        if (!Dialect.SupportsJsonQueryFunction)
        {
            return;
        }

        var state = GetPreparedNoopQueryState("NoopQuery");
        var value = state.Service.RunJsonQueryRootFragmentAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the JSON_MODIFY replacement benchmark and keeps the provider result alive.
    /// PT-br: Executa o benchmark de substituicao JSON_MODIFY e mantem o resultado do provedor vivo.
    /// </summary>
    protected virtual void RunJsonModifyReplace()
    {
        if (!Dialect.SupportsSqlServerScalarFunction("JSON_MODIFY"))
        {
            return;
        }

        var state = GetPreparedNoopQueryState("NoopQuery");
        var value = state.Service.RunJsonModifyReplaceAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the JSON typed field matrix benchmark and keeps the validated snapshot alive.
    /// PT-br: Executa o benchmark da matriz de campos tipados com JSON e mantem o snapshot validado ativo.
    /// </summary>
    protected virtual void RunJsonTypedFieldMatrix()
    {
        if (!Dialect.SupportsJsonScalarRead)
        {
            return;
        }

        var state = GetPreparedTypedFieldStorageMatrixState("JsonTypedFieldMatrix");
        var snapshot = state.RunJsonTypedFieldMatrix();
        GC.KeepAlive(snapshot);
    }

    /// <summary>
    /// EN: Executes the json_each benchmark over a JSON array and keeps the provider result alive.
    /// PT-br: Executa o benchmark json_each sobre um array JSON e mantem o resultado do provedor vivo.
    /// </summary>
    protected virtual void RunJsonEachFromArray()
    {
        if (!Dialect.SupportsJsonEachFunction)
        {
            return;
        }

        var state = GetPreparedNoopQueryState("NoopQuery");
        var value = state.Service.RunJsonEachFromArrayAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the json_each benchmark over a JSON object and keeps the provider result alive.
    /// PT-br: Executa o benchmark json_each sobre um objeto JSON e mantem o resultado do provedor vivo.
    /// </summary>
    protected virtual void RunJsonEachFromObject()
    {
        if (!Dialect.SupportsJsonEachFunction)
        {
            return;
        }

        var state = GetPreparedNoopQueryState("NoopQuery");
        var value = state.Service.RunJsonEachFromObjectAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the json_tree benchmark over JSON and keeps the provider result alive.
    /// PT-br: Executa o benchmark json_tree sobre JSON e mantem o resultado do provedor vivo.
    /// </summary>
    protected virtual void RunJsonTreeStructure()
    {
        if (!Dialect.SupportsJsonTreeFunction)
        {
            return;
        }

        var state = GetPreparedNoopQueryState("NoopQuery");
        var value = state.Service.RunJsonTreeStructureAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the OPENJSON array benchmark and keeps the provider result alive.
    /// PT-br: Executa o benchmark OPENJSON de array e mantem o resultado do provedor vivo.
    /// </summary>
    protected virtual void RunOpenJsonArray()
    {
        if (!Dialect.SupportsOpenJsonFunction)
        {
            return;
        }

        var state = GetPreparedNoopQueryState("NoopQuery");
        var value = state.Service.RunOpenJsonArrayAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }
}
