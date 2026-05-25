namespace DbSqlLikeMem.Benchmarks.Core;

public abstract partial class BenchmarkSuiteBase
{
    /// <summary>
    /// EN: Executes a JSON scalar read benchmark.
    /// PT-br: Executa um benchmark de leitura escalar JSON.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("json")]
    public void JsonScalarRead() => Run(BenchmarkFeatureId.JsonScalarRead);

    /// <summary>
    /// EN: Executes a JSON path read benchmark.
    /// PT-br: Executa um benchmark de leitura por caminho JSON.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("json")]
    public void JsonPathRead() => Run(BenchmarkFeatureId.JsonPathRead);

    /// <summary>
    /// EN: Executes the JSON missing-path benchmark.
    /// PT-br: Executa o benchmark de caminho JSON ausente.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("json")]
    public void JsonMissingPathRead() => Run(BenchmarkFeatureId.JsonMissingPathRead);

    /// <summary>
    /// EN: Executes the JSON missing-path benchmark and expects a null result.
    /// PT-br: Executa o benchmark de caminho JSON ausente e espera um resultado nulo.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("json")]
    public void JsonMissingPathReturnsNull() => Run(BenchmarkFeatureId.JsonMissingPathReturnsNull);

    /// <summary>
    /// EN: Executes the JSON root-fragment benchmark.
    /// PT-br: Executa o benchmark de fragmento raiz JSON.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("json")]
    public void JsonQueryRootFragment() => Run(BenchmarkFeatureId.JsonQueryRootFragment);

    /// <summary>
    /// EN: Executes the JSON_MODIFY replacement benchmark.
    /// PT-br: Executa o benchmark de substituicao JSON_MODIFY.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("json")]
    public void JsonModifyReplace() => Run(BenchmarkFeatureId.JsonModifyReplace);

    /// <summary>
    /// EN: Executes the JSON typed field matrix benchmark.
    /// PT-br: Executa o benchmark da matriz de campos tipados com JSON.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("json")]
    public void JsonTypedFieldMatrix() => Run(BenchmarkFeatureId.JsonTypedFieldMatrix);

    /// <summary>
    /// EN: Executes the json_each benchmark over a JSON array.
    /// PT-br: Executa o benchmark json_each sobre um array JSON.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("json")]
    public void JsonEachFromArray() => Run(BenchmarkFeatureId.JsonEachFromArray);

    /// <summary>
    /// EN: Executes the json_each benchmark over a JSON object.
    /// PT-br: Executa o benchmark json_each sobre um objeto JSON.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("json")]
    public void JsonEachFromObject() => Run(BenchmarkFeatureId.JsonEachFromObject);

    /// <summary>
    /// EN: Executes the json_tree benchmark over JSON.
    /// PT-br: Executa o benchmark json_tree sobre JSON.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("json")]
    public void JsonTreeStructure() => Run(BenchmarkFeatureId.JsonTreeStructure);

    /// <summary>
    /// EN: Executes the OPENJSON benchmark over a JSON array.
    /// PT-br: Executa o benchmark OPENJSON sobre um array JSON.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("json")]
    public void OpenJsonArray() => Run(BenchmarkFeatureId.OpenJsonArray);
}
