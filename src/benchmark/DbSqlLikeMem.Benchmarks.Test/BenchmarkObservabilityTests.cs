namespace DbSqlLikeMem.Benchmarks.Test;

/// <summary>
/// EN: Verifies the benchmark observability contracts used by logs and exported run metadata.
/// PT-br: Verifica os contratos de observabilidade do benchmark usados por logs e metadados exportados da execucao.
/// </summary>
public sealed class BenchmarkObservabilityTests
{
    /// <summary>
    /// EN: Verifies the current run identifier is embedded in the benchmark log directory path.
    /// PT-br: Verifica se o identificador da execucao atual fica embutido no caminho da pasta de logs do benchmark.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void BenchmarkRunContext_ShouldEmbedRunIdInLogDirectory()
    {
        BenchmarkRunContext.Initialize(BenchmarkRunProfile.Diagnostic);

        var logDirectory = BenchmarkLogPath.GetDirectory();

        Assert.EndsWith(
            Path.Combine("Logs", BenchmarkRunContext.RunId),
            logDirectory,
            StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Verifies the environment manifest serializes the run correlation identifier for downstream exports.
    /// PT-br: Verifica se o manifesto de ambiente serializa o identificador de correlacao da execucao para exportacoes posteriores.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void BenchmarkRunEnvironmentManifest_ShouldSerializeRunId()
    {
        var manifest = new BenchmarkRunEnvironmentManifest(
            RunId: "run-123",
            JobId: "job-456",
            Environment: new BenchmarkRunEnvironmentDetails(
                Profile: "core",
                Os: "Windows",
                Framework: "net8.0",
                Runtime: ".NET 8",
                Machine: "machine-1",
                BenchmarkDotNetVersion: "0.0.0",
                TimestampUtc: new DateTimeOffset(2026, 5, 1, 12, 0, 0, TimeSpan.Zero)));

        var json = JsonSerializer.Serialize(
            manifest,
            new JsonSerializerOptions
            {
                PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
                WriteIndented = true
            });

        Assert.Contains("\"runId\": \"run-123\"", json, StringComparison.Ordinal);
        Assert.Contains("\"jobId\": \"job-456\"", json, StringComparison.Ordinal);
    }

    /// <summary>
    /// EN: Verifies the environment manifest keeps the published JSON snapshot shape.
    /// PT-br: Verifica se o manifesto de ambiente mantem o formato JSON publicado no snapshot.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void BenchmarkRunEnvironmentManifest_ShouldMatchSnapshot()
    {
        var manifest = new BenchmarkRunEnvironmentManifest(
            RunId: "run-123",
            JobId: "job-456",
            Environment: new BenchmarkRunEnvironmentDetails(
                Profile: "core",
                Os: "Windows",
                Framework: "net8.0",
                Runtime: ".NET 8",
                Machine: "machine-1",
                BenchmarkDotNetVersion: "0.0.0",
                TimestampUtc: new DateTimeOffset(2026, 5, 1, 12, 0, 0, TimeSpan.Zero)));

        var json = JsonSerializer.Serialize(
            manifest,
            new JsonSerializerOptions
            {
                PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
                WriteIndented = true
            });

        Assert.Equal(
            SnapshotTestHelper.NormalizeLineEndings(SnapshotTestHelper.ReadRepoFile("src/benchmark/DbSqlLikeMem.Benchmarks.Test/Fixtures/benchmark-run.environment.snapshot.json")),
            SnapshotTestHelper.NormalizeLineEndings(json));
    }

    /// <summary>
    /// EN: Verifies the published benchmark result schema exposes the expected run and status fields.
    /// PT-br: Verifica se o schema publicado de resultado de benchmark expõe os campos esperados de execucao e status.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void BenchmarkResultSchema_ShouldExposeRunIdAndStatus()
    {
        BenchmarkRunContext.Initialize(BenchmarkRunProfile.Core);

        var benchmarkProjectDirectory = Directory
            .GetParent(BenchmarkLogPath.GetDirectory())!
            .Parent!
            .FullName;

        var schemaPath = Path.Combine(benchmarkProjectDirectory, "benchmark-result.schema.json");
        using var document = JsonDocument.Parse(File.ReadAllText(schemaPath));

        var properties = document.RootElement.GetProperty("properties");
        Assert.True(properties.TryGetProperty("runId", out _));
        Assert.True(properties.TryGetProperty("status", out _));

        var statusEnum = document
            .RootElement
            .GetProperty("$defs")
            .GetProperty("status")
            .GetProperty("enum");

        Assert.Contains("Succeeded", statusEnum.EnumerateArray().Select(static value => value.GetString()));
        Assert.Contains("Skipped", statusEnum.EnumerateArray().Select(static value => value.GetString()));
        Assert.Contains("NotSupported", statusEnum.EnumerateArray().Select(static value => value.GetString()));
        Assert.Contains("Failed", statusEnum.EnumerateArray().Select(static value => value.GetString()));
    }

}
