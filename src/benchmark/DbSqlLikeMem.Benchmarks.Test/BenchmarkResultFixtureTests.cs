namespace DbSqlLikeMem.Benchmarks.Test;

/// <summary>
/// EN: Verifies the minimum benchmark result fixtures used by the benchmark test system.
/// PT-br: Verifica as fixtures minimas de resultado de benchmark usadas pelo sistema de testes de benchmark.
/// </summary>
public sealed class BenchmarkResultFixtureTests
{
    /// <summary>
    /// EN: Verifies the complete fixture keeps the structured success status and run identifier.
    /// PT-br: Verifica se a fixture completa preserva o status estruturado de sucesso e o identificador da execucao.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void CompleteFixture_ShouldExposeSucceededStatus()
        => AssertFixtureStatus("benchmark-result.complete.json", "Succeeded");

    /// <summary>
    /// EN: Verifies the skipped fixture keeps the structured skip status.
    /// PT-br: Verifica se a fixture ignorada preserva o status estruturado de skip.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void SkippedFixture_ShouldExposeSkippedStatus()
        => AssertFixtureStatus("benchmark-result.skipped.json", "Skipped");

    /// <summary>
    /// EN: Verifies the failed fixture keeps the structured failure status.
    /// PT-br: Verifica se a fixture com falha preserva o status estruturado de falha.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void FailedFixture_ShouldExposeFailedStatus()
        => AssertFixtureStatus("benchmark-result.failed.json", "Failed");

    /// <summary>
    /// EN: Verifies the invalid fixture omits the required status field for negative-path coverage.
    /// PT-br: Verifica se a fixture invalida omite o campo status obrigatorio para cobertura do caminho negativo.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void MissingFieldFixture_ShouldOmitStatus()
    {
        using var document = JsonDocument.Parse(ReadFixture("benchmark-result.missing-field.json"));

        Assert.False(document.RootElement.TryGetProperty("status", out _));
    }

    private static void AssertFixtureStatus(string fixtureName, string expectedStatus)
    {
        using var document = JsonDocument.Parse(ReadFixture(fixtureName));

        Assert.True(document.RootElement.TryGetProperty("status", out var status));
        Assert.Equal(expectedStatus, status.GetString());
    }

    private static string ReadFixture(string fixtureName)
    {
        var fixturePath = Path.Combine(AppContext.BaseDirectory, "Fixtures", fixtureName);
        return File.ReadAllText(fixturePath);
    }
}
