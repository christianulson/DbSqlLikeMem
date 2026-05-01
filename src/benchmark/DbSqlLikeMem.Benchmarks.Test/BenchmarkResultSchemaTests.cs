namespace DbSqlLikeMem.Benchmarks.Test;

/// <summary>
/// EN: Verifies the published benchmark result schema keeps the expected contract shape.
/// PT-br: Verifica se o schema publicado de resultado de benchmark mantem o formato esperado de contrato.
/// </summary>
public sealed class BenchmarkResultSchemaTests
{
    /// <summary>
    /// EN: Verifies the top-level schema stays closed and keeps the required public fields.
    /// PT-br: Verifica se o schema de topo permanece fechado e mantem os campos publicos obrigatorios.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void BenchmarkResultSchema_ShouldExposeExpectedTopLevelContract()
    {
        using var document = JsonDocument.Parse(ReadSchema());
        var root = document.RootElement;

        Assert.Equal("object", root.GetProperty("type").GetString());
        Assert.False(root.GetProperty("additionalProperties").GetBoolean());

        var required = root.GetProperty("required").EnumerateArray().Select(static value => value.GetString()).ToHashSet(StringComparer.Ordinal);
        Assert.Contains("benchmarkStableId", required);
        Assert.Contains("benchmarkFeatureId", required);
        Assert.Contains("providerId", required);
        Assert.Contains("engine", required);
        Assert.Contains("suiteName", required);
        Assert.Contains("methodName", required);
        Assert.Contains("category", required);
        Assert.Contains("environment", required);
        Assert.Contains("status", required);
        Assert.DoesNotContain("runId", required);

        var properties = root.GetProperty("properties");
        Assert.True(properties.TryGetProperty("runId", out _));
        Assert.True(properties.TryGetProperty("meanMicroseconds", out _));
        Assert.True(properties.TryGetProperty("errorMicroseconds", out _));
        Assert.True(properties.TryGetProperty("ratio", out _));
        Assert.True(properties.TryGetProperty("iterationCount", out _));
        Assert.True(properties.TryGetProperty("tags", out _));

        var statusEnum = root.GetProperty("$defs").GetProperty("status").GetProperty("enum").EnumerateArray().Select(static value => value.GetString()).ToHashSet(StringComparer.Ordinal);
        Assert.Equal(["Succeeded", "Skipped", "NotSupported", "Failed"], statusEnum);
    }

    /// <summary>
    /// EN: Verifies the environment block stays closed and preserves the official profile enum.
    /// PT-br: Verifica se o bloco de ambiente permanece fechado e preserva a enumeração oficial de perfil.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void BenchmarkResultSchema_ShouldExposeExpectedEnvironmentContract()
    {
        using var document = JsonDocument.Parse(ReadSchema());
        var environment = document.RootElement.GetProperty("$defs").GetProperty("environment");

        Assert.Equal("object", environment.GetProperty("type").GetString());
        Assert.False(environment.GetProperty("additionalProperties").GetBoolean());

        var required = environment.GetProperty("required").EnumerateArray().Select(static value => value.GetString()).ToHashSet(StringComparer.Ordinal);
        Assert.Contains("profile", required);
        Assert.Contains("os", required);
        Assert.Contains("framework", required);
        Assert.Contains("runtime", required);
        Assert.DoesNotContain("machine", required);
        Assert.DoesNotContain("providerVersion", required);
        Assert.DoesNotContain("benchmarkDotNetVersion", required);
        Assert.DoesNotContain("timestampUtc", required);

        var profileEnum = environment.GetProperty("properties").GetProperty("profile").GetProperty("enum").EnumerateArray().Select(static value => value.GetString()).ToHashSet(StringComparer.Ordinal);
        Assert.Equal(["smoke", "core", "full", "diagnostic"], profileEnum);
    }

    private static string ReadSchema()
        => SnapshotTestHelper.ReadRepoFile("src/benchmark/DbSqlLikeMem.Benchmarks/benchmark-result.schema.json");
}
