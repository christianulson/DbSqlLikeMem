using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Toolchains;

using BenchmarkDotNet.Configs;

namespace DbSqlLikeMem.Benchmarks;

internal static class Program
{
    public static void Main(string[] args)
    {
        BenchmarkSwitcher
            .FromAssembly(typeof(Program).Assembly)
            .Run(args);
    }
}

public class BenchmarkConfig : ManualConfig
{
    public BenchmarkConfig()
    {
        ArtifactsPath = @"../../docs/Wiki/BenchmarkResults";
    }
}