using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Engines;
using BenchmarkDotNet.Exporters;
using BenchmarkDotNet.Exporters.Csv;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Order;
using BenchmarkDotNet.Loggers;

namespace DbSqlLikeMem.Benchmarks;

internal static class Program
{
    public static void Main(string[] args)
    {
        BenchmarkSwitcher
            .FromAssembly(typeof(Program).Assembly)
            .Run(args, new BenchmarkConfig(args));
    }
}


public class BenchmarkConfig : ManualConfig
{
    public BenchmarkConfig(string[] args)
    {
        if (args.Contains("test"))
        {
            AddJob(Job.Default
                .WithStrategy(RunStrategy.ColdStart) // roda uma vez
                .WithLaunchCount(1)                  // um processo
                .WithWarmupCount(0)                  // sem warmup
                .WithIterationCount(1));             // uma iteração

            AddLogger(ConsoleLogger.Default);

            AddColumnProvider(DefaultColumnProviders.Instance);

            AddExporter(HtmlExporter.Default);
            AddExporter(MarkdownExporter.GitHub);
            AddExporter(CsvExporter.Default);

            WithOrderer(new DefaultOrderer(SummaryOrderPolicy.FastestToSlowest));
        }
        else
        {
            ArtifactsPath = Path.GetFullPath("../../docs/Wiki/BenchmarkResults");
        }
    }
}