using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Engines;
using BenchmarkDotNet.Exporters;
using BenchmarkDotNet.Exporters.Csv;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Loggers;
using BenchmarkDotNet.Order;
using BenchmarkDotNet.Reports;
using Perfolizer.Horology;
using System.Globalization;

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
        AddLogger(ConsoleLogger.Default);

        AddColumnProvider(DefaultColumnProviders.Instance);

        AddExporter(HtmlExporter.Default);
        AddExporter(MarkdownExporter.GitHub);
        AddExporter(CsvExporter.Default);

        if (args.Contains("test"))
        {
            AddJob(Job.Default
                .WithStrategy(RunStrategy.ColdStart) // roda uma vez
                .WithLaunchCount(1)                  // um processo
                .WithWarmupCount(0)                  // sem warmup
                .WithIterationCount(1));             // uma iteração

            WithOrderer(new DefaultOrderer(SummaryOrderPolicy.FastestToSlowest));
        }
        else
        {
            AddJob(Job.Default
                .WithStrategy(RunStrategy.Throughput)
                .WithLaunchCount(1)
                .WithWarmupCount(1)
                .WithIterationCount(3));

            ArtifactsPath = Path.GetFullPath("../../docs/Wiki/BenchmarkResults");
        }
        SummaryStyle = new SummaryStyle(
            cultureInfo: CultureInfo.GetCultureInfo("en-US"),
            printUnitsInHeader: true,
            sizeUnit: null,
            timeUnit: TimeUnit.Microsecond,
            printUnitsInContent: true,
            printZeroValuesInContent: false
        );
    }
}