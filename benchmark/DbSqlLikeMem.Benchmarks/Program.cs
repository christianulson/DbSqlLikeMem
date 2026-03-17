using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Engines;
using BenchmarkDotNet.Exporters;
using BenchmarkDotNet.Exporters.Csv;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Loggers;
using BenchmarkDotNet.Order;
using BenchmarkDotNet.Reports;
using BenchmarkDotNet.Toolchains.InProcess.NoEmit;
using Perfolizer.Horology;
using System.Globalization;

namespace DbSqlLikeMem.Benchmarks;

internal static class Program
{
    public static void Main(string[] args)
    {
        var options = BenchmarkRunOptions.Parse(args);

        BenchmarkSwitcher
            .FromAssembly(typeof(Program).Assembly)
            .Run(options.BenchmarkDotNetArgs, new BenchmarkConfig(options));
    }
}

public sealed record BenchmarkRunOptions(
    bool IsTest,
    bool UseInProcess,
    bool PreferPreProvisionedDatabases,
    string[] BenchmarkDotNetArgs)
{
    public static BenchmarkRunOptions Parse(string[] args)
    {
        var benchmarkArgs = new List<string>();
        var isTest = false;
        var useInProcess = false;
        var preferPreProvisionedDatabases = false;

        foreach (var arg in args)
        {
            switch (arg)
            {
                case "test":
                case "--test":
                    isTest = true;
                    break;
                case "inprocess":
                case "--inprocess":
                    useInProcess = true;
                    break;
                case "preprovisioned":
                case "--preprovisioned":
                    preferPreProvisionedDatabases = true;
                    break;
                default:
                    benchmarkArgs.Add(arg);
                    break;
            }
        }

        return new BenchmarkRunOptions(
            IsTest: isTest,
            UseInProcess: useInProcess,
            PreferPreProvisionedDatabases: preferPreProvisionedDatabases,
            BenchmarkDotNetArgs: benchmarkArgs.ToArray());
    }
}

public class BenchmarkConfig : ManualConfig
{
    public BenchmarkConfig(BenchmarkRunOptions options)
    {
        AddLogger(ConsoleLogger.Default);
        AddColumnProvider(DefaultColumnProviders.Instance);
        AddExporter(HtmlExporter.Default);
        AddExporter(MarkdownExporter.GitHub);
        AddExporter(CsvExporter.Default);

        var job = Job.Default;

        if (options.IsTest)
        {
            job = job
                .WithStrategy(RunStrategy.ColdStart)
                .WithLaunchCount(1)
                .WithWarmupCount(0)
                .WithIterationCount(1);

            WithOrderer(new DefaultOrderer(SummaryOrderPolicy.FastestToSlowest));
        }
        else
        {
            job = job
                .WithStrategy(RunStrategy.Throughput)
                .WithLaunchCount(1)
                .WithWarmupCount(1)
                .WithIterationCount(3);

            ArtifactsPath = Path.GetFullPath("../../docs/Wiki/BenchmarkResults");
        }

        if (options.UseInProcess)
        {
            job = job
                .WithToolchain(InProcessNoEmitToolchain.Instance)
                .WithId(options.PreferPreProvisionedDatabases ? "InProcess-PreProvisioned" : "InProcess");
        }
        else if (options.PreferPreProvisionedDatabases)
        {
            job = job.WithId("PreProvisioned");
        }

        AddJob(job);

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
