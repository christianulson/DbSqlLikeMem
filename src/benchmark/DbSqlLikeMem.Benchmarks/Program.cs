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
    /// <summary>
    /// EN: Parses the benchmark command line and runs the selected benchmark mode or catalog validation.
    /// PT: Analisa a linha de comando do benchmark e executa o modo selecionado ou a validacao do catalogo.
    /// </summary>
    public static void Main(string[] args)
    {
        var options = BenchmarkRunOptions.Parse(args);

        if (options.ValidateCatalog)
        {
            var report = BenchmarkCatalogValidator.Validate();
            Console.WriteLine(report.Format());
            Environment.ExitCode = report.IsValid ? 0 : 1;
            return;
        }

        BenchmarkSwitcher
            .FromAssembly(typeof(Program).Assembly)
            .Run(options.BenchmarkDotNetArgs, new BenchmarkConfig(options));
    }
}

/// <summary>
/// EN: Holds the command-line options used to configure benchmark execution.
/// PT: Guarda as opcoes de linha de comando usadas para configurar a execucao do benchmark.
/// </summary>
public sealed record BenchmarkRunOptions(
    bool IsTest,
    bool UseInProcess,
    bool PreferPreProvisionedDatabases,
    bool ValidateCatalog,
    string[] BenchmarkDotNetArgs)
{
    /// <summary>
    /// EN: Parses benchmark command-line arguments into a structured options record.
    /// PT: Analisa os argumentos de linha de comando do benchmark em um registro estruturado de opcoes.
    /// </summary>
    /// <param name="args">EN: The raw command-line arguments. PT: Os argumentos brutos da linha de comando.</param>
    /// <returns>EN: The parsed benchmark execution options. PT: As opcoes de execucao do benchmark analisadas.</returns>
    public static BenchmarkRunOptions Parse(string[] args)
    {
        var benchmarkArgs = new List<string>();
        var isTest = false;
        var useInProcess = false;
        var preferPreProvisionedDatabases = false;
        var validateCatalog = false;

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
                case "--validate-catalog":
                    validateCatalog = true;
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
            ValidateCatalog: validateCatalog,
            BenchmarkDotNetArgs: benchmarkArgs.ToArray());
    }
}

/// <summary>
/// EN: Builds the BenchmarkDotNet configuration used by the benchmark entry point.
/// PT: Monta a configuracao do BenchmarkDotNet usada pelo ponto de entrada do benchmark.
/// </summary>
public class BenchmarkConfig : ManualConfig
{
    /// <summary>
    /// EN: Creates a BenchmarkDotNet configuration for the selected benchmark mode.
    /// PT: Cria uma configuracao do BenchmarkDotNet para o modo de benchmark selecionado.
    /// </summary>
    /// <param name="options">EN: The parsed benchmark execution options. PT: As opcoes de execucao do benchmark analisadas.</param>
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

        var useInProcess = ShouldUseInProcess(options);

        if (options.UseInProcess && !useInProcess)
        {
            Console.WriteLine("InProcess was requested but skipped for this run. Use it only for short DbSqlLikeMem or Sqlite benchmark filters.");
        }

        if (useInProcess)
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

    private static bool ShouldUseInProcess(BenchmarkRunOptions options)
    {
        if (!options.UseInProcess)
        {
            return false;
        }

        if (!TryGetFilter(options.BenchmarkDotNetArgs, out var filter))
        {
            return false;
        }

        if (filter.Contains("Testcontainers", StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        return filter.Contains("DbSqlLikeMem", StringComparison.OrdinalIgnoreCase)
            || filter.Contains("Sqlite", StringComparison.OrdinalIgnoreCase);
    }

    private static bool TryGetFilter(string[] args, out string filter)
    {
        for (var i = 0; i < args.Length - 1; i++)
        {
            var arg = args[i];
            if (string.Equals(arg, "--filter", StringComparison.OrdinalIgnoreCase)
                || string.Equals(arg, "-f", StringComparison.OrdinalIgnoreCase))
            {
                filter = args[i + 1];
                return true;
            }
        }

        filter = string.Empty;
        return false;
    }
}
