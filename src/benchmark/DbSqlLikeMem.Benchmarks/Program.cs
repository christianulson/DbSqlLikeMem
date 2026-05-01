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
using System.Runtime.InteropServices;
using System.Text.Json;

namespace DbSqlLikeMem.Benchmarks;

internal static class Program
{
    /// <summary>
    /// EN: Parses the benchmark command line and runs the selected benchmark mode or catalog validation.
    /// PT-br: Analisa a linha de comando do benchmark e executa o modo selecionado ou a validacao do catalogo.
    /// </summary>
    public static void Main(string[] args)
    {
        var options = BenchmarkRunOptions.Parse(args);
        BenchmarkRunContext.Initialize(options.Profile);

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
/// PT-br: Guarda as opcoes de linha de comando usadas para configurar a execucao do benchmark.
/// </summary>
/// <param name="Profile">EN: The selected benchmark execution profile. PT-br: O perfil de execucao de benchmark selecionado.</param>
/// <param name="IsTest">EN: Indicates whether the run uses the short test mode. PT-br: Indica se a execucao usa o modo curto de teste.</param>
/// <param name="UseInProcess">EN: Indicates whether the runner should prefer the in-process toolchain. PT-br: Indica se o executavel deve preferir a toolchain in-process.</param>
/// <param name="PreferPreProvisionedDatabases">EN: Indicates whether the run should prefer pre-provisioned databases. PT-br: Indica se a execucao deve preferir bancos preprovisionados.</param>
/// <param name="ValidateCatalog">EN: Indicates whether the runner should validate the benchmark catalog only. PT-br: Indica se o executavel deve validar apenas o catalogo de benchmarks.</param>
/// <param name="BenchmarkDotNetArgs">EN: The remaining arguments forwarded to BenchmarkDotNet. PT-br: Os argumentos restantes encaminhados ao BenchmarkDotNet.</param>
public sealed record BenchmarkRunOptions(
    BenchmarkRunProfile Profile,
    bool IsTest,
    bool UseInProcess,
    bool PreferPreProvisionedDatabases,
    bool ValidateCatalog,
    string[] BenchmarkDotNetArgs)
{
    /// <summary>
    /// EN: Parses benchmark command-line arguments into a structured options record.
    /// PT-br: Analisa os argumentos de linha de comando do benchmark em um registro estruturado de opcoes.
    /// </summary>
    /// <param name="args">EN: The raw command-line arguments. PT-br: Os argumentos brutos da linha de comando.</param>
    /// <returns>EN: The parsed benchmark execution options. PT-br: As opcoes de execucao do benchmark analisadas.</returns>
    public static BenchmarkRunOptions Parse(string[] args)
    {
        var benchmarkArgs = new List<string>();
        var profile = BenchmarkRunProfile.Full;
        var isTest = false;
        var useInProcess = false;
        var preferPreProvisionedDatabases = false;
        var validateCatalog = false;

        for (var i = 0; i < args.Length; i++)
        {
            var arg = args[i];
            switch (arg)
            {
                case "--profile":
                    if (i + 1 < args.Length && !args[i + 1].StartsWith("-", StringComparison.Ordinal))
                    {
                        profile = ParseProfile(args[++i]);
                    }
                    break;
                case "smoke":
                case "core":
                case "full":
                case "diagnostic":
                    profile = ParseProfile(arg);
                    break;
                case "--profile=smoke":
                case "--profile=core":
                case "--profile=full":
                case "--profile=diagnostic":
                    profile = ParseProfile(arg[(arg.IndexOf('=') + 1)..]);
                    break;
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
            Profile: profile,
            IsTest: isTest,
            UseInProcess: useInProcess,
            PreferPreProvisionedDatabases: preferPreProvisionedDatabases,
            ValidateCatalog: validateCatalog,
            BenchmarkDotNetArgs: [.. benchmarkArgs]);
    }

    private static BenchmarkRunProfile ParseProfile(string value) =>
        value.ToLowerInvariant() switch
        {
            "smoke" => BenchmarkRunProfile.Smoke,
            "core" => BenchmarkRunProfile.Core,
            "full" => BenchmarkRunProfile.Full,
            "diagnostic" => BenchmarkRunProfile.Diagnostic,
            _ => BenchmarkRunProfile.Full
        };
}

/// <summary>
/// EN: Builds the BenchmarkDotNet configuration used by the benchmark entry point.
/// PT-br: Monta a configuracao do BenchmarkDotNet usada pelo ponto de entrada do benchmark.
/// </summary>
internal class BenchmarkConfig : ManualConfig
{
    private const string BenchmarkArtifactsRelativePath = "../../../docs/Wiki/BenchmarkResults";
    private const string EnvironmentManifestName = "benchmark-run.environment.json";

    /// <summary>
    /// EN: Creates a BenchmarkDotNet configuration for the selected benchmark mode.
    /// PT-br: Cria uma configuracao do BenchmarkDotNet para o modo de benchmark selecionado.
    /// </summary>
    /// <param name="options">EN: The parsed benchmark execution options. PT-br: As opcoes de execucao do benchmark analisadas.</param>
    public BenchmarkConfig(BenchmarkRunOptions options)
    {
        AddLogger(ConsoleLogger.Default);
        AddColumnProvider(DefaultColumnProviders.Instance);
        AddExporter(HtmlExporter.Default);
        AddExporter(MarkdownExporter.GitHub);
        AddExporter(CsvExporter.Default);

        var job = Job.Default;

        if (options.Profile == BenchmarkRunProfile.Smoke || options.IsTest)
        {
            job = job
                .WithStrategy(RunStrategy.ColdStart)
                .WithLaunchCount(1)
                .WithWarmupCount(0)
                .WithIterationCount(1);

            WithOrderer(new DefaultOrderer(SummaryOrderPolicy.FastestToSlowest));
        }
        else if (options.Profile == BenchmarkRunProfile.Core)
        {
            job = job
                .WithStrategy(RunStrategy.Throughput)
                .WithLaunchCount(1)
                .WithWarmupCount(1)
                .WithIterationCount(3);

            ArtifactsPath = Path.GetFullPath(BenchmarkArtifactsRelativePath);
        }
        else if (options.Profile == BenchmarkRunProfile.Diagnostic)
        {
            job = job
                .WithStrategy(RunStrategy.Throughput)
                .WithLaunchCount(1)
                .WithWarmupCount(2)
                .WithIterationCount(1);

            ArtifactsPath = Path.GetFullPath(BenchmarkArtifactsRelativePath);
        }
        else
        {
            job = job
                .WithStrategy(RunStrategy.Throughput)
                .WithLaunchCount(1)
                .WithWarmupCount(1)
                .WithIterationCount(5);

            ArtifactsPath = Path.GetFullPath(BenchmarkArtifactsRelativePath);
        }

        var jobId = options.Profile.ToString();

        var useInProcess = ShouldUseInProcess(options);

        if (options.UseInProcess && !useInProcess)
        {
            Console.WriteLine("InProcess was requested but skipped for this run. Use it only for short DbSqlLikeMem or Sqlite benchmark filters.");
        }

        if (useInProcess)
        {
            job = job
                .WithToolchain(InProcessNoEmitToolchain.Instance)
                .WithId(options.PreferPreProvisionedDatabases ? $"{jobId}-InProcess-PreProvisioned" : $"{jobId}-InProcess");
        }
        else if (options.PreferPreProvisionedDatabases)
        {
            job = job.WithId($"{jobId}-PreProvisioned");
        }
        else
        {
            job = job.WithId(jobId);
        }

        WriteEnvironmentManifest(options, jobId);
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

    private static void WriteEnvironmentManifest(BenchmarkRunOptions options, string jobId)
    {
        var manifestDirectory = Path.GetFullPath(BenchmarkArtifactsRelativePath);
        Directory.CreateDirectory(manifestDirectory);

        var manifest = new BenchmarkRunEnvironmentManifest(
            RunId: BenchmarkRunContext.RunId,
            JobId: jobId,
            Environment: new BenchmarkRunEnvironmentDetails(
                Profile: options.Profile.ToString().ToLowerInvariant(),
                Os: RuntimeInformation.OSDescription,
                Framework: "net8.0",
                Runtime: RuntimeInformation.FrameworkDescription,
                Machine: Environment.MachineName,
                BenchmarkDotNetVersion: typeof(BenchmarkAttribute).Assembly.GetName().Version?.ToString() ?? throw new Exception("Failed to get BenchmarkDotNet version."),
                TimestampUtc: DateTimeOffset.UtcNow));

        var json = JsonSerializer.Serialize(
            manifest,
            new JsonSerializerOptions
            {
                PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
                WriteIndented = true
            });

        json = json.Replace("\n", Environment.NewLine);
        File.WriteAllText(Path.Combine(manifestDirectory, EnvironmentManifestName), json);
    }
}

/// <summary>
/// EN: Captures the execution environment written alongside benchmark artifacts.
/// PT-br: Registra o ambiente de execucao gravado junto com os artefatos do benchmark.
/// </summary>
/// <param name="RunId">EN: The run correlation identifier. PT-br: O identificador de correlacao da execucao.</param>
/// <param name="JobId">EN: The BenchmarkDotNet job identifier. PT-br: O identificador do job do BenchmarkDotNet.</param>
public sealed record BenchmarkRunEnvironmentManifest(
    string RunId,
    string JobId,
    BenchmarkRunEnvironmentDetails Environment);

/// <summary>
/// EN: Captures the environment values used by the benchmark export pipeline.
/// PT-br: Registra os valores de ambiente usados pelo pipeline de exportacao do benchmark.
/// </summary>
/// <param name="Profile">EN: The benchmark execution profile. PT-br: O perfil de execucao do benchmark.</param>
/// <param name="Os">EN: The operating system description. PT-br: A descricao do sistema operacional.</param>
/// <param name="Framework">EN: The target framework used by the runner. PT-br: O framework alvo usado pelo executavel.</param>
/// <param name="Runtime">EN: The runtime description. PT-br: A descricao do runtime.</param>
/// <param name="Machine">EN: The machine name. PT-br: O nome da maquina.</param>
/// <param name="BenchmarkDotNetVersion">EN: The BenchmarkDotNet assembly version. PT-br: A versao do assembly do BenchmarkDotNet.</param>
/// <param name="TimestampUtc">EN: The UTC timestamp for the run. PT-br: O carimbo de tempo UTC da execucao.</param>
public sealed record BenchmarkRunEnvironmentDetails(
    string Profile,
    string Os,
    string Framework,
    string Runtime,
    string Machine,
    string BenchmarkDotNetVersion,
    DateTimeOffset TimestampUtc);

/// <summary>
/// EN: Defines the benchmark execution profiles used by the command-line runner.
/// PT-br: Define os perfis de execucao de benchmark usados pelo executavel de linha de comando.
/// </summary>
public enum BenchmarkRunProfile
{
    /// <summary>
    /// EN: Uses the shortest run for smoke validation.
    /// PT-br: Usa a execucao mais curta para validacao smoke.
    /// </summary>
    Smoke,

    /// <summary>
    /// EN: Uses the core benchmark cadence for regular comparison runs.
    /// PT-br: Usa a cadencia core para execucoes regulares de comparacao.
    /// </summary>
    Core,

    /// <summary>
    /// EN: Uses the full comparison cadence.
    /// PT-br: Usa a cadencia completa de comparacao.
    /// </summary>
    Full,

    /// <summary>
    /// EN: Uses a compact cadence with extra diagnostic emphasis.
    /// PT-br: Usa uma cadencia compacta com mais enfase diagnostica.
    /// </summary>
    Diagnostic
}
