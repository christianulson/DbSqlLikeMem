using DbSqlLikeMem.Benchmarks.Core;
using DbSqlLikeMem.Benchmarks.Sessions.External;
using DbSqlLikeMem.Benchmarks.Sessions.DbSqlLikeMem;
using DbSqlLikeMem.Npgsql.TestTools;
using DbSqlLikeMem.TestTools;
using Microsoft.Data.Sqlite;
using System.Data.Common;
using Xunit;

namespace DbSqlLikeMem.Benchmarks.Test;

/// <summary>
/// EN: Covers the benchmark automation catalog and its command-line validation flow.
/// PT: Cobre o catalogo de automacao de benchmarks e seu fluxo de validacao por linha de comando.
/// </summary>
public sealed class BenchmarkCatalogValidationTests(
        ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Ensures the benchmark catalog stays aligned with the public benchmark suite surface.
    /// PT: Garante que o catalogo de benchmarks permaneça alinhado com a superficie publica da suite.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void ValidateCatalog_ShouldHaveNoIssues()
    {
        var report = BenchmarkCatalogValidator.Validate();

        Assert.True(report.IsValid, report.Format());
    }

    /// <summary>
    /// EN: Ensures the benchmark run options parse the catalog validation flag without affecting benchmark arguments.
    /// PT: Garante que as opcoes de execucao de benchmark analisem a flag de validacao do catalogo sem afetar os argumentos do benchmark.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void Parse_ShouldRecognizeValidateCatalogFlag()
    {
        var options = BenchmarkRunOptions.Parse(["--validate-catalog", "--inprocess", "--filter", "*Insert*"]);

        Assert.True(options.ValidateCatalog);
        Assert.True(options.UseInProcess);
        Assert.False(options.IsTest);
        Assert.Equal(["--filter", "*Insert*"], options.BenchmarkDotNetArgs);
    }

    /// <summary>
    /// EN: Ensures the SQLite mock session can execute the stored procedure benchmark without parameter-direction errors.
    /// PT: Garante que a sessao mock de SQLite execute o benchmark de procedimento armazenado sem erros de direcao de parametro.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void SqliteSession_RunFeature_StoredProcedureCall_ShouldNotThrow()
    {
        using var session = new SqliteDbSqlLikeMemSession();

        session.Initialize();
        session.RunFeature(BenchmarkFeatureId.StoredProcedureCall);
    }

    /// <summary>
    /// EN: Ensures the SQLite native session skips the stored procedure benchmark instead of hitting an unsupported path.
    /// PT: Garante que a sessao nativa de SQLite ignore o benchmark de procedimento armazenado em vez de atingir um caminho nao suportado.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void SqliteNativeSession_RunFeature_StoredProcedureCall_ShouldNotThrow()
    {
        using var session = new SqliteNativeSession();

        session.Initialize();
        session.RunFeature(BenchmarkFeatureId.StoredProcedureCall);
    }

    /// <summary>
    /// EN: Ensures benchmark issue logging still writes a file when the provider display name contains a slash.
    /// PT: Garante que o log de issues do benchmark ainda grave um arquivo quando o nome exibido do provedor contem barra.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void Execute_ShouldWriteBenchmarkIssueLogForDisplayNamesWithSlashes()
    {
        var dialect = new NpgsqlProviderSqlDialect();
        var session = new ThrowingBenchmarkSession(dialect);
        var logDirectory = BenchmarkLogPath.GetDirectory();
        var logFile = BenchmarkLogPath.GetFilePath($"{session.GetType().FullName}-{dialect.DisplayName}-errors.log");

        try
        {
            Assert.True(
                logDirectory.EndsWith(
                    Path.Combine("src", "benchmark", "DbSqlLikeMem.Benchmarks", "Logs"),
                    StringComparison.OrdinalIgnoreCase),
                $"Expected benchmark log directory '{logDirectory}' to end with the benchmark project Logs folder.");

            if (File.Exists(logFile))
                File.Delete(logFile);

            session.Execute(BenchmarkFeatureId.ConnectionOpen);

            Assert.True(File.Exists(logFile), $"Expected benchmark log file '{logFile}' to be created.");
        }
        finally
        {
            session.Dispose();
            if (File.Exists(logFile))
                File.Delete(logFile);
        }
    }

    /// <summary>
    /// EN: Ensures cleanup failures write benchmark issue logs to the benchmark project Logs directory.
    /// PT: Garante que falhas de cleanup gravem logs de benchmark na pasta Logs do projeto de benchmark.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void SafeExecute_ShouldWriteBenchmarkIssueLogForMissingTableCleanup()
    {
        var dialect = new NpgsqlProviderSqlDialect();
        var session = new CleanupLoggingBenchmarkSession(dialect);
        var logDirectory = BenchmarkLogPath.GetDirectory();
        var logFile = BenchmarkLogPath.GetFilePath($"{session.GetType().FullName}-{dialect.DisplayName}-errors.log");

        try
        {
            Assert.True(
                logDirectory.EndsWith(
                    Path.Combine("src", "benchmark", "DbSqlLikeMem.Benchmarks", "Logs"),
                    StringComparison.OrdinalIgnoreCase),
                $"Expected benchmark log directory '{logDirectory}' to end with the benchmark project Logs folder.");

            if (File.Exists(logFile))
                File.Delete(logFile);

            session.TriggerCleanupFailure();

            Assert.True(File.Exists(logFile), $"Expected benchmark log file '{logFile}' to be created.");
        }
        finally
        {
            session.Dispose();
            if (File.Exists(logFile))
                File.Delete(logFile);
        }
    }

    private sealed class ThrowingBenchmarkSession(ProviderSqlDialect dialect)
        : BenchmarkSessionBase(dialect, BenchmarkEngine.Testcontainers)
    {
        protected override DbConnection CreateConnection() => throw new NotSupportedException();

        protected override void RunConnectionOpen()
            => throw new NotSupportedException("Benchmark logging regression.");
    }

    private sealed class CleanupLoggingBenchmarkSession(ProviderSqlDialect dialect)
        : BenchmarkSessionBase(dialect, BenchmarkEngine.NativeAdoNet)
    {
        protected override DbConnection CreateConnection() => new SqliteConnection("Data Source=:memory:");

        public void TriggerCleanupFailure()
        {
            using var connection = new SqliteConnection("Data Source=:memory:");
            connection.Open();

            SafeExecute(connection, "DROP TABLE usr_4b899215_0003d8f7_0003d8f6");
        }
    }
}
