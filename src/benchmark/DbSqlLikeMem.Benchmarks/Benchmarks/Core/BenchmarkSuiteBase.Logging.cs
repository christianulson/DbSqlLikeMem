namespace DbSqlLikeMem.Benchmarks.Core;

public abstract partial class BenchmarkSuiteBase
{
    private static readonly object _setupLogSync = new();

    protected void LogSetupIssue(Exception ex)
    {
        var root = ex?.GetBaseException();

        var message =
            $"[SETUP-{root!.GetType().Name}] {root.ToString()}";

        Console.WriteLine(message);

        var logEntry =
            $"{DateTime.UtcNow:O} {message}{Environment.NewLine}{root?.StackTrace}{Environment.NewLine}{Environment.NewLine}";

        lock (_setupLogSync)
        {
            var directory = BenchmarkLogPath.GetDirectory();
            Directory.CreateDirectory(directory);

            var file = BenchmarkLogPath.GetFilePath($"{GetType().FullName}-setup-errors.log");
            if (!File.Exists(file))
                File.Create(file).Dispose();
            File.AppendAllText(
                file,
                logEntry);
        }
    }

    private static readonly object _logSync = new();
    private static readonly HashSet<string> Errors = [];

    protected virtual void LogBenchmarkIssue(BenchmarkFeatureId feature, Exception ex)
    {
        var root = ex?.GetBaseException();
        var message = root is NotSupportedException
            ? $"[NA-{root!.GetType().Name}] {feature}: {root.Message}{Environment.NewLine}{Environment.NewLine}"
            : $"[NA-{root!.GetType().Name}] {feature}: {root.Message} -- {ex?.StackTrace}{Environment.NewLine}{Environment.NewLine}";

        Console.WriteLine(message);

        lock (_logSync)
        {
            var errorKey = $"{GetType().FullName}|{feature}|{root.GetType().FullName}|{root.Message}";
            if (Errors.Contains(errorKey))
                return;
            Errors.Add(errorKey);

            var directory = BenchmarkLogPath.GetDirectory();
            Directory.CreateDirectory(directory);

            var file = BenchmarkLogPath.GetFilePath($"{GetType().FullName}-errors.log");
            File.AppendAllText(
                file,
                message + Environment.NewLine);
        }
    }
}
