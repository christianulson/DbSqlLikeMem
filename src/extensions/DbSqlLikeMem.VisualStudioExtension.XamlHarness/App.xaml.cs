using System.Diagnostics;
using System.Windows;
using DbSqlLikeMem.VisualStudioExtension.Core.Models;

namespace DbSqlLikeMem.VisualStudioExtension.XamlHarness;

/// <summary>
/// EN: Defines the WPF application entry point for the XAML harness host.
/// PT-br: Define o ponto de entrada da aplicação WPF para o host de testes de XAML.
/// </summary>
public partial class App : Application
{
    private const string HarnessLoadEnvironmentArgument = "--load-harness-environment";
    private const string HarnessStorageScopeArgumentPrefix = "--storage-scope=";
    private const string HarnessWorkspaceScopeArgumentPrefix = "--workspace-scope=";
    private const string HarnessLoadEnvironmentPropertyKey = "DbSqlLikeMem.XamlHarness.LoadEnvironment";
    private const string HarnessStorageScopePropertyKey = "DbSqlLikeMem.XamlHarness.StorageScopeKey";

    private HarnessEnvironmentManager? harnessEnvironmentManager;
    private CancellationTokenSource? shutdownCancellationTokenSource;

    internal Task<IReadOnlyCollection<ConnectionDefinition>>? HarnessEnvironmentLoadTask { get; private set; }

    /// <inheritdoc />
    protected override void OnStartup(StartupEventArgs e)
    {
        if (HasArgument(e.Args, HarnessLoadEnvironmentArgument))
        {
            Properties[HarnessLoadEnvironmentPropertyKey] = true;
            shutdownCancellationTokenSource = new CancellationTokenSource();
            harnessEnvironmentManager = new HarnessEnvironmentManager();
            HarnessEnvironmentLoadTask = harnessEnvironmentManager.InitializeAsync(shutdownCancellationTokenSource.Token);
        }
        else
        {
            var storageScopeKey = TryGetArgumentValue(e.Args, HarnessStorageScopeArgumentPrefix)
                ?? TryGetArgumentValue(e.Args, HarnessWorkspaceScopeArgumentPrefix);

            if (!string.IsNullOrWhiteSpace(storageScopeKey))
            {
                Properties[HarnessStorageScopePropertyKey] = storageScopeKey;
            }
        }

        base.OnStartup(e);

        MainWindow = new MainWindow();
        MainWindow.Show();
    }

    /// <inheritdoc />
    protected override void OnExit(ExitEventArgs e)
    {
        try
        {
            shutdownCancellationTokenSource?.Cancel();
#pragma warning disable VSTHRD002 // Avoid problematic synchronous waits
            harnessEnvironmentManager?.CleanupAsync().GetAwaiter().GetResult();
#pragma warning restore VSTHRD002 // Avoid problematic synchronous waits
        }
        catch (System.Exception ex)
        {
            Trace.WriteLine($"DbSqlLikeMem XAML harness cleanup error: {ex}");
        }
        finally
        {
            shutdownCancellationTokenSource?.Dispose();
            shutdownCancellationTokenSource = null;
        }

        base.OnExit(e);
    }

    private static bool HasArgument(string[] args, string expectedArgument)
    {
        foreach (var arg in args)
        {
            if (string.Equals(arg, expectedArgument, StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }
        }

        return false;
    }

    private static string? TryGetArgumentValue(string[] args, string expectedPrefix)
    {
        foreach (var arg in args)
        {
            if (!arg.StartsWith(expectedPrefix, StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            var value = arg.Substring(expectedPrefix.Length).Trim();
            return string.IsNullOrWhiteSpace(value) ? null : value.Trim('"');
        }

        return null;
    }
}
