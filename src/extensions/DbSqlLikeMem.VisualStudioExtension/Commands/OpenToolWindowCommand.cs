using System.ComponentModel.Design;
using Microsoft.VisualStudio.Shell;
using Microsoft.VisualStudio.Shell.Interop;
using UiResources = DbSqlLikeMem.VisualStudioExtension.Properties.Resources;

namespace DbSqlLikeMem.VisualStudioExtension.Commands;

internal sealed class OpenToolWindowCommand
{
    private readonly AsyncPackage package;
    private const int CommandId = 0x0100;
    private static readonly Guid CommandSet = new("ae30167a-a4d2-41d7-8a97-15792afdae4f");

    private OpenToolWindowCommand(AsyncPackage package, OleMenuCommandService commandService)
    {
        this.package = package;
        var menuCommandId = new CommandID(CommandSet, CommandId);
        var menuItem = new MenuCommand(Execute, menuCommandId);
        commandService.AddCommand(menuItem);
    }

    /// <summary>
    /// EN: Registers the explorer command on the UI thread and reports an unavailable command service.
    /// PT-br: Registra o comando do explorador na thread de UI e informa quando o servico de comandos esta indisponivel.
    /// </summary>
    public static async Task InitializeAsync(AsyncPackage package)
    {
        await ThreadHelper.JoinableTaskFactory.SwitchToMainThreadAsync(package.DisposalToken);
        if (await package.GetServiceAsync(typeof(IMenuCommandService)).ConfigureAwait(true) is not OleMenuCommandService commandService)
        {
            throw new InvalidOperationException("Visual Studio menu command service is unavailable.");
        }

        _ = new OpenToolWindowCommand(package, commandService);
        ActivityLog.LogInformation(nameof(OpenToolWindowCommand), "Registered DbSqlLikeMem.OpenExplorer.");
    }

    private void Execute(object? sender, EventArgs e)
    {
        ThreadHelper.ThrowIfNotOnUIThread();
        try
        {
            var window = package.FindToolWindow(typeof(DbSqlLikeMemToolWindow), 0, true);
            if (window?.Frame is not IVsWindowFrame frame)
            {
                throw new InvalidOperationException(UiResources.FailedToOpenToolWindow);
            }

            Microsoft.VisualStudio.ErrorHandler.ThrowOnFailure(frame.Show());
            ActivityLog.LogInformation(nameof(OpenToolWindowCommand), "Opened DbSqlLikeMem Explorer.");
        }
        catch (Exception ex)
        {
            ActivityLog.LogError(nameof(OpenToolWindowCommand), $"Failed to open DbSqlLikeMem Explorer: {ex}");
            VsShellUtilities.ShowMessageBox(
                package,
                $"{UiResources.FailedToOpenToolWindow}{Environment.NewLine}{ex.Message}",
                "DbSqlLikeMem",
                OLEMSGICON.OLEMSGICON_CRITICAL,
                OLEMSGBUTTON.OLEMSGBUTTON_OK,
                OLEMSGDEFBUTTON.OLEMSGDEFBUTTON_FIRST);
        }
    }
}
