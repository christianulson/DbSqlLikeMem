using System.ComponentModel.Design;
using Microsoft.VisualStudio.Shell;

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

    public static async Task InitializeAsync(AsyncPackage package)
    {
        await ThreadHelper.JoinableTaskFactory.SwitchToMainThreadAsync(package.DisposalToken);
        if (await package.GetServiceAsync(typeof(IMenuCommandService)).ConfigureAwait(true) is not OleMenuCommandService commandService)
            return;

        _ = new OpenToolWindowCommand(package, commandService);
    }

    private void Execute(object? sender, EventArgs e)
    {
        ThreadHelper.ThrowIfNotOnUIThread();
        var window = package.FindToolWindow(typeof(DbSqlLikeMemToolWindow), 0, true);
        if (window?.Frame is null)
            throw new InvalidOperationException("Não foi possível criar a janela da extensão DbSqlLikeMem.");

        Microsoft.VisualStudio.ErrorHandler.ThrowOnFailure(((Microsoft.VisualStudio.Shell.Interop.IVsWindowFrame)window.Frame).Show());
    }
}
