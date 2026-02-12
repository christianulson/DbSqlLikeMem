using System;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using DbSqlLikeMem.VisualStudioExtension.Commands;
using Microsoft.VisualStudio.Shell;
using Microsoft.VisualStudio.Shell.Interop;
using Task = System.Threading.Tasks.Task;

namespace DbSqlLikeMem.VisualStudioExtension;

/// <summary>
/// Initializes and registers the DbSqlLikeMem Visual Studio extension package.
/// Inicializa e registra o pacote da extensão DbSqlLikeMem no Visual Studio.
/// </summary>
[PackageRegistration(UseManagedResourcesOnly = true, AllowsBackgroundLoading = true)]
[InstalledProductRegistration("DbSqlLikeMem", "Explorer e geração de classes para objetos de banco", "0.1")]
[ProvideMenuResource("Menus.ctmenu", 1)]
[ProvideAutoLoad(UIContextGuids80.NoSolution, PackageAutoLoadFlags.BackgroundLoad)]
[ProvideAutoLoad(UIContextGuids80.SolutionExists, PackageAutoLoadFlags.BackgroundLoad)]
[ProvideToolWindow(typeof(DbSqlLikeMemToolWindow))]
[Guid(PackageGuidString)]
public sealed class DbSqlLikeMemExtensionPackage : AsyncPackage
{
    /// <summary>
    /// Gets the package GUID used by Visual Studio to identify this extension package.
    /// Obtém o GUID do pacote usado pelo Visual Studio para identificar este pacote de extensão.
    /// </summary>
    public const string PackageGuidString = "f175ddf6-0067-43ed-9fd7-5780f8e8ff70";

    /// <summary>
    /// Initializes commands and services required by the extension package.
    /// Inicializa os comandos e serviços necessários pelo pacote da extensão.
    /// </summary>
    protected override Task InitializeAsync(CancellationToken cancellationToken, IProgress<ServiceProgressData> progress)
        => OpenToolWindowCommand.InitializeAsync(this);
}
