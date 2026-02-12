using System;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using DbSqlLikeMem.VisualStudioExtension.Commands;
using Microsoft.VisualStudio.Shell;
using Task = System.Threading.Tasks.Task;

namespace DbSqlLikeMem.VisualStudioExtension;

[PackageRegistration(UseManagedResourcesOnly = true, AllowsBackgroundLoading = true)]
[InstalledProductRegistration("DbSqlLikeMem", "Explorer e geração de classes para objetos de banco", "0.1")]
[ProvideMenuResource("Menus.ctmenu", 1)]
[ProvideToolWindow(typeof(DbSqlLikeMemToolWindow))]
[Guid(PackageGuidString)]
public sealed class DbSqlLikeMemExtensionPackage : AsyncPackage
{
    public const string PackageGuidString = "f175ddf6-0067-43ed-9fd7-5780f8e8ff70";

    protected override async Task InitializeAsync(CancellationToken cancellationToken, IProgress<ServiceProgressData> progress)
    {
        await OpenToolWindowCommand.InitializeAsync(this);
    }
}
