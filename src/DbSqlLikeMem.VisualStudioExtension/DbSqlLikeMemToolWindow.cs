using Microsoft.VisualStudio.Shell;

namespace DbSqlLikeMem.VisualStudioExtension;

/// <summary>
/// Hosts the main DbSqlLikeMem explorer tool window in Visual Studio.
/// Hospeda a janela principal do explorador DbSqlLikeMem no Visual Studio.
/// </summary>
public sealed class DbSqlLikeMemToolWindow : ToolWindowPane
{
    /// <summary>
    /// Creates the DbSqlLikeMem tool window and sets its caption and content.
    /// Cria a janela da ferramenta DbSqlLikeMem e define seu título e conteúdo.
    /// </summary>
    public DbSqlLikeMemToolWindow() : base(null)
    {
        Caption = "DbSqlLikeMem Explorer";
        Content = new UI.DbSqlLikeMemToolWindowControl();
    }
}
