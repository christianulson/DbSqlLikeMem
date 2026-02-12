using System;
using Microsoft.VisualStudio.Shell;

namespace DbSqlLikeMem.VisualStudioExtension;

public sealed class DbSqlLikeMemToolWindow : ToolWindowPane
{
    public DbSqlLikeMemToolWindow() : base(null)
    {
        Caption = "DbSqlLikeMem Explorer";
        Content = new UI.DbSqlLikeMemToolWindowControl();
    }
}
