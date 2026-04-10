using DbSqlLikeMem.VisualStudioExtension.Core.Models;

namespace DbSqlLikeMem.VisualStudioExtension.UI;

internal static class DatabaseTypeCatalog
{
    internal const string DefaultDatabaseType = "SqlServer";

    internal static IReadOnlyList<string> SupportedDatabaseTypes { get; } = DatabaseTypeNormalizer.GetSupportedDisplayNames();
}
