using DbSqlLikeMem.VisualStudioExtension.Core.Models;

namespace DbSqlLikeMem.VisualStudioExtension.Core.Services;

public sealed class ObjectFilterService
{
    public IReadOnlyCollection<DatabaseObjectReference> Filter(
        IEnumerable<DatabaseObjectReference> objects,
        string value,
        FilterMode mode)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return objects.ToArray();
        }

        return mode switch
        {
            FilterMode.Equals => objects
                .Where(o => o.Name.Equals(value, StringComparison.OrdinalIgnoreCase))
                .ToArray(),
            FilterMode.Like => objects
                .Where(o => o.Name.Contains(value, StringComparison.OrdinalIgnoreCase))
                .ToArray(),
            _ => Array.Empty<DatabaseObjectReference>()
        };
    }
}
