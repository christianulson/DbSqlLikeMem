using DbSqlLikeMem.VisualStudioExtension.Core.Models;

namespace DbSqlLikeMem.VisualStudioExtension.Core.Services;

/// <summary>
/// Represents this public API type.
/// Representa este tipo público da API.
/// </summary>
public sealed class ObjectFilterService
{
    /// <summary>
    /// Executes this API operation.
    /// Executa esta operação da API.
    /// </summary>
    public IReadOnlyCollection<DatabaseObjectReference> Filter(
        IEnumerable<DatabaseObjectReference> objects,
        string value,
        FilterMode mode)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return [.. objects];
        }

        return mode switch
        {
            FilterMode.Equals => objects
                .Where(o => o.Name.Equals(value, StringComparison.OrdinalIgnoreCase))
                .ToArray(),
            FilterMode.Like => [.. objects.Where(o => o.Name.Contains(value, StringComparison.OrdinalIgnoreCase))],
            _ => []
        };
    }
}
