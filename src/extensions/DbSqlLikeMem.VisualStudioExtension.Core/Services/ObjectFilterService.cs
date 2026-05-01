using DbSqlLikeMem.VisualStudioExtension.Core.Models;

namespace DbSqlLikeMem.VisualStudioExtension.Core.Services;

/// <summary>
/// EN: Filters database objects by name using the selected comparison mode.
/// PT-br: Filtra objetos de banco pelo nome usando o modo de comparacao selecionado.
/// </summary>
public sealed class ObjectFilterService
{
    /// <summary>
    /// EN: Returns the objects whose names match the provided filter value.
    /// PT-br: Retorna os objetos cujos nomes correspondem ao valor de filtro informado.
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
