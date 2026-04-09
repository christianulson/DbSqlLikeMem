using static DbSqlLikeMem.AstQueryExecutorBase;

namespace DbSqlLikeMem;

internal static class AstQueryRowSourceHelper
{
    internal static EvalRow CreateSourceEvalRow(Source source, Dictionary<string, object?> fields)
    {
        var sourceColumns = source.ColumnNames;
        var ordinalValues = new object?[sourceColumns.Count];
        var ordinalIndexes = new Dictionary<string, int>(sourceColumns.Count * 3, StringComparer.OrdinalIgnoreCase);
        for (var i = 0; i < sourceColumns.Count; i++)
        {
            var columnName = sourceColumns[i];
            var qualifiedName = $"{source.Alias}.{columnName}";
            var value = fields.TryGetValue(qualifiedName, out var current) ? current : null;
            ordinalValues[i] = value;
            ordinalIndexes.TryAdd(qualifiedName, i);
            ordinalIndexes.TryAdd(columnName, i);
            if (!source.Name.Equals(source.Alias, StringComparison.OrdinalIgnoreCase))
                ordinalIndexes.TryAdd($"{source.Name}.{columnName}", i);
        }

        var rowSources = new Dictionary<string, Source>(StringComparer.OrdinalIgnoreCase)
        {
            [source.Alias] = source
        };
        if (!source.Name.Equals(source.Alias, StringComparison.OrdinalIgnoreCase))
            rowSources[source.Name] = source;

        return new EvalRow(fields, rowSources)
        {
            OrdinalValues = ordinalValues,
            OrdinalIndexes = ordinalIndexes,
            SingleSource = source
        };
    }
}
