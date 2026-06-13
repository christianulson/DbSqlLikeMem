using static DbSqlLikeMem.AstQueryExecutorBase;

namespace DbSqlLikeMem;

internal static class AstQueryRowSourceHelper
{
    internal static EvalRow CreateSourceEvalRow(Source source, Dictionary<string, object?> fields)
    {
        var sourceColumns = source.ColumnNames;
        var ordinalValues = OrdinalPool.Rent(sourceColumns.Count);
        var ordinalIndexes = source.SourceOrdinalIndexes;

        var pooledFields = SqlRowPool.Get(fields.Count);
        foreach (var kvp in fields)
            pooledFields[kvp.Key] = kvp.Value;

        for (var i = 0; i < sourceColumns.Count; i++)
        {
            var qualifiedName = source.GetQualifiedColumnName(i);
            ordinalValues[i] = fields.TryGetValue(qualifiedName, out var current) ? current : null;
        }

        return new EvalRow(pooledFields, source.SourceDict)
        {
            OrdinalValues = ordinalValues,
            OrdinalIndexes = ordinalIndexes,
            SingleSource = source.SourceDict.Count == 1 ? source : null
        };
    }
}
