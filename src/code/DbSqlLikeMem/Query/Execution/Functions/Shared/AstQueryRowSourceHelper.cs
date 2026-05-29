using static DbSqlLikeMem.AstQueryExecutorBase;

namespace DbSqlLikeMem;

internal static class AstQueryRowSourceHelper
{
    internal static EvalRow CreateSourceEvalRow(Source source, Dictionary<string, object?> fields)
    {
        var sourceColumns = source.ColumnNames;
        var ordinalValues = new object?[sourceColumns.Count];
        var ordinalIndexes = source.SourceOrdinalIndexes;

        // Extract ordered field keys for lazy Fields reconstruction.
        var fieldKeys = new string[fields.Count];
        fields.Keys.CopyTo(fieldKeys, 0);

        for (var i = 0; i < sourceColumns.Count; i++)
        {
            var qualifiedName = source.GetQualifiedColumnName(i);
            ordinalValues[i] = fields.TryGetValue(qualifiedName, out var current) ? current : null;
        }

        return new EvalRow(null!, source.SourceDict)
        {
            OrdinalValues = ordinalValues,
            OrdinalIndexes = ordinalIndexes,
            SingleSource = source.SourceDict.Count == 1 ? source : null,
            FieldKeys = fieldKeys
        };
    }
}
