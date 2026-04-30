namespace DbSqlLikeMem;

internal abstract partial class AstQueryExecutorBase
{
    private readonly Stack<IReadOnlyDictionary<string, object?>> _localParameterScopes = new();

    private static EvalRow AttachOuterRow(
        EvalRow inner,
        EvalRow outer)
        => inner.AttachOuterRow(outer);

    private static IEnumerable<EvalRow> AttachOuterRows(
        IEnumerable<EvalRow> rows,
        EvalRow outer)
    {
        foreach (var row in rows)
            yield return row.AttachOuterRow(outer);
    }
}
