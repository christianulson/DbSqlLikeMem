namespace DbSqlLikeMem;

internal sealed class AstQueryStringSplitTableFunctionHandler(
    QueryExecutionContext context,
    Func<SqlExpr, AstQueryExecutorBase.EvalRow, AstQueryExecutorBase.EvalGroup?, IDictionary<string, AstQueryExecutorBase.Source>, object?> evalExpression)
{
    private readonly QueryExecutionContext _context = context ?? throw new ArgumentNullException(nameof(context));
    private readonly Func<SqlExpr, AstQueryExecutorBase.EvalRow, AstQueryExecutorBase.EvalGroup?, IDictionary<string, AstQueryExecutorBase.Source>, object?> _evalExpression = evalExpression ?? throw new ArgumentNullException(nameof(evalExpression));

    internal TableResultMock Execute(
        SqlTableSource tableSource,
        IDictionary<string, AstQueryExecutorBase.Source> ctes,
        AstQueryExecutorBase.EvalRow? outerRow)
    {
        var function = tableSource.TableFunction ?? throw new InvalidOperationException("STRING_SPLIT source is missing function metadata.");
        var alias = tableSource.Alias ?? function.Name;
        var dialect = _context.Dialect ?? throw new InvalidOperationException("Dialeto SQL não disponível para STRING_SPLIT.");
        if (!dialect.SupportsStringSplitFunction)
            throw SqlUnsupported.NotSupported(dialect, SqlConst.STRING_SPLIT);

        if (function.Args.Count is < 2 or > 3)
            throw new NotSupportedException("STRING_SPLIT table source currently supports two or three arguments in the mock.");

        var evalRow = AstQueryTableFunctionExecutionHelper.CreateFunctionEvaluationRow(outerRow);
        var input = _evalExpression(function.Args[0], evalRow, null, ctes);
        var separator = _evalExpression(function.Args[1], evalRow, null, ctes)?.ToString() ?? string.Empty;
        var includeOrdinal = false;
        if (function.Args.Count == 3)
        {
            if (!dialect.SupportsStringSplitOrdinalArgument)
                throw SqlUnsupported.NotSupported(dialect, "STRING_SPLIT enable_ordinal");

            includeOrdinal = EvaluateStringSplitOrdinalFlag(
                _evalExpression(function.Args[2], evalRow, null, ctes));
        }

        var result = CreateStringSplitTableResult(alias, includeOrdinal);
        if (AstQueryTableFunctionExecutionHelper.IsNullish(input))
            return result;

        if (separator.Length != 1)
            throw new InvalidOperationException("STRING_SPLIT separator must be a single character in the mock.");

        var pieces = (input?.ToString() ?? string.Empty)
            .Split(separator[0]);

        for (var index = 0; index < pieces.Length; index++)
        {
            var row = new Dictionary<int, object?>
            {
                [0] = pieces[index]
            };

            if (includeOrdinal)
                row[1] = (long)index + 1L;

            result.Add(row);
        }

        return result;
    }

    private static TableResultMock CreateStringSplitTableResult(string tableAlias, bool includeOrdinal)
    {
        var columns = new List<TableResultColMock>(includeOrdinal ? 2 : 1)
        {
            new(tableAlias, "value", "value", 0, DbType.String, true)
        };

        if (includeOrdinal)
            columns.Add(new TableResultColMock(tableAlias, "ordinal", "ordinal", 1, DbType.Int64, false));

        return new TableResultMock
        {
            Columns = columns
        };
    }

    private static bool EvaluateStringSplitOrdinalFlag(object? rawValue)
    {
        if (rawValue is null or DBNull)
            throw new InvalidOperationException("STRING_SPLIT enable_ordinal must be 0 or 1 in the mock.");

        if (rawValue is bool boolean)
            return boolean;

        if (rawValue is byte or sbyte or short or ushort or int or uint or long or ulong)
        {
            var numeric = Convert.ToInt64(rawValue, CultureInfo.InvariantCulture);
            return numeric switch
            {
                0 => false,
                1 => true,
                _ => throw new InvalidOperationException("STRING_SPLIT enable_ordinal must be 0 or 1 in the mock.")
            };
        }

        throw new InvalidOperationException("STRING_SPLIT enable_ordinal must be 0 or 1 in the mock.");
    }
}
