namespace DbSqlLikeMem;

internal static class SqlSourceFormattingHelper
{
    internal static string FormatSource(SqlTableSource? source)
    {
        if (source is null)
            return "<none>";

        if (source.Derived is not null)
            return $"subquery AS {source.Alias ?? "<derived>"}";

        if (source.DerivedUnion is not null)
            return $"union-subquery AS {source.Alias ?? "<derived_union>"}";

        if (source.TableFunction is not null)
        {
            var functionName = FormatQualifiedFunctionSource(source);
            var alias = source.Alias ?? source.TableFunction.Name;
            return alias.Equals(source.TableFunction.Name, StringComparison.OrdinalIgnoreCase)
                ? functionName
                : $"{functionName} AS {alias}";
        }

        return FormatQualifiedTableName(source);
    }

    internal static string FormatJoinDebugDetails(SqlJoin join)
    {
        var source = FormatSource(join.Table);
        if (join.Type is SqlJoinType.CrossApply or SqlJoinType.OuterApply)
            return source;

        var predicate = SqlExprPrinter.Print(join.On);
        return string.IsNullOrWhiteSpace(predicate)
            ? source
            : $"{source};on={predicate}";
    }

    internal static string FormatQualifiedFunctionSource(SqlTableSource source)
    {
        var functionName = source.DbName is null
            ? source.TableFunction?.Name ?? "<unknown_function>"
            : $"{source.DbName}.{source.TableFunction?.Name ?? "<unknown_function>"}";

        if (source.TableFunction?.Name.Equals(SqlConst.STRING_SPLIT, StringComparison.OrdinalIgnoreCase) == true
            && source.TableFunction.Args.Count == 3)
        {
            return $"{functionName}(..., ..., enable_ordinal)";
        }

        if (source.TableFunction?.Name.Equals(SqlConst.OPENJSON, StringComparison.OrdinalIgnoreCase) == true
            && source.TableFunction.Args.Count == 2)
        {
            var pathShape = TryFormatOpenJsonPathShape(source.TableFunction.Args[1]);
            return source.OpenJsonWithClause is null
                ? $"{functionName}(..., {pathShape})"
                : $"{functionName}(..., {pathShape}) WITH (...)";
        }

        if (source.TableFunction?.Name.Equals(SqlConst.JSON_TABLE, StringComparison.OrdinalIgnoreCase) == true
            && source.TableFunction.Args.Count == 2)
        {
            var pathShape = TryFormatOpenJsonPathShape(source.TableFunction.Args[1]);
            return source.JsonTableClause is null
                ? $"{functionName}(..., {pathShape})"
                : $"{functionName}(..., {pathShape}) COLUMNS (...)";
        }

        return source.OpenJsonWithClause is null && source.JsonTableClause is null
            ? $"{functionName}(...)"
            : $"{functionName}(...) WITH (...)";
    }

    internal static string FormatQualifiedTableName(SqlTableSource source)
    {
        if (source.Name is null)
            return "<unknown_table>";

        return source.DbName is null
            ? source.Name
            : $"{source.DbName}.{source.Name}";
    }

    internal static string TryFormatOpenJsonPathShape(SqlExpr pathExpr)
    {
        if (pathExpr is not LiteralExpr { Value: string pathText })
            return "path";

        var trimmed = pathText.Trim();
        if (trimmed.StartsWith("strict ", StringComparison.OrdinalIgnoreCase))
            return "strict path";

        if (trimmed.StartsWith("lax ", StringComparison.OrdinalIgnoreCase))
            return "lax path";

        return "path";
    }
}
