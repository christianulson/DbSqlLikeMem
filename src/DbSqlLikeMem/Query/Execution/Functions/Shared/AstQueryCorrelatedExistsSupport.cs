using static DbSqlLikeMem.AstQueryExecutorBase;

namespace DbSqlLikeMem;

internal static class AstQueryCorrelatedExistsSupport
{
    internal static bool TryGetCorrelatedCountLookupPattern(
        SqlExpr where,
        Source source,
        out IReadOnlyList<CorrelatedLookupKeyPair> keyPairs,
        out SqlExpr? innerFilterExpr)
    {
        keyPairs = [];
        innerFilterExpr = null;

        var conjuncts = new List<SqlExpr>();
        AstQuerySubqueryLookupSupport.FlattenConjuncts(where, conjuncts);
        var conjunctCount = conjuncts.Count;
        if (conjunctCount == 0)
            return false;

        var pairs = new List<CorrelatedLookupKeyPair>(conjunctCount);
        var filterParts = new List<SqlExpr>(conjunctCount);

        for (var i = 0; i < conjunctCount; i++)
        {
            var conjunct = conjuncts[i];
            if (TryGetCorrelatedCountEquality(conjunct, source, out var innerKeyExpr, out var outerKeyExpr))
            {
                pairs.Add(new CorrelatedLookupKeyPair(innerKeyExpr, outerKeyExpr));
                continue;
            }

            if (!AstQueryInnerColumnAnalysisHelper.ExpressionUsesOnlyInnerColumnsOrConstants(conjunct, source))
                return false;

            filterParts.Add(conjunct);
        }

        if (pairs.Count == 0)
            return false;

        keyPairs = pairs;
        innerFilterExpr = filterParts.Count switch
        {
            0 => null,
            1 => filterParts[0],
            _ => AstQuerySubqueryLookupSupport.CombineConjuncts(filterParts)
        };
        return true;
    }

    internal static Source ResolveCorrelatedExistsPatternSource(
        SqlTableSource tableSource,
        IDictionary<string, Source> ctes,
        Func<string, SqlTableSource, IDictionary<string, Source>, Source> buildPatternSource,
        Func<SqlTableSource, IDictionary<string, Source>, Source> resolveSource)
    {
        var name = tableSource.Name;
        if (tableSource.Derived is null
            && tableSource.DerivedUnion is null
            && tableSource.TableFunction is null
            && tableSource.Pivot is null
            && tableSource.Unpivot is null
            && !string.IsNullOrWhiteSpace(name)
            && !name!.Equals("DUAL", StringComparison.OrdinalIgnoreCase)
            && !ctes.ContainsKey(name!))
        {
            var normalizedName = name.NormalizeName();
            var cacheKey = string.Concat(
                tableSource.DbName ?? string.Empty,
                '\u001F',
                normalizedName);

            return buildPatternSource(cacheKey, tableSource, ctes);
        }

        return resolveSource(tableSource, ctes);
    }

    internal static string BuildCorrelatedLookupStateCacheKey(
        string operation,
        SqlTableSource table,
        IReadOnlyList<CorrelatedLookupKeyPair> keyPairs,
        SqlExpr? innerFilterExpr)
    {
        var canonicalSql = BuildCorrelatedLookupCanonicalSql(table, keyPairs, innerFilterExpr);
        if (string.IsNullOrWhiteSpace(canonicalSql))
            return string.Empty;

        return string.Concat(operation, '\u001F', canonicalSql);
    }

    internal static bool TryGetCorrelatedCountEquality(
        SqlExpr expression,
        Source source,
        out SqlExpr innerKeyExpr,
        out SqlExpr outerKeyExpr)
    {
        innerKeyExpr = null!;
        outerKeyExpr = null!;

        if (expression is not BinaryExpr eq || eq.Op != SqlBinaryOp.Eq)
            return false;

        var leftIsInner = AstQueryInnerColumnAnalysisHelper.TryResolveInnerColumnName(eq.Left, source, out _);
        var rightIsInner = AstQueryInnerColumnAnalysisHelper.TryResolveInnerColumnName(eq.Right, source, out _);

        if (leftIsInner == rightIsInner)
            return false;

        var otherSide = leftIsInner ? eq.Right : eq.Left;
        if (AstQueryInnerColumnAnalysisHelper.ExpressionReferencesInnerColumns(otherSide, source))
            return false;

        innerKeyExpr = leftIsInner ? eq.Left : eq.Right;
        outerKeyExpr = otherSide;
        return true;
    }

    internal static string BuildCorrelatedLookupCanonicalSql(
        SqlTableSource table,
        IReadOnlyList<CorrelatedLookupKeyPair> keyPairs,
        SqlExpr? innerFilterExpr)
    {
        if (table.Name is null)
            return string.Empty;

        var sourceSql = table.Name.NormalizeName();
        if (string.IsNullOrWhiteSpace(sourceSql) || keyPairs.Count == 0)
            return string.Empty;

        var source = table;
        var keyPairCount = keyPairs.Count;
        var fragmentCapacity = keyPairCount;
        List<SqlExpr>? conjuncts = null;

        if (innerFilterExpr is not null)
        {
            conjuncts = new List<SqlExpr>();
            AstQuerySubqueryLookupSupport.FlattenConjuncts(innerFilterExpr, conjuncts);
            var conjunctCount = conjuncts.Count;
            fragmentCapacity += conjunctCount;
        }

        var fragments = new List<string>(fragmentCapacity);

        for (var i = 0; i < keyPairCount; i++)
        {
            var pair = keyPairs[i];
            var left = NormalizeCorrelatedExistsExpressionForCacheKey(pair.InnerExpr, source);
            var right = NormalizeCorrelatedExistsExpressionForCacheKey(pair.OuterExpr, source);
            if (string.IsNullOrWhiteSpace(left) || string.IsNullOrWhiteSpace(right))
                continue;

            fragments.Add(StringComparer.Ordinal.Compare(left, right) <= 0
                ? $"{left} = {right}"
                : $"{right} = {left}");
        }

        if (conjuncts is not null)
        {
            var conjunctCount = conjuncts.Count;
            for (var i = 0; i < conjunctCount; i++)
            {
                var conjunct = conjuncts[i];
                var normalized = NormalizeCorrelatedExistsExpressionForCacheKey(conjunct, source);
                if (!string.IsNullOrWhiteSpace(normalized))
                    fragments.Add(normalized);
            }
        }

        if (fragments.Count == 0)
            return string.Empty;

        if (fragments.Count == 1)
            return $"SELECT 1 FROM {sourceSql} T1 WHERE {fragments[0]}";

        fragments.Sort(StringComparer.OrdinalIgnoreCase);
        return $"SELECT 1 FROM {sourceSql} T1 WHERE {string.Join(" AND ", fragments)}";
    }

    internal static string FormatCorrelatedLookupCacheFieldName(SqlExpr expr)
        => expr switch
        {
            IdentifierExpr id => id.Name.NormalizeName(),
            ColumnExpr column when string.IsNullOrWhiteSpace(column.Qualifier)
                => column.Name.NormalizeName(),
            ColumnExpr column => $"{column.Qualifier.NormalizeName()}.{column.Name.NormalizeName()}",
            _ => SqlExprPrinter.Print(expr).NormalizeName()
        };

    internal static string NormalizeCorrelatedExistsPredicateForCacheKey(
        SqlExpr predicate,
        SqlTableSource source)
    {
        var conjuncts = new List<SqlExpr>();
        AstQuerySubqueryLookupSupport.FlattenConjuncts(predicate, conjuncts);
        var conjunctCount = conjuncts.Count;
        if (conjunctCount == 0)
            return string.Empty;

        var segments = new List<string>(conjunctCount);
        for (var i = 0; i < conjunctCount; i++)
        {
            var segment = NormalizeCorrelatedExistsConjunctForCacheKey(conjuncts[i], source);
            if (!string.IsNullOrWhiteSpace(segment))
                segments.Add(segment);
        }

        if (segments.Count == 0)
            return string.Empty;

        if (segments.Count == 1)
            return segments[0];

        segments.Sort(StringComparer.OrdinalIgnoreCase);
        return segments.Count == 1
            ? segments[0]
            : string.Join(" AND ", segments);
    }

    internal static string NormalizeCorrelatedExistsConjunctForCacheKey(
        SqlExpr conjunct,
        SqlTableSource source)
    {
        if (conjunct is BinaryExpr binary && binary.Op == SqlBinaryOp.Eq)
        {
            var left = NormalizeCorrelatedExistsExpressionForCacheKey(binary.Left, source);
            if (string.IsNullOrWhiteSpace(left))
                return string.Empty;

            var right = NormalizeCorrelatedExistsExpressionForCacheKey(binary.Right, source);
            if (string.IsNullOrWhiteSpace(right))
                return string.Empty;

            return StringComparer.Ordinal.Compare(left, right) <= 0
                ? $"{left} = {right}"
                : $"{right} = {left}";
        }

        return NormalizeCorrelatedExistsExpressionForCacheKey(conjunct, source);
    }

    internal static string NormalizeCorrelatedExistsExpressionForCacheKey(
        SqlExpr expr,
        SqlTableSource source)
    {
        var text = SqlExprPrinter.Print(expr);
        if (text.IndexOf('.') < 0)
            return text;

        var alias = source.Alias;
        var name = source.Name;

        if (!string.IsNullOrWhiteSpace(alias))
            text = ReplaceIdentifierQualifierForCacheKey(text, alias, "T1");

        if (!string.IsNullOrWhiteSpace(name)
            && !string.Equals(name, alias, StringComparison.OrdinalIgnoreCase))
            text = ReplaceIdentifierQualifierForCacheKey(text, name, "T1");

        return text;
    }

    private static string ReplaceIdentifierQualifierForCacheKey(
        string sql,
        string? qualifier,
        string replacement)
    {
        if (string.IsNullOrWhiteSpace(sql) || string.IsNullOrWhiteSpace(qualifier) || string.IsNullOrWhiteSpace(replacement))
            return sql;

        var safeQualifier = qualifier!;
        var sqlLength = sql.Length;
        var qualifierLength = safeQualifier.Length;
        if (sql.IndexOf(safeQualifier, StringComparison.OrdinalIgnoreCase) < 0)
            return sql;

        var sb = new StringBuilder(sqlLength);
        for (var i = 0; i < sqlLength; i++)
        {
            if (IsIdentifierQualifierReferenceAt(sql, i, safeQualifier))
            {
                sb.Append(replacement);
                sb.Append('.');
                i += qualifierLength;
                continue;
            }

            sb.Append(sql[i]);
        }

        return sb.ToString();
    }

    private static bool IsSqlIdentifierChar(char c)
        => char.IsLetterOrDigit(c) || c is '_' or '$';

    private static bool IsIdentifierQualifierReferenceAt(
        string sql,
        int startIndex,
        string qualifier)
    {
        if (startIndex < 0 || startIndex >= sql.Length || string.IsNullOrWhiteSpace(qualifier))
            return false;

        var sqlLength = sql.Length;
        var qualifierLength = qualifier.Length;

        if (startIndex + qualifierLength >= sqlLength)
            return false;

        if (startIndex > 0 && IsSqlIdentifierChar(sql[startIndex - 1]))
            return false;

        for (var i = 0; i < qualifierLength; i++)
        {
            if (char.ToUpperInvariant(sql[startIndex + i]) != char.ToUpperInvariant(qualifier[i]))
                return false;
        }

        return sql[startIndex + qualifierLength] == '.';
    }
}
