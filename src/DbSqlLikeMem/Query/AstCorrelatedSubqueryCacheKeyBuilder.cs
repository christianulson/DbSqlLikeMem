using System.Collections.Concurrent;

namespace DbSqlLikeMem;

internal static class AstCorrelatedSubqueryCacheKeyBuilder
{
    private static readonly ConcurrentDictionary<string, string> _normalizedSqlCache = new(StringComparer.Ordinal);
    private static readonly Regex _cacheKeyWherePredicateRegex = new(
        @"\bWHERE\s+(?<predicate>.+?)(?=(?:\bGROUP\s+BY\b|\bHAVING\b|\bORDER\s+BY\b|\bLIMIT\b|\bOFFSET\b|\bUNION\b|$))",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Singleline | RegexOptions.Compiled);
    private static readonly Regex _cacheKeyHavingPredicateRegex = new(
        @"\bHAVING\s+(?<predicate>.+?)(?=(?:\bORDER\s+BY\b|\bLIMIT\b|\bOFFSET\b|\bUNION\b|$))",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Singleline | RegexOptions.Compiled);
    private static readonly Regex _qualifiedSqlIdentifierRegex = new(
        @"(?<![A-Za-z0-9_$])([A-Za-z_][A-Za-z0-9_$]*\.[A-Za-z_][A-Za-z0-9_$]*)(?![A-Za-z0-9_$])",
        RegexOptions.CultureInvariant | RegexOptions.Compiled);
    private static readonly Regex _subqueryAliasDeclarationRegex = new(
        @"\b(?:FROM|JOIN|APPLY)\s+(?:[A-Z_][A-Z0-9_$]*(?:\.[A-Z_][A-Z0-9_$]*)*)\s+(?:AS\s+)?([A-Z_][A-Z0-9_$]*)\b",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Compiled);
    private static readonly Regex _simpleAliasTokenRegex = new(
        @"^[A-Z_][A-Z0-9_$]*$",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Compiled);
    private static readonly HashSet<string> _sqlAliasReservedTokens = new(StringComparer.OrdinalIgnoreCase)
    {
        "SELECT","FROM","JOIN","INNER","LEFT","RIGHT","FULL","CROSS","OUTER","APPLY","ON","WHERE","GROUP","BY","ORDER","HAVING","LIMIT","OFFSET","UNION","ALL","AS","USING","WHEN","THEN","ELSE","END"
    };

    internal static string Build(string operation, string? subquerySql, AstQueryExecutorBase.EvalRow row)
    {
        var normalizedSubquerySql = GetOrNormalizeSubquerySql(operation, subquerySql);
        var sb = new StringBuilder();
        AppendCorrelatedSubqueryCacheKeyPrefix(sb, operation, normalizedSubquerySql);
        AppendCorrelatedSubqueryCacheKeyFields(sb, GetCorrelatedSubqueryCacheFields(subquerySql ?? string.Empty, row));

        return sb.ToString();
    }

    private static string BuildNormalizedCorrelatedSubquerySql(string operation, string? subquerySql)
    {
        var normalizedSubquerySql = NormalizeSubquerySqlForCacheKey(subquerySql ?? string.Empty);
        return NormalizeOperationSpecificSubquerySqlForCacheKey(operation, normalizedSubquerySql);
    }

    private static void AppendCorrelatedSubqueryCacheKeyPrefix(
        StringBuilder sb,
        string operation,
        string normalizedSubquerySql)
    {
        sb.Append(operation);
        sb.Append('\u001F');
        sb.Append(normalizedSubquerySql);
        sb.Append('\u001F');
    }

    private static void AppendCorrelatedSubqueryCacheKeyFields(
        StringBuilder sb,
        IReadOnlyList<KeyValuePair<string, object?>> cacheFields)
    {
        foreach (var kv in cacheFields)
        {
            sb.Append(kv.Key);
            sb.Append('=');
            sb.Append(NormalizeSubqueryCacheValue(kv.Value));
            sb.Append('\u001E');
        }
    }

    /// <summary>
    /// EN: Applies operation-specific canonicalization rules for subquery SQL used in correlated cache keys.
    /// PT: Aplica regras de canonização específicas por operação para SQL de subquery usado em chaves de cache correlacionado.
    /// </summary>
    private static string NormalizeOperationSpecificSubquerySqlForCacheKey(
        string operation,
        string normalizedSubquerySql)
    {
        if (string.IsNullOrWhiteSpace(normalizedSubquerySql))
            return string.Empty;

        if (string.Equals(operation, "EXISTS", StringComparison.OrdinalIgnoreCase))
            return NormalizeExistsProjectionPayloadForCacheKey(normalizedSubquerySql);

        if (string.Equals(operation, "IN", StringComparison.OrdinalIgnoreCase)
            || string.Equals(operation, "SCALAR", StringComparison.OrdinalIgnoreCase)
            || operation.StartsWith("QANY_", StringComparison.OrdinalIgnoreCase)
            || operation.StartsWith("QALL_", StringComparison.OrdinalIgnoreCase))
            return NormalizeSelectProjectionAliasesForCacheKey(normalizedSubquerySql);

        return normalizedSubquerySql;
    }

    /// <summary>
    /// EN: Selects relevant outer-row fields for correlated subquery cache keys, prioritizing identifiers explicitly referenced in subquery SQL.
    /// PT: Seleciona campos relevantes da linha externa para chaves de cache de subquery correlacionada, priorizando identificadores explicitamente referenciados no SQL da subquery.
    /// </summary>
    private static IReadOnlyList<KeyValuePair<string, object?>> GetCorrelatedSubqueryCacheFields(
        string subquerySql,
        AstQueryExecutorBase.EvalRow row)
    {
        var allFields = GetOrderedCorrelatedSubqueryCacheFields(row);

        if (allFields.Count == 0 || string.IsNullOrWhiteSpace(subquerySql))
            return allFields;

        var normalizedSql = NormalizeSqlIdentifierSpacing(subquerySql);
        var qualifiedMatches = GetQualifiedCorrelatedSubqueryCacheFieldMatches(allFields, normalizedSql);

        if (qualifiedMatches.Count > 0)
            return qualifiedMatches;

        var unqualifiedMatches = GetUnqualifiedCorrelatedSubqueryCacheFieldMatches(allFields, normalizedSql);

        if (unqualifiedMatches.Count > 0)
            return unqualifiedMatches;

        // If we cannot match any outer identifier but SQL still appears to reference outer qualifiers,
        // keep conservative behavior and include all fields to avoid stale cross-row reuse.
        return ContainsPotentialOuterQualifierReference(normalizedSql, allFields)
            ? allFields
            : [];
    }

    private static List<KeyValuePair<string, object?>> GetOrderedCorrelatedSubqueryCacheFields(AstQueryExecutorBase.EvalRow row)
        => row.Fields
            .OrderBy(static kv => kv.Key, StringComparer.OrdinalIgnoreCase)
            .ToList();

    private static List<KeyValuePair<string, object?>> GetQualifiedCorrelatedSubqueryCacheFieldMatches(
        IReadOnlyList<KeyValuePair<string, object?>> allFields,
        string normalizedSql)
    {
        var qualifiedIdentifiers = ExtractQualifiedSqlIdentifiers(normalizedSql);
        return allFields
            .Where(static kv => kv.Key.IndexOf('.') >= 0)
            .Where(kv => qualifiedIdentifiers.Contains(kv.Key))
            .ToList();
    }

    private static List<KeyValuePair<string, object?>> GetUnqualifiedCorrelatedSubqueryCacheFieldMatches(
        IReadOnlyList<KeyValuePair<string, object?>> allFields,
        string normalizedSql)
        => allFields
            .Where(static kv => kv.Key.IndexOf('.') < 0)
            .Where(kv => ContainsSqlIdentifierToken(normalizedSql, kv.Key))
            .ToList();

    /// <summary>
    /// EN: Checks whether a candidate identifier token appears in SQL text using lightweight identifier-boundary guards.
    /// PT: Verifica se um token identificador candidato aparece no texto SQL usando guardas leves de fronteira de identificador.
    /// </summary>
    private static bool ContainsSqlIdentifierToken(string sql, string token)
    {
        if (string.IsNullOrWhiteSpace(sql) || string.IsNullOrWhiteSpace(token))
            return false;

        var index = 0;
        while (true)
        {
            index = sql.IndexOf(token, index, StringComparison.OrdinalIgnoreCase);
            if (index < 0)
                return false;

            var leftBoundaryOk = index == 0 || !IsSqlIdentifierChar(sql[index - 1]);
            var right = index + token.Length;
            var rightBoundaryOk = right >= sql.Length || !IsSqlIdentifierChar(sql[right]);

            if (leftBoundaryOk && rightBoundaryOk)
                return true;

            index = right;
        }
    }

    /// <summary>
    /// EN: Determines whether a character can participate in SQL identifiers when evaluating token boundaries.
    /// PT: Determina se um caractere pode participar de identificadores SQL ao avaliar fronteiras de token.
    /// </summary>
    private static bool IsSqlIdentifierChar(char c)
        => char.IsLetterOrDigit(c) || c is '_' or '$';

    /// <summary>
    /// EN: Extracts qualified identifier tokens (alias.column) from SQL text using lightweight lexical boundaries.
    /// PT: Extrai tokens de identificador qualificado (alias.coluna) do texto SQL usando fronteiras léxicas leves.
    /// </summary>
    private static HashSet<string> ExtractQualifiedSqlIdentifiers(string sql)
    {
        var identifiers = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        if (string.IsNullOrWhiteSpace(sql))
            return identifiers;

        var matches = _qualifiedSqlIdentifierRegex.Matches(sql);

        foreach (Match match in matches)
        {
            if (match.Success && match.Groups.Count > 1)
                identifiers.Add(match.Groups[1].Value);
        }

        return identifiers;
    }

    /// <summary>
    /// EN: Detects whether SQL text appears to reference any qualifier from outer-row fields, even when full token matching failed.
    /// PT: Detecta se o texto SQL parece referenciar algum qualificador dos campos da linha externa, mesmo quando o matching completo de token falha.
    /// </summary>
    private static bool ContainsPotentialOuterQualifierReference(
        string sql,
        IReadOnlyList<KeyValuePair<string, object?>> fields)
    {
        if (string.IsNullOrWhiteSpace(sql) || fields.Count == 0)
            return false;

        var qualifiers = fields
            .Select(static kv =>
            {
                var dot = kv.Key.IndexOf('.');
                return dot > 0 ? kv.Key[..dot] : null;
            })
            .Where(static q => !string.IsNullOrWhiteSpace(q))
            .Distinct(StringComparer.OrdinalIgnoreCase);

        foreach (var qualifier in qualifiers)
        {
            if (Regex.IsMatch(
                    sql,
                    $@"(?<![A-Za-z0-9_$]){Regex.Escape(qualifier!)}\.",
                    RegexOptions.IgnoreCase | RegexOptions.CultureInvariant))
                return true;
        }

        return false;
    }

    /// <summary>
    /// EN: Normalizes SQL text by collapsing optional whitespace around dot separators in qualified identifiers.
    /// PT: Normaliza texto SQL colapsando espaços opcionais ao redor de separadores com ponto em identificadores qualificados.
    /// </summary>
    private static string NormalizeSqlIdentifierSpacing(string sql)
        => string.IsNullOrWhiteSpace(sql)
            ? string.Empty
            : Regex.Replace(sql, @"\s*\.\s*", ".", RegexOptions.CultureInvariant);

    /// <summary>
    /// EN: Canonicalizes subquery SQL text for cache-key usage by normalizing identifier spacing, keyword casing and redundant whitespace while preserving string literals.
    /// PT: Canoniza o texto SQL da subquery para uso na chave de cache normalizando espaçamento de identificadores, casing de palavras-chave e whitespace redundante preservando literais de texto.
    /// </summary>
    private static string NormalizeSubquerySqlForCacheKey(string sql)
    {
        if (string.IsNullOrWhiteSpace(sql))
            return string.Empty;

        var normalized = NormalizeSqlIdentifierSpacing(sql);
        var sb = new StringBuilder(normalized.Length);
        var previousWasSpace = false;

        for (var i = 0; i < normalized.Length; i++)
        {
            if (TryAppendProtectedSqlSegment(normalized, ref i, sb))
            {
                previousWasSpace = false;
                continue;
            }

            var ch = normalized[i];

            if (char.IsWhiteSpace(ch))
            {
                if (!previousWasSpace)
                {
                    sb.Append(' ');
                    previousWasSpace = true;
                }

                continue;
            }

            sb.Append(char.ToUpperInvariant(ch));
            previousWasSpace = false;
        }

        var canonicalSql = sb.ToString().Trim();
        canonicalSql = NormalizeRelationalOperatorSpacingForCacheKey(canonicalSql);
        canonicalSql = NormalizeSubqueryLocalAliasesForCacheKey(canonicalSql);
        return NormalizeCommutativeAndClausesForCacheKey(canonicalSql);
    }

    /// <summary>
    /// EN: Appends a SQL quoted segment handling escaped quote doubles and returns the consumed end index.
    /// PT: Anexa um segmento SQL entre aspas tratando escape por duplicidade de aspas e retorna o índice final consumido.
    /// </summary>
    private static int AppendQuotedSegment(
        string sql,
        int startIndex,
        char quoteChar,
        StringBuilder sb)
    {
        sb.Append(quoteChar);

        var i = startIndex + 1;
        while (i < sql.Length)
        {
            var ch = sql[i];
            sb.Append(sql[i]);

            if (ch == quoteChar)
            {
                var hasEscapedQuote = i + 1 < sql.Length && sql[i + 1] == quoteChar;
                if (hasEscapedQuote)
                {
                    sb.Append(sql[i + 1]);
                    i += 2;
                    continue;
                }

                return i;
            }

            i++;
        }

        return sql.Length - 1;
    }

    /// <summary>
    /// EN: Appends a SQL bracket-identifier segment and returns the consumed end index.
    /// PT: Anexa um segmento SQL de identificador entre colchetes e retorna o índice final consumido.
    /// </summary>
    private static int AppendBracketIdentifierSegment(
        string sql,
        int startIndex,
        StringBuilder sb)
    {
        sb.Append('[');

        var i = startIndex + 1;
        while (i < sql.Length)
        {
            var ch = sql[i];
            sb.Append(sql[i]);
            if (ch == ']')
                return i;
            i++;
        }

        return sql.Length - 1;
    }

    private static bool TryAppendProtectedSqlSegment(
        string sql,
        ref int index,
        StringBuilder sb)
    {
        if (index < 0 || index >= sql.Length)
            return false;

        var ch = sql[index];
        if (ch is '\'' or '"' or '`')
        {
            index = AppendQuotedSegment(sql, index, ch, sb);
            return true;
        }

        if (ch != '[')
            return false;

        index = AppendBracketIdentifierSegment(sql, index, sb);
        return true;
    }

    /// <summary>
    /// EN: Normalizes local aliases declared inside the subquery so semantically equivalent aliases generate the same cache-key SQL fragment.
    /// PT: Normaliza aliases locais declarados dentro da subquery para que aliases semanticamente equivalentes gerem o mesmo fragmento SQL da chave de cache.
    /// </summary>
    private static string NormalizeSubqueryLocalAliasesForCacheKey(string sql)
    {
        if (string.IsNullOrWhiteSpace(sql))
            return string.Empty;

        var aliasMap = ExtractSubqueryAliasMap(sql);
        if (aliasMap.Count == 0)
            return sql;

        return ApplySubqueryAliasMapForCacheKey(sql, aliasMap);
    }

    private static string ApplySubqueryAliasMapForCacheKey(
        string sql,
        IReadOnlyDictionary<string, string> aliasMap)
    {
        var normalized = sql;
        foreach (var aliasPair in aliasMap)
        {
            normalized = ReplaceAliasDeclarationForCacheKey(normalized, aliasPair.Key, aliasPair.Value);
            normalized = ReplaceAliasQualifierReferencesForCacheKey(normalized, aliasPair.Key, aliasPair.Value);
        }

        return normalized;
    }

    /// <summary>
    /// EN: Extracts a deterministic map of local alias names declared in FROM/JOIN clauses to canonical placeholders.
    /// PT: Extrai um mapa determinístico de nomes de aliases locais declarados em cláusulas FROM/JOIN para placeholders canônicos.
    /// </summary>
    private static IReadOnlyDictionary<string, string> ExtractSubqueryAliasMap(string sql)
    {
        var map = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        if (string.IsNullOrWhiteSpace(sql))
            return map;

        var matches = _subqueryAliasDeclarationRegex.Matches(sql);

        foreach (Match match in matches)
        {
            if (!match.Success || match.Groups.Count < 2)
                continue;

            var alias = match.Groups[1].Value;
            if (string.IsNullOrWhiteSpace(alias) || _sqlAliasReservedTokens.Contains(alias))
                continue;

            if (!map.ContainsKey(alias))
                map[alias] = $"T{map.Count + 1}";
        }

        return map;
    }

    /// <summary>
    /// EN: Rewrites FROM/JOIN alias declarations to canonical placeholders for cache-key normalization.
    /// PT: Reescreve declarações de alias em FROM/JOIN para placeholders canônicos na normalização da chave de cache.
    /// </summary>
    private static string ReplaceAliasDeclarationForCacheKey(
        string sql,
        string alias,
        string replacementAlias)
    {
        if (string.IsNullOrWhiteSpace(sql) || string.IsNullOrWhiteSpace(alias))
            return sql;

        var pattern =
            $@"(?<![A-Z0-9_$])(?<kw>FROM|JOIN|APPLY)\s+(?<table>[A-Z_][A-Z0-9_$]*(?:\.[A-Z_][A-Z0-9_$]*)*)\s+(?:AS\s+)?" +
            $@"{Regex.Escape(alias)}(?![A-Z0-9_$])";

        return Regex.Replace(
            sql,
            pattern,
            m => $"{m.Groups["kw"].Value} {m.Groups["table"].Value} {replacementAlias}",
            RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);
    }

    /// <summary>
    /// EN: Rewrites alias-qualified references (alias.column) outside quoted segments to canonical placeholders for cache-key normalization.
    /// PT: Reescreve referências qualificadas por alias (alias.coluna) fora de segmentos entre aspas para placeholders canônicos na normalização da chave de cache.
    /// </summary>
    private static string ReplaceAliasQualifierReferencesForCacheKey(
        string sql,
        string alias,
        string replacementAlias)
    {
        if (string.IsNullOrWhiteSpace(sql) || string.IsNullOrWhiteSpace(alias))
            return sql;

        var sb = new StringBuilder(sql.Length);

        for (var i = 0; i < sql.Length; i++)
        {
            if (TryAppendProtectedSqlSegment(sql, ref i, sb))
                continue;

            if (IsAliasQualifierReferenceAt(sql, i, alias))
            {
                sb.Append(replacementAlias);
                sb.Append('.');
                i += alias.Length;
                continue;
            }

            sb.Append(sql[i]);
        }

        return sb.ToString();
    }

    /// <summary>
    /// EN: Checks whether the SQL text at a given index starts with an alias qualifier reference (alias followed by dot) respecting identifier boundaries.
    /// PT: Verifica se o texto SQL em um índice inicia uma referência de qualificador por alias (alias seguido de ponto) respeitando fronteiras de identificador.
    /// </summary>
    private static bool IsAliasQualifierReferenceAt(
        string sql,
        int startIndex,
        string alias)
    {
        if (startIndex < 0 || startIndex >= sql.Length || string.IsNullOrWhiteSpace(alias))
            return false;

        if (startIndex + alias.Length >= sql.Length)
            return false;

        if (startIndex > 0 && IsSqlIdentifierChar(sql[startIndex - 1]))
            return false;

        for (var i = 0; i < alias.Length; i++)
        {
            if (char.ToUpperInvariant(sql[startIndex + i]) != char.ToUpperInvariant(alias[i]))
                return false;
        }

        return sql[startIndex + alias.Length] == '.';
    }

    /// <summary>
    /// EN: Normalizes spacing around top-level relational operators outside quoted segments so semantically equivalent operator formatting maps to the same cache-key SQL.
    /// PT: Normaliza espaçamento ao redor de operadores relacionais no topo fora de segmentos entre aspas para que formatações equivalentes mapeiem para o mesmo SQL de chave de cache.
    /// </summary>
    private static string NormalizeRelationalOperatorSpacingForCacheKey(string sql)
    {
        if (string.IsNullOrWhiteSpace(sql))
            return string.Empty;

        var sb = new StringBuilder(sql.Length + 16);

        for (var i = 0; i < sql.Length; i++)
        {
            if (TryAppendProtectedSqlSegment(sql, ref i, sb))
                continue;

            var ch = sql[i];

            if (!TryReadRelationalOperator(sql, i, out var op, out var opLength))
            {
                sb.Append(ch);
                continue;
            }

            TrimTrailingSpaces(sb);
            if (sb.Length > 0)
                sb.Append(' ');

            sb.Append(op);
            sb.Append(' ');
            i += opLength - 1;
        }

        return CollapseWhitespaceOutsideQuotedSegments(sb.ToString()).Trim();
    }

    /// <summary>
    /// EN: Tries to read a relational comparison operator at the current index, including two-character variants.
    /// PT: Tenta ler um operador relacional de comparação no índice atual, incluindo variantes de dois caracteres.
    /// </summary>
    private static bool TryReadRelationalOperator(
        string sql,
        int startIndex,
        out string op,
        out int opLength)
    {
        op = string.Empty;
        opLength = 0;

        if (string.IsNullOrWhiteSpace(sql) || startIndex < 0 || startIndex >= sql.Length)
            return false;

        var ch = sql[startIndex];
        var next = startIndex + 1 < sql.Length ? sql[startIndex + 1] : '\0';

        if (ch == '<' && next == '=')
        {
            op = "<=";
            opLength = 2;
            return true;
        }

        if (ch == '>' && next == '=')
        {
            op = ">=";
            opLength = 2;
            return true;
        }

        if (ch == '<' && next == '>')
        {
            op = "<>";
            opLength = 2;
            return true;
        }

        if (ch == '!' && next == '=')
        {
            op = "!=";
            opLength = 2;
            return true;
        }

        if (ch is '=' or '<' or '>')
        {
            op = ch.ToString();
            opLength = 1;
            return true;
        }

        return false;
    }

    /// <summary>
    /// EN: Trims trailing spaces from a StringBuilder buffer.
    /// PT: Remove espaços à direita de um buffer StringBuilder.
    /// </summary>
    private static void TrimTrailingSpaces(StringBuilder sb)
    {
        while (sb.Length > 0 && sb[^1] == ' ')
            sb.Length--;
    }

    /// <summary>
    /// EN: Collapses repeated whitespace outside quoted or bracket-delimited segments while preserving inner literal content.
    /// PT: Colapsa whitespace repetido fora de segmentos entre aspas ou delimitados por colchetes preservando o conteúdo interno de literais.
    /// </summary>
    private static string CollapseWhitespaceOutsideQuotedSegments(string sql)
    {
        if (string.IsNullOrWhiteSpace(sql))
            return string.Empty;

        var sb = new StringBuilder(sql.Length);
        var previousWasSpace = false;

        for (var i = 0; i < sql.Length; i++)
        {
            if (TryAppendProtectedSqlSegment(sql, ref i, sb))
            {
                previousWasSpace = false;
                continue;
            }

            var ch = sql[i];

            if (!char.IsWhiteSpace(ch))
            {
                sb.Append(ch);
                previousWasSpace = false;
                continue;
            }

            if (previousWasSpace)
                continue;

            sb.Append(' ');
            previousWasSpace = true;
        }

        return sb.ToString();
    }

    /// <summary>
    /// EN: Canonicalizes top-level EXISTS subquery projection payload by replacing SELECT list with a fixed token while preserving relational clauses.
    /// PT: Canoniza o payload de projeção de subquery EXISTS no nível de topo substituindo a lista do SELECT por token fixo preservando cláusulas relacionais.
    /// </summary>
    private static string NormalizeExistsProjectionPayloadForCacheKey(string sql)
    {
        return RewriteTopLevelSelectPayloadForCacheKey(sql, static _ => "<EXISTS_PAYLOAD>");
    }

    /// <summary>
    /// EN: Canonicalizes top-level SELECT projection aliases by removing explicit AS aliases while preserving projection expressions and relational clauses.
    /// PT: Canoniza aliases da projeção SELECT no nível de topo removendo aliases explícitos AS e preservando expressões projetadas e cláusulas relacionais.
    /// </summary>
    private static string NormalizeSelectProjectionAliasesForCacheKey(string sql)
    {
        return RewriteTopLevelSelectPayloadForCacheKey(sql, NormalizeSelectListAliasesForCacheKey);
    }

    private static string RewriteTopLevelSelectPayloadForCacheKey(
        string sql,
        Func<string, string> rewritePayload)
    {
        if (string.IsNullOrWhiteSpace(sql))
            return string.Empty;

        if (!TryGetTopLevelSelectPayloadRange(sql, out var afterSelect, out var fromIndex))
            return sql;

        return string.Concat(
            sql.Substring(0, afterSelect),
            " ",
            rewritePayload(sql[afterSelect..fromIndex]),
            " ",
            sql.Substring(fromIndex));
    }

    private static bool TryGetTopLevelSelectPayloadRange(
        string sql,
        out int afterSelect,
        out int fromIndex)
    {
        afterSelect = -1;
        fromIndex = -1;
        if (string.IsNullOrWhiteSpace(sql))
            return false;

        if (!TryFindTopLevelKeywordIndex(sql, "SELECT", 0, out var selectIndex))
            return false;

        afterSelect = selectIndex + "SELECT".Length;
        if (!TryFindTopLevelKeywordIndex(sql, "FROM", afterSelect, out fromIndex))
            return false;

        return fromIndex > afterSelect;
    }

    /// <summary>
    /// EN: Normalizes explicit AS aliases from a top-level SELECT list while preserving nested expressions.
    /// PT: Normaliza aliases explícitos AS de uma lista SELECT de topo preservando expressões aninhadas.
    /// </summary>
    private static string NormalizeSelectListAliasesForCacheKey(string selectList)
    {
        if (string.IsNullOrWhiteSpace(selectList))
            return string.Empty;

        var segments = SplitTopLevelCommaSegments(selectList);
        if (segments.Count == 0)
            return selectList.Trim();

        for (var i = 0; i < segments.Count; i++)
            segments[i] = RemoveExplicitAsAliasFromSelectExpression(segments[i]);

        return string.Join(", ", segments);
    }

    /// <summary>
    /// EN: Splits text by top-level comma separators outside quoted segments and nested parentheses.
    /// PT: Divide o texto por vírgulas de topo fora de segmentos entre aspas e parênteses aninhados.
    /// </summary>
    private static List<string> SplitTopLevelCommaSegments(string text)
    {
        var segments = new List<string>();
        if (string.IsNullOrWhiteSpace(text))
            return segments;

        var start = 0;
        var depth = 0;
        for (var i = 0; i < text.Length; i++)
        {
            if (TrySkipProtectedSqlSegment(text, ref i))
                continue;

            var ch = text[i];

            if (ch == '(')
            {
                depth++;
                continue;
            }

            if (ch == ')' && depth > 0)
            {
                depth--;
                continue;
            }

            if (depth == 0 && ch == ',')
            {
                var segment = text[start..i].Trim();
                if (segment.Length > 0)
                    segments.Add(segment);
                start = i + 1;
            }
        }

        var last = text[start..].Trim();
        if (last.Length > 0)
            segments.Add(last);

        return segments;
    }

    /// <summary>
    /// EN: Removes a trailing explicit AS alias from a SELECT expression when alias syntax is valid.
    /// PT: Remove alias explícito AS ao final de uma expressão SELECT quando a sintaxe do alias é válida.
    /// </summary>
    private static string RemoveExplicitAsAliasFromSelectExpression(string expression)
    {
        if (string.IsNullOrWhiteSpace(expression))
            return string.Empty;

        var trimmed = expression.Trim();
        if (!TrySplitExplicitAsAlias(trimmed, out var beforeAs, out var aliasPart)
            || !IsValidExplicitAliasToken(aliasPart))
            return trimmed;

        return beforeAs;
    }

    private static bool TrySplitExplicitAsAlias(
        string expression,
        out string beforeAs,
        out string aliasPart)
    {
        beforeAs = string.Empty;
        aliasPart = string.Empty;
        if (string.IsNullOrWhiteSpace(expression))
            return false;

        if (!TryFindTopLevelKeywordIndex(expression, "AS", 0, out var asIndex))
            return false;

        beforeAs = expression[..asIndex].TrimEnd();
        aliasPart = expression[(asIndex + 2)..].Trim();
        return true;
    }

    /// <summary>
    /// EN: Validates whether an alias token matches supported explicit alias forms (identifier or quoted identifier).
    /// PT: Valida se um token de alias corresponde às formas suportadas de alias explícito (identificador ou identificador entre delimitadores).
    /// </summary>
    private static bool IsValidExplicitAliasToken(string aliasToken)
    {
        if (string.IsNullOrWhiteSpace(aliasToken))
            return false;

        var trimmed = aliasToken.Trim();
        if (_simpleAliasTokenRegex.IsMatch(trimmed))
            return true;

        if (trimmed.Length >= 2 && trimmed[0] == '[' && trimmed[^1] == ']')
            return true;

        if (trimmed.Length >= 2 && trimmed[0] == '"' && trimmed[^1] == '"')
            return true;

        if (trimmed.Length >= 2 && trimmed[0] == '`' && trimmed[^1] == '`')
            return true;

        return false;
    }

    /// <summary>
    /// EN: Tries to find a top-level SQL keyword index outside quoted segments and nested parentheses, starting from a given position.
    /// PT: Tenta localizar o índice de uma palavra-chave SQL no topo fora de segmentos entre aspas e parênteses aninhados, iniciando em uma posição informada.
    /// </summary>
    private static bool TryFindTopLevelKeywordIndex(
        string sql,
        string keyword,
        int startIndex,
        out int keywordIndex)
    {
        keywordIndex = -1;
        if (string.IsNullOrWhiteSpace(sql) || string.IsNullOrWhiteSpace(keyword))
            return false;

        var safeStart = startIndex;
        if (safeStart < 0)
            safeStart = 0;
        else if (safeStart > sql.Length)
            safeStart = sql.Length;
        var depth = 0;
        for (var i = safeStart; i < sql.Length; i++)
        {
            if (TrySkipProtectedSqlSegment(sql, ref i))
                continue;

            var ch = sql[i];

            if (ch == '(')
            {
                depth++;
                continue;
            }

            if (ch == ')' && depth > 0)
            {
                depth--;
                continue;
            }

            if (depth == 0 && MatchesKeywordTokenAt(sql, i, keyword))
            {
                keywordIndex = i;
                return true;
            }
        }

        return false;
    }

    private static bool TrySkipProtectedSqlSegment(string sql, ref int index)
    {
        if (index < 0 || index >= sql.Length)
            return false;

        var ch = sql[index];
        if (ch is '\'' or '"' or '`')
        {
            index = FindQuotedSegmentEndIndex(sql, index, ch);
            return true;
        }

        if (ch != '[')
            return false;

        index = FindBracketSegmentEndIndex(sql, index);
        return true;
    }

    /// <summary>
    /// EN: Normalizes commutative top-level AND chains in WHERE/HAVING clauses so equivalent predicate orderings reuse the same cache-key SQL fragment.
    /// PT: Normaliza cadeias comutativas de AND no topo em cláusulas WHERE/HAVING para que ordenações equivalentes de predicados reutilizem o mesmo fragmento SQL da chave de cache.
    /// </summary>
    private static string NormalizeCommutativeAndClausesForCacheKey(string sql)
    {
        if (string.IsNullOrWhiteSpace(sql))
            return string.Empty;

        var normalizedWhere = RewritePredicateClauseForCacheKey(sql, _cacheKeyWherePredicateRegex, "WHERE");
        return RewritePredicateClauseForCacheKey(normalizedWhere, _cacheKeyHavingPredicateRegex, "HAVING");
    }

    private static string RewritePredicateClauseForCacheKey(
        string sql,
        Regex clauseRegex,
        string clauseKeyword)
        => clauseRegex.Replace(
            sql,
            match => $"{clauseKeyword} {NormalizeTopLevelAndPredicateForCacheKey(match.Groups["predicate"].Value)}");

    /// <summary>
    /// EN: Canonicalizes a predicate text by sorting top-level AND segments when safe (no top-level OR and no BETWEEN token).
    /// PT: Canoniza um texto de predicado ordenando segmentos AND de topo quando seguro (sem OR de topo e sem token BETWEEN).
    /// </summary>
    private static string NormalizeTopLevelAndPredicateForCacheKey(string predicate)
    {
        if (string.IsNullOrWhiteSpace(predicate))
            return string.Empty;

        var trimmedPredicate = TrimRedundantOuterParentheses(predicate);
        if (!ShouldNormalizeTopLevelAndPredicateForCacheKey(trimmedPredicate))
            return trimmedPredicate;

        var segments = SplitTopLevelAndSegments(trimmedPredicate);
        return JoinNormalizedTopLevelAndSegments(trimmedPredicate, segments);
    }

    private static bool ShouldNormalizeTopLevelAndPredicateForCacheKey(string predicate)
        => !string.IsNullOrWhiteSpace(predicate)
            && !ContainsTokenOutsideQuotedSegments(predicate, "OR")
            && !ContainsTokenOutsideQuotedSegments(predicate, "BETWEEN");

    private static string JoinNormalizedTopLevelAndSegments(
        string originalPredicate,
        List<string> segments)
    {
        if (segments.Count <= 1)
            return originalPredicate;

        segments.Sort(StringComparer.Ordinal);
        return string.Join(" AND ", segments);
    }

    /// <summary>
    /// EN: Splits predicate text by top-level AND operators outside quoted segments and nested parentheses.
    /// PT: Divide o texto do predicado por operadores AND de topo fora de segmentos entre aspas e parênteses aninhados.
    /// </summary>
    private static List<string> SplitTopLevelAndSegments(string predicate)
    {
        var segments = new List<string>();
        if (string.IsNullOrWhiteSpace(predicate))
            return segments;

        var start = 0;
        var depth = 0;

        for (var i = 0; i < predicate.Length; i++)
        {
            if (TrySkipProtectedSqlSegment(predicate, ref i))
                continue;

            var ch = predicate[i];

            if (ch == '(')
            {
                depth++;
                continue;
            }

            if (ch == ')' && depth > 0)
            {
                depth--;
                continue;
            }

            if (depth == 0 && MatchesKeywordTokenAt(predicate, i, "AND"))
            {
                AppendNormalizedPredicateSegment(segments, predicate[start..i]);
                start = i + 3;
                i += 2;
            }
        }

        AppendNormalizedPredicateSegment(segments, predicate[start..]);

        return segments;
    }

    private static void AppendNormalizedPredicateSegment(List<string> segments, string rawSegment)
    {
        var segment = NormalizePredicateSegmentForCacheKey(rawSegment);
        if (segment.Length > 0)
            segments.Add(segment);
    }

    /// <summary>
    /// EN: Normalizes an individual predicate segment by trimming redundant outer parentheses and canonicalizing simple commutative equalities.
    /// PT: Normaliza um segmento individual de predicado removendo parênteses externos redundantes e canonizando igualdades comutativas simples.
    /// </summary>
    private static string NormalizePredicateSegmentForCacheKey(string segment)
    {
        var trimmedSegment = TrimRedundantOuterParentheses(segment);
        if (string.IsNullOrWhiteSpace(trimmedSegment))
            return string.Empty;

        return NormalizeCommutativeEqualitySegmentForCacheKey(trimmedSegment);
    }

    /// <summary>
    /// EN: Canonicalizes a simple top-level equality segment (`lhs = rhs`) by sorting operands lexicographically when safe.
    /// PT: Canoniza um segmento de igualdade simples no topo (`lhs = rhs`) ordenando operandos lexicograficamente quando seguro.
    /// </summary>
    private static string NormalizeCommutativeEqualitySegmentForCacheKey(string segment)
    {
        if (string.IsNullOrWhiteSpace(segment))
            return string.Empty;

        if (!TrySplitStandaloneTopLevelEqualityOperands(segment, out var left, out var right))
            return segment;

        return BuildOrderedEqualitySegmentForCacheKey(left, right);
    }

    private static bool TrySplitStandaloneTopLevelEqualityOperands(
        string segment,
        out string left,
        out string right)
    {
        left = string.Empty;
        right = string.Empty;
        if (!TryFindStandaloneTopLevelEqualityOperator(segment, out var equalityIndex))
            return false;

        left = TrimRedundantOuterParentheses(segment[..equalityIndex]);
        right = TrimRedundantOuterParentheses(segment[(equalityIndex + 1)..]);
        return left.Length > 0 && right.Length > 0;
    }

    private static string BuildOrderedEqualitySegmentForCacheKey(string left, string right)
        => StringComparer.Ordinal.Compare(left, right) <= 0
            ? $"{left} = {right}"
            : $"{right} = {left}";

    /// <summary>
    /// EN: Tries to find a single standalone top-level equality operator, excluding composite comparisons such as less-or-equal, greater-or-equal, different and double-equals.
    /// PT: Tenta localizar um único operador de igualdade isolado no topo, excluindo comparações compostas como menor-ou-igual, maior-ou-igual, diferente e igualdade dupla.
    /// </summary>
    private static bool TryFindStandaloneTopLevelEqualityOperator(string segment, out int equalityIndex)
    {
        equalityIndex = -1;
        if (string.IsNullOrWhiteSpace(segment))
            return false;

        var depth = 0;
        for (var i = 0; i < segment.Length; i++)
        {
            if (TrySkipProtectedSqlSegment(segment, ref i))
                continue;

            var ch = segment[i];

            if (ch == '(')
            {
                depth++;
                continue;
            }

            if (ch == ')' && depth > 0)
            {
                depth--;
                continue;
            }

            if (depth != 0 || ch != '=')
                continue;

            var previous = i > 0 ? segment[i - 1] : '\0';
            var next = i + 1 < segment.Length ? segment[i + 1] : '\0';

            var isCompositeComparison = previous is '<' or '>' or '!' or '='
                                      || next is '<' or '>' or '!' or '=';
            if (isCompositeComparison)
                continue;

            if (equalityIndex >= 0)
                return false;

            equalityIndex = i;
        }

        return equalityIndex >= 0;
    }

    /// <summary>
    /// EN: Removes redundant outer parentheses that wrap the full expression while preserving inner structure.
    /// PT: Remove parênteses externos redundantes que envolvem a expressão inteira preservando a estrutura interna.
    /// </summary>
    private static string TrimRedundantOuterParentheses(string expression)
    {
        if (string.IsNullOrWhiteSpace(expression))
            return string.Empty;

        var trimmed = expression.Trim();
        while (trimmed.Length >= 2 && trimmed[0] == '(' && trimmed[^1] == ')')
        {
            if (!HasSingleOuterParenthesesWrappingWholeExpression(trimmed))
                break;

            trimmed = trimmed[1..^1].Trim();
        }

        return trimmed;
    }

    /// <summary>
    /// EN: Checks whether the first and last parentheses wrap the whole expression without closing earlier at top level.
    /// PT: Verifica se o primeiro e o último parêntese envolvem toda a expressão sem fechar antes no nível de topo.
    /// </summary>
    private static bool HasSingleOuterParenthesesWrappingWholeExpression(string expression)
    {
        if (string.IsNullOrWhiteSpace(expression) || expression.Length < 2)
            return false;

        if (expression[0] != '(' || expression[^1] != ')')
            return false;

        var depth = 0;
        for (var i = 0; i < expression.Length; i++)
        {
            if (TrySkipProtectedSqlSegment(expression, ref i))
                continue;

            var ch = expression[i];

            if (ch == '(')
            {
                depth++;
                continue;
            }

            if (ch != ')')
                continue;

            depth--;
            if (depth < 0)
                return false;

            if (depth == 0 && i < expression.Length - 1)
                return false;
        }

        return depth == 0;
    }

    /// <summary>
    /// EN: Detects whether a token appears outside quoted segments and nested parentheses.
    /// PT: Detecta se um token aparece fora de segmentos entre aspas e parênteses aninhados.
    /// </summary>
    private static bool ContainsTokenOutsideQuotedSegments(string sql, string token)
        => TryFindTopLevelKeywordIndex(sql, token, 0, out _);

    /// <summary>
    /// EN: Matches a SQL keyword token at a position ensuring identifier boundaries.
    /// PT: Compara um token de palavra-chave SQL em uma posição garantindo fronteiras de identificador.
    /// </summary>
    private static bool MatchesKeywordTokenAt(string sql, int startIndex, string token)
    {
        if (string.IsNullOrWhiteSpace(sql) || string.IsNullOrWhiteSpace(token))
            return false;

        if (startIndex < 0 || startIndex + token.Length > sql.Length)
            return false;

        if (startIndex > 0 && IsSqlIdentifierChar(sql[startIndex - 1]))
            return false;

        var endIndex = startIndex + token.Length;
        if (endIndex < sql.Length && IsSqlIdentifierChar(sql[endIndex]))
            return false;

        for (var i = 0; i < token.Length; i++)
        {
            if (char.ToUpperInvariant(sql[startIndex + i]) != char.ToUpperInvariant(token[i]))
                return false;
        }

        return true;
    }

    /// <summary>
    /// EN: Finds the end index of a quoted SQL segment handling escaped doubled quote characters.
    /// PT: Localiza o índice final de um segmento SQL entre aspas tratando escapes por duplicidade de aspas.
    /// </summary>
    private static int FindQuotedSegmentEndIndex(string sql, int startIndex, char quoteChar)
    {
        var i = startIndex + 1;
        while (i < sql.Length)
        {
            if (sql[i] == quoteChar)
            {
                var hasEscapedQuote = i + 1 < sql.Length && sql[i + 1] == quoteChar;
                if (hasEscapedQuote)
                {
                    i += 2;
                    continue;
                }

                return i;
            }

            i++;
        }

        return sql.Length - 1;
    }

    /// <summary>
    /// EN: Finds the end index of a bracket-delimited SQL identifier segment.
    /// PT: Localiza o índice final de um segmento SQL de identificador delimitado por colchetes.
    /// </summary>
    private static int FindBracketSegmentEndIndex(string sql, int startIndex)
    {
        var i = startIndex + 1;
        while (i < sql.Length)
        {
            if (sql[i] == ']')
                return i;
            i++;
        }

        return sql.Length - 1;
    }

    /// <summary>
    /// EN: Normalizes scalar and tuple-like values into stable cache-key fragments for correlated subquery memoization.
    /// PT: Normaliza valores escalares e em formato tupla em fragmentos estáveis de chave de cache para memoização de subquery correlacionada.
    /// </summary>
    private static string NormalizeSubqueryCacheValue(object? value)
    {
        if (value is null || value is DBNull)
            return "<null>";

        if (value is object?[] tuple)
            return "[" + string.Join(",", tuple.Select(NormalizeSubqueryCacheValue)) + "]";

        return Convert.ToString(value, System.Globalization.CultureInfo.InvariantCulture) ?? string.Empty;
    }


    private static string GetOrNormalizeSubquerySql(string operation, string? subquerySql)
    {
        var rawSql = subquerySql ?? string.Empty;
        var cacheKey = string.Concat(operation.ToUpperInvariant(), "\u001F", rawSql);
        return _normalizedSqlCache.GetOrAdd(cacheKey, _ => BuildNormalizedCorrelatedSubquerySql(operation, rawSql));
    }
}
