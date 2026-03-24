namespace DbSqlLikeMem;

internal sealed class SqlQueryParserContext
{
    private static readonly HashSet<string> JoinStart = new(StringComparer.OrdinalIgnoreCase)
    {
        SqlConst.JOIN, SqlConst.INNER, SqlConst.LEFT, SqlConst.RIGHT, SqlConst.CROSS, SqlConst.OUTER
    };

    private static readonly HashSet<string> ClauseKeywordToken = new(StringComparer.OrdinalIgnoreCase)
    {
        SqlConst.FROM,
        SqlConst.WHERE,
        SqlConst.GROUP,
        SqlConst.HAVING,
        SqlConst.ORDER,
        SqlConst.LIMIT,
        SqlConst.UNION,
        SqlConst.ON,
        SqlConst.JOIN,
        SqlConst.INNER,
        SqlConst.LEFT,
        SqlConst.RIGHT,
        SqlConst.CROSS,
        SqlConst.OUTER,
        SqlConst.APPLY,
        SqlConst.OFFSET,
        SqlConst.FETCH,
        SqlConst.OPTION,
        SqlConst.SET,
        SqlConst.VALUES,
        SqlConst.SELECT,
        SqlConst.INTO,
        SqlConst.USING,
        SqlConst.WHEN,
        SqlConst.MATCHED,
        SqlConst.THEN,
        SqlConst.PIVOT,
        SqlConst.UNPIVOT,
        SqlConst.RETURNING
    };

    private readonly IReadOnlyList<SqlToken> _toks;
    private readonly ISqlDialect _dialect;
    private readonly IDataParameterCollection? _parameters;
    private readonly Func<string, bool>? _customFunctionSupported;
    private readonly AutoSqlSyntaxFeatures _autoSyntaxFeatures;
    private readonly Func<string, SqlQueryBase> _parseQuery;
    private readonly Func<string, SqlExpr> _parseScalar;
    private readonly Func<string, SqlExpr> _parseWhere;
    private int _i;

    internal SqlQueryParserContext(
        IReadOnlyList<SqlToken> toks,
        ISqlDialect dialect,
        IDataParameterCollection? parameters,
        Func<string, bool>? customFunctionSupported,
        AutoSqlSyntaxFeatures autoSyntaxFeatures,
        Func<string, SqlQueryBase> parseQuery,
        Func<string, SqlExpr> parseScalar,
        Func<string, SqlExpr> parseWhere)
    {
        _toks = toks ?? throw new ArgumentNullException(nameof(toks));
        _dialect = dialect ?? throw new ArgumentNullException(nameof(dialect));
        _parameters = parameters;
        _customFunctionSupported = customFunctionSupported;
        _autoSyntaxFeatures = autoSyntaxFeatures;
        _parseQuery = parseQuery ?? throw new ArgumentNullException(nameof(parseQuery));
        _parseScalar = parseScalar ?? throw new ArgumentNullException(nameof(parseScalar));
        _parseWhere = parseWhere ?? throw new ArgumentNullException(nameof(parseWhere));
    }

    internal IReadOnlyList<SqlToken> Toks => _toks;

    internal ISqlDialect Dialect => _dialect;

    internal IDataParameterCollection? Parameters => _parameters;

    internal Func<string, bool>? CustomFunctionSupported => _customFunctionSupported;

    internal AutoSqlSyntaxFeatures AutoSyntaxFeatures => _autoSyntaxFeatures;

    internal int Index
    {
        get => _i;
        set => _i = value;
    }

    internal bool AllowInsertSelectSuffixBoundary { get; set; }

    internal SqlQueryBase ParseQuery(string sql) => _parseQuery(sql);

    internal SqlExpr ParseScalar(string sql) => _parseScalar(sql);

    internal SqlExpr ParseWhere(string sql) => _parseWhere(sql);

    internal SqlTableSource ParseTableSource(
        bool consumeHints = true,
        bool allowFunctionSource = true,
        IReadOnlyCollection<string>? aliasStopWords = null)
        => SqlTableSourceParserHelper.ParseTableSource(this, consumeHints, allowFunctionSource, aliasStopWords);

    internal List<string> ParseIdentifierList(string context)
    {
        var identifiers = new List<string>();
        var expectIdentifier = true;

        while (true)
        {
            var token = Peek();
            if (IsEnd(token))
                throw new InvalidOperationException($"{context} was not closed correctly.");

            if (IsSymbol(token, ")"))
            {
                if (expectIdentifier)
                    throw new InvalidOperationException($"{context} cannot end with a comma.");

                break;
            }

            if (expectIdentifier)
            {
                if (IsSymbol(token, ","))
                    throw new InvalidOperationException($"{context} cannot start with a comma.");

                if (token.Kind != SqlTokenKind.Identifier)
                    throw new InvalidOperationException($"{context} expects a column name, found {token.Kind} '{token.Text}'.");

                identifiers.Add(Consume().Text);
                expectIdentifier = false;
                continue;
            }

            if (IsSymbol(token, ","))
            {
                Consume();
                expectIdentifier = true;
                continue;
            }

            throw new InvalidOperationException($"{context} must separate columns with commas.");
        }

        if (identifiers.Count == 0)
            throw new InvalidOperationException($"{context} requires at least one column name.");

        return identifiers;
    }

    internal SqlToken Peek(int offset = 0) => (_i + offset < _toks.Count) ? _toks[_i + offset] : SqlToken.EOF;

    internal SqlToken PeekTokenFrom(int index)
        => (index >= 0 && index < _toks.Count) ? _toks[index] : SqlToken.EOF;

    internal SqlToken Consume() => _toks[_i++];

    internal static bool IsEnd(SqlToken t) => t.Kind == SqlTokenKind.EndOfFile;

    internal bool IsEnd() => IsEnd(Peek());

    internal static bool IsWord(SqlToken t, string w) => t.Text.Equals(w, StringComparison.OrdinalIgnoreCase);

    internal bool IsWord(string w) => IsWord(Peek(), w);
    internal bool IsWord(int offset, string w) => IsWord(Peek(offset), w);

    internal static bool IsSymbol(SqlToken t, string s) => t.Kind == SqlTokenKind.Symbol && t.Text == s;

    internal bool IsSymbol(string s) => IsSymbol(Peek(), s);
    internal bool IsSymbol(int offset, string s) => IsSymbol(Peek(offset), s);

    internal void ExpectWord(string w)
    {
        if (!IsWord(w))
            throw new InvalidOperationException($"Esperava {w}, veio {Peek().Text}");

        Consume();
    }

    internal void ExpectSymbol(string s)
    {
        var t = Peek();
        if (!string.Equals(t.Text, s, StringComparison.Ordinal))
            throw new InvalidOperationException($"Esperava símbolo {s}, veio {t.Text}");

        Consume();
    }

    internal string ExpectIdentifier()
    {
        var t = Consume();
        if (t.Kind == SqlTokenKind.Identifier || t.Kind == SqlTokenKind.Keyword)
            return t.Text;

        throw new InvalidOperationException($"Esperava identifier, veio {t.Kind}");
    }

    internal long ExpectSignedNumberLong(string clauseName)
    {
        var sign = 1L;
        if (IsSymbol(Peek(), "+"))
        {
            Consume();
        }
        else if (IsSymbol(Peek(), "-"))
        {
            Consume();
            sign = -1L;
        }

        var t = Consume();
        if (t.Kind == SqlTokenKind.Number)
            return sign * long.Parse(t.Text, CultureInfo.InvariantCulture);

        if (t.Kind == SqlTokenKind.Parameter)
            return sign * ResolveParameterLong(t.Text);

        throw new InvalidOperationException($"{clauseName} requires an integer literal or parameter.");
    }

    internal int ResolveParameterInt(string parameterToken)
    {
        if (_parameters is null)
            throw new FormatException($"The input string '{parameterToken}' was not in a correct format.");

        var normalized = parameterToken.TrimStart('@', ':', '?');

        foreach (IDataParameter parameter in _parameters)
        {
            var name = (parameter.ParameterName ?? string.Empty).TrimStart('@', ':', '?');
            if (!string.Equals(name, normalized, StringComparison.OrdinalIgnoreCase))
                continue;

            if (parameter.Value is null || parameter.Value == DBNull.Value)
                throw new FormatException($"The input string '{parameterToken}' was not in a correct format.");

            return Convert.ToInt32(parameter.Value, CultureInfo.InvariantCulture);
        }

        throw new FormatException($"The input string '{parameterToken}' was not in a correct format.");
    }

    internal long ResolveParameterLong(string parameterToken)
    {
        if (_parameters is null)
            throw new FormatException($"The input string '{parameterToken}' was not in a correct format.");

        var normalized = parameterToken.TrimStart('@', ':', '?');

        foreach (IDataParameter parameter in _parameters)
        {
            var name = (parameter.ParameterName ?? string.Empty).TrimStart('@', ':', '?');
            if (!string.Equals(name, normalized, StringComparison.OrdinalIgnoreCase))
                continue;

            if (parameter.Value is null || parameter.Value == DBNull.Value)
                throw new FormatException($"The input string '{parameterToken}' was not in a correct format.");

            return Convert.ToInt64(parameter.Value, CultureInfo.InvariantCulture);
        }

        throw new FormatException($"The input string '{parameterToken}' was not in a correct format.");
    }

    internal void EnsureStatementEnd(string statementName)
    {
        if (IsSymbol(Peek(), ";"))
            Consume();

        if (!IsEnd(Peek()))
        {
            var t = Peek();
            throw new InvalidOperationException($"Unexpected token after {statementName}: {t.Kind} '{t.Text}'");
        }
    }

    internal void ExpectEndOrUnionBoundary()
    {
        var t = Peek();
        if (IsEnd(t) || IsWord(t, SqlConst.UNION))
            return;

        if (AllowInsertSelectSuffixBoundary && (IsWord(t, SqlConst.ON) || IsWord(t, SqlConst.RETURNING)))
            return;

        if (IsSymbol(t, ";"))
        {
            Consume();
            return;
        }

        throw new InvalidOperationException($"Token inesperado após SELECT: {t.Kind} '{t.Text}'");
    }

    internal void TryConsumeQueryHintOption()
    {
        if (!IsWord(Peek(), SqlConst.OPTION))
            return;

        if (!_dialect.SupportsSqlServerQueryHints)
            throw SqlUnsupported.ForOptionQueryHints(_dialect);

        Consume();
        _ = ReadBalancedParenRawTokens();
    }

    internal bool HasTopLevelWordInRemaining(string word)
    {
        var depth = 0;
        for (var idx = _i; idx < _toks.Count; idx++)
        {
            var t = _toks[idx];
            if (t.Kind == SqlTokenKind.EndOfFile)
                break;

            if (IsSymbol(t, "("))
            {
                depth++;
                continue;
            }

            if (IsSymbol(t, ")"))
            {
                depth = Math.Max(0, depth - 1);
                continue;
            }

            if (depth == 0 && IsWord(t, word))
                return true;
        }

        return false;
    }

    internal bool HasTopLevelMergeWhenClause()
    {
        var depth = 0;
        for (var idx = _i; idx < _toks.Count; idx++)
        {
            var t = _toks[idx];
            if (t.Kind == SqlTokenKind.EndOfFile)
                break;

            if (IsSymbol(t, "("))
            {
                depth++;
                continue;
            }

            if (IsSymbol(t, ")"))
            {
                depth = Math.Max(0, depth - 1);
                continue;
            }

            if (depth != 0 || !IsWord(t, SqlConst.WHEN))
                continue;

            var next = PeekTokenFrom(idx + 1);
            if (IsWord(next, SqlConst.MATCHED))
                return true;

            if (IsWord(next, SqlConst.NOT) && IsWord(PeekTokenFrom(idx + 2), SqlConst.MATCHED))
                return true;
        }

        return false;
    }

    internal void SkipUntilTopLevelWord(params string[] words)
    {
        if (words is null || words.Length == 0)
            throw new ArgumentException("words vazio", nameof(words));

        var set = new HashSet<string>(words, StringComparer.OrdinalIgnoreCase);

        var depth = 0;
        while (!IsEnd(Peek()))
        {
            var t = Peek();

            if (IsSymbol(t, "(")) { depth++; Consume(); continue; }
            if (IsSymbol(t, ")")) { depth = Math.Max(0, depth - 1); Consume(); continue; }

            if (depth == 0 && t.Kind == SqlTokenKind.Identifier && set.Contains(t.Text))
                return;

            if (depth == 0 && t.Kind == SqlTokenKind.Keyword && set.Contains(t.Text))
                return;

            if (depth == 0 && set.Contains(t.Text))
                return;

            Consume();
        }

        throw new InvalidOperationException($"Não encontrei nenhum destes tokens no nível top-level: {string.Join(", ", words)}");
    }

    internal SqlTableSource ParseQualifiedObjectName()
    {
        var first = ExpectIdentifier();
        string? dbName = null;
        var objectName = first;
        if (IsSymbol(Peek(), "."))
        {
            Consume();
            dbName = objectName;
            objectName = ExpectIdentifier();
        }

        return new SqlTableSource(
            dbName,
            objectName,
            Alias: null,
            Derived: null,
            DerivedUnion: null,
            DerivedSql: null,
            Pivot: null,
            MySqlIndexHints: null);
    }

    internal string? ReadOptionalAlias(IReadOnlyCollection<string>? additionalStopWords = null)
    {
        if (IsWord(Peek(), SqlConst.AS))
        {
            var next = Peek(1);
            if (next.Kind == SqlTokenKind.Identifier && IsClauseKeywordToken(next, additionalStopWords))
                return null;

            Consume();
            return ExpectIdentifier();
        }

        var t = Peek();
        if (t.Kind == SqlTokenKind.Identifier && !IsClauseKeywordToken(t, additionalStopWords))
            return Consume().Text;

        return null;
    }

    internal List<string> ParseCommaSeparatedRawItemsUntilAny(params string[] stopWords)
    {
        var items = new List<string>();
        var buf = new List<SqlToken>();
        var depth = 0;

        while (!IsEnd(Peek()))
        {
            var t = Peek();
            if (IsSymbol(t, "(")) depth++;
            else if (IsSymbol(t, ")")) depth--;

            if (depth == 0 && IsSymbol(t, ";"))
                break;

            if (depth == 0 && ShouldStopAtTopLevelToken(t, stopWords, buf))
                break;

            if (depth == 0 && IsSymbol(t, ","))
            {
                Consume();
                items.Add(TokensToSql(buf));
                buf.Clear();
                continue;
            }

            buf.Add(Consume());
        }

        if (buf.Count > 0)
            items.Add(TokensToSql(buf));

        return items;
    }

    internal string ReadClauseTextUntilTopLevelStop(params string[] stopWords)
    {
        var buf = new List<SqlToken>();
        var depth = 0;

        while (!IsEnd(Peek()))
        {
            var t = Peek();
            if (IsSymbol(t, "(")) depth++;
            else if (IsSymbol(t, ")")) depth--;

            if (depth == 0 && IsSymbol(t, ";"))
                break;

            if (depth == 0 && ShouldStopAtTopLevelToken(t, stopWords, buf))
                break;

            buf.Add(Consume());
        }

        return TokensToSql(buf);
    }

    internal string ReadBalancedParenRawTokens()
    {
        ExpectSymbol("(");
        var depth = 1;
        var buf = new List<SqlToken>();

        while (!IsEnd(Peek()))
        {
            var t = Consume();
            if (IsSymbol(t, "("))
                depth++;
            else if (IsSymbol(t, ")"))
            {
                depth--;
                if (depth == 0)
                    break;
            }

            buf.Add(t);
        }

        if (depth != 0)
            throw new InvalidOperationException("INSERT VALUES row tuple was not closed correctly.");

        return TokensToSql(buf);
    }

    internal string TokensToSql(List<SqlToken> toks)
    {
        var sb = new StringBuilder();
        SqlToken? prev = null;

        foreach (var t in toks)
        {
            var text = t.Kind switch
            {
                SqlTokenKind.String => $"'{EscapeStringLiteral(t.Text)}'",
                SqlTokenKind.Identifier => NeedsIdentifierQuoting(t.Text) ? QuoteIdentifier(t.Text) : t.Text,
                _ => t.Text
            };

            if (sb.Length > 0 && NeedsSpace(prev, t))
                sb.Append(' ');

            sb.Append(text);
            prev = t;
        }

        return sb.ToString().Trim();

        string EscapeStringLiteral(string value)
        {
            if (_dialect.StringEscapeStyle == SqlStringEscapeStyle.backslash)
            {
                return value
                    .Replace("\\", "\\\\")
                    .Replace("'", "\\'");
            }

            return value.Replace("'", "''");
        }

        bool NeedsIdentifierQuoting(string ident)
        {
            if (string.IsNullOrWhiteSpace(ident))
                return true;

            if (_dialect.IsKeyword(ident))
                return true;

            if (!Regex.IsMatch(ident, @"^[A-Za-z_#][A-Za-z0-9_$#]*$", RegexOptions.CultureInvariant))
                return true;

            return ident.Contains(' ')
                   || ident.Contains('\t')
                   || ident.Contains('\n')
                   || ident.Contains('\r');
        }

        string QuoteIdentifier(string ident)
        {
            var style = _dialect.IdentifierEscapeStyle;

            if (style == SqlIdentifierEscapeStyle.double_quote && _dialect.IsStringQuote('"'))
            {
                if (_dialect.AllowsBacktickIdentifiers)
                    style = SqlIdentifierEscapeStyle.backtick;
                else if (_dialect.AllowsBracketIdentifiers)
                    style = SqlIdentifierEscapeStyle.bracket;
            }

            return style switch
            {
                SqlIdentifierEscapeStyle.backtick => $"`{ident.Replace("`", "``")}`",
                SqlIdentifierEscapeStyle.double_quote => $"\"{ident.Replace("\"", "\"\"")}\"",
                SqlIdentifierEscapeStyle.bracket => $"[{ident.Replace("]", "]]")}]",
                _ => ident
            };
        }

        static bool IsWordLike(SqlToken tok)
            => tok.Kind is SqlTokenKind.Identifier
            or SqlTokenKind.Keyword
            or SqlTokenKind.Number
            or SqlTokenKind.Parameter
            or SqlTokenKind.String;

        static bool NeedsSpace(SqlToken? p, SqlToken c)
        {
            if (p is null) return false;

            if (c.Kind == SqlTokenKind.Symbol && (c.Text is "." or ")" or "," or ";")) return false;
            if (p.Value.Kind == SqlTokenKind.Symbol && (p.Value.Text is "." or "(")) return false;
            if (p.Value.Kind == SqlTokenKind.Symbol && (p.Value.Text is ")" or ","))
                return IsWordLike(c) || c.Kind == SqlTokenKind.Number || c.Kind == SqlTokenKind.String;
            if (p.Value.Kind == SqlTokenKind.Symbol && p.Value.Text == ";") return false;
            if (c.Kind == SqlTokenKind.Symbol && c.Text == "(") return false;
            if (IsWordLike(p.Value) && IsWordLike(c)) return true;
            if ((p.Value.Kind == SqlTokenKind.Operator && c.Kind != SqlTokenKind.Symbol) ||
                (c.Kind == SqlTokenKind.Operator && p.Value.Kind != SqlTokenKind.Symbol))
                return true;

            return true;
        }
    }

    internal static string DescribeFoundToken(SqlToken token)
        => IsEnd(token) ? "<end-of-statement>" : token.Text;

    internal string DescribeFoundToken() => DescribeFoundToken(Peek());

    internal static string DescribeFoundTokenFromRaw(string raw)
    {
        var trimmed = raw.TrimStart();
        if (string.IsNullOrWhiteSpace(trimmed))
            return "<end-of-statement>";

        var tokenEnd = 0;
        while (tokenEnd < trimmed.Length && !char.IsWhiteSpace(trimmed[tokenEnd]))
            tokenEnd++;

        return trimmed[..tokenEnd];
    }

    internal static string NormalizeClauseText(string? raw)
    {
        if (string.IsNullOrWhiteSpace(raw))
            return string.Empty;

        var txt = raw!.Trim();
        if (txt.EndsWith(";", StringComparison.Ordinal))
            txt = txt[..^1].TrimEnd();

        return txt;
    }

    internal static bool IsJoinStart(SqlToken t)
        => JoinStart.Contains(t.Text);

    internal bool IsJoinStart() => IsJoinStart(Peek());

    internal static bool IsClauseKeywordToken(SqlToken t, IReadOnlyCollection<string>? additionalStopWords = null)
        => ClauseKeywordToken.Contains(t.Text)
           || (additionalStopWords?.Contains(t.Text) == true);

    private static bool ShouldStopAtTopLevelToken(SqlToken current, IReadOnlyList<string> stopWords, IReadOnlyList<SqlToken> buffer)
    {
        if (!stopWords.Any(sw => IsWord(current, sw)))
            return false;

        if (IsWord(current, SqlConst.FOR) && EndsWithWords(buffer, SqlConst.NEXT, SqlConst.VALUE))
            return false;

        if (IsWord(current, SqlConst.FOR) && EndsWithWords(buffer, SqlConst.PREVIOUS, SqlConst.VALUE))
            return false;

        if (IsWord(current, SqlConst.GROUP) && EndsWithWord(buffer, SqlConst.WITHIN))
            return false;

        return true;
    }

    private static bool EndsWithWord(IReadOnlyList<SqlToken> buffer, string word)
    {
        if (buffer.Count == 0)
            return false;

        var tail = buffer[^1];
        return IsWord(tail, word);
    }

    private static bool EndsWithWords(IReadOnlyList<SqlToken> buffer, params string[] words)
    {
        if (buffer.Count < words.Length)
            return false;

        for (var index = 0; index < words.Length; index++)
        {
            if (!IsWord(buffer[buffer.Count - words.Length + index], words[index]))
                return false;
        }

        return true;
    }
}
