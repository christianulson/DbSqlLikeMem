namespace DbSqlLikeMem;

internal sealed class SqlExpressionParserContext
{
    private readonly IReadOnlyList<SqlToken> _toks;
    private readonly ISqlDialect _dialect;
    private readonly IDataParameterCollection? _parameters;
    private readonly Func<string, bool>? _customFunctionSupported;
    private int _i;

    internal SqlExpressionParserContext(
        IReadOnlyList<SqlToken> toks,
        ISqlDialect dialect,
        IDataParameterCollection? parameters,
        Func<string, bool>? customFunctionSupported)
    {
        _toks = toks ?? throw new ArgumentNullException(nameof(toks));
        _dialect = dialect ?? throw new ArgumentNullException(nameof(dialect));
        _parameters = parameters;
        _customFunctionSupported = customFunctionSupported;
    }
    internal IReadOnlyList<SqlToken> Toks => _toks;

    internal ISqlDialect Dialect => _dialect;

    internal IDataParameterCollection? Parameters => _parameters;

    internal Func<string, bool>? CustomFunctionSupported => _customFunctionSupported;

    internal int Index
    {
        get => _i;
        set => _i = value;
    }

    internal SqlToken Peek(int offset = 0)
    {
        var idx = _i + offset;
        if (idx < 0)
            idx = 0;

        return idx < _toks.Count ? _toks[idx] : SqlToken.EOF;
    }

    internal SqlToken PeekTokenFrom(int index)
        => (index >= 0 && index < _toks.Count) ? _toks[index] : SqlToken.EOF;

    internal SqlToken Consume() => _toks[_i++];

    internal static bool IsEnd(SqlToken t) => t.Kind == SqlTokenKind.EndOfFile;

    internal bool IsEnd() => IsEnd(Peek());

    internal static bool IsWord(SqlToken t, string w) => t.Text.Equals(w, StringComparison.OrdinalIgnoreCase);

    internal bool IsWord(string w) => IsWord(Peek(), w);

    internal static bool IsKeyword(SqlToken t, string kw)
        => t.Kind == SqlTokenKind.Keyword && t.Text.Equals(kw, StringComparison.OrdinalIgnoreCase);

    internal bool IsKeyword(string kw) => IsKeyword(Peek(), kw);

    internal static bool IsSymbol(SqlToken t, string s) => t.Kind == SqlTokenKind.Symbol && t.Text == s;

    internal bool IsSymbol(string s) => IsSymbol(Peek(), s);

    internal static bool IsKeywordOrIdentifierWord(SqlToken t, string word)
        => t.Text.Equals(word, StringComparison.OrdinalIgnoreCase);

    internal bool IsKeywordOrIdentifierWord(string word) => IsKeywordOrIdentifierWord(Peek(), word);

    internal void ExpectWord(string w)
    {
        if (!IsKeywordOrIdentifierWord(Peek(), w))
            throw Error($"Esperava {w}, veio {Peek().Text}", Peek());

        Consume();
    }

    internal void ExpectSymbol(string s)
    {
        var t = Peek();
        if (!string.Equals(t.Text, s, StringComparison.Ordinal))
            throw Error($"Esperava símbolo {s}, veio {t.Text}", t);

        Consume();
    }

    internal void ExpectEnd()
    {
        var t = Peek();
        if (!IsEnd(t))
            throw Error($"Esperava fim da expressão, veio {t.Kind} '{t.Text}'", t);

        Consume();
    }

    internal InvalidOperationException Error(string msg)
    => Error(msg, Peek());

    internal InvalidOperationException Error(string msg, SqlToken t)
        => new($"{msg} (pos {t.Position})");

    internal string TokenToSql(SqlToken t)
    {
        return t.Kind switch
        {
            SqlTokenKind.String => $"'{EscapeStringLiteral(t.Text)}'",
            _ => t.Text
        };

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
    }

    internal string TokensToSql(IEnumerable<SqlToken> toks)
    {
        var sb = new StringBuilder();
        SqlToken? prev = null;

        foreach (var t in toks)
        {
            var text = TokenToSql(t);

            if (sb.Length > 0 && NeedsSpace(prev, t))
                sb.Append(' ');

            sb.Append(text);
            prev = t;
        }

        return sb.ToString().Trim();

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

    internal IReadOnlyList<SqlToken> ReadTokensUntilMatchingParen()
    {
        var depth = 0;
        var tokens = new List<SqlToken>();
        while (true)
        {
            var t = Peek();
            if (IsEnd(t))
                break;

            if (IsSymbol(t, "("))
            {
                depth++;
                tokens.Add(Consume());
                continue;
            }

            if (IsSymbol(t, ")"))
            {
                if (depth == 0)
                    break;

                depth--;
                tokens.Add(Consume());
                continue;
            }

            tokens.Add(Consume());
        }

        return tokens;
    }

    public NotSupportedException NotSupported(string feature)
        => Dialect.NotSupported(feature);
}
