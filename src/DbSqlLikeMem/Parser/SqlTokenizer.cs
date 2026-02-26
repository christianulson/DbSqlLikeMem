namespace DbSqlLikeMem;

internal sealed class SqlTokenizer
{
    private readonly string _sql;
    private readonly ISqlDialect _dialect;
    private int _pos;

    /// <summary>
    /// EN: Implements SqlTokenizer.
    /// PT: Implementa SqlTokenizer.
    /// </summary>
    public SqlTokenizer(string sql, ISqlDialect dialect)
    {
        _sql = sql ?? throw new ArgumentNullException(nameof(sql));
        _dialect = dialect;
    }

    /// <summary>
    /// EN: Implements Tokenize.
    /// PT: Implementa Tokenize.
    /// </summary>
    public IReadOnlyList<SqlToken> Tokenize()
    {
        var tokens = new List<SqlToken>();

        while (!Eof)
        {
            SkipWhiteSpace();
            if (Eof) break;

            var ch = Peek();

            // --- comentários: -- linha e /* bloco */
            if (TrySkipComment())
                continue;

            if (ch == '\\' && _dialect.IsStringQuote(Peek(1)))
            {
                Read(); // skip escape char before string literal
                continue;
            }

            if (_dialect.IsStringQuote(ch))
            {
                tokens.Add(ReadString());
                continue;
            }

            if (ch == ':' && Peek(1) == ':' && _dialect.Operators.Contains("::"))
            {
                var start = _pos;
                Read();
                Read();
                tokens.Add(new SqlToken(SqlTokenKind.Operator, "::", start));
                continue;
            }

            if (_dialect.IsParameterPrefix(ch))
            {
                tokens.Add(ReadParameter());
                continue;
            }

            if (char.IsDigit(ch) || (ch == '-' && char.IsDigit(Peek(1))))
            {
                tokens.Add(ReadNumber());
                continue;
            }

            // If we see an identifier quote that the current dialect does not allow,
            // fail fast with a clear error instead of tokenizing a confusing sequence.
            if ((ch == '`' || ch == '[' || ch == '"')
                && !_dialect.TryGetIdentifierQuote(ch, out _)
                && !_dialect.IsStringQuote(ch)
                && !_dialect.AllowsParserCrossDialectQuotedIdentifiers)
                throw SqlUnsupported.ForDialect(_dialect, $"alias/identificadores com '{ch}'");

            if (IsIdentStart(ch) || IsStartOfQuotedIdentifier(ch))
            {
                tokens.Add(ReadIdentifierOrKeyword());
                continue;
            }

            if (TryReadOperator(out var op))
            {
                tokens.Add(op);
                continue;
            }

            tokens.Add(new SqlToken(SqlTokenKind.Symbol, Read().ToString(), _pos - 1));
        }

        tokens.Add(SqlToken.EOF);
        return tokens;
    }

    private bool IsStartOfQuotedIdentifier(char ch)
        => TryGetIdentifierQuote(ch, out _);

    private bool TryGetIdentifierQuote(char begin, out SqlQuotePair pair)
    {
        if (_dialect.TryGetIdentifierQuote(begin, out pair))
            return true;

        if (!_dialect.AllowsParserCrossDialectQuotedIdentifiers)
        {
            pair = default;
            return false;
        }

        // Parser em modo compatível: aceita aspas de outros dialetos para round-trip.
        if (begin == '`')
        {
            pair = new SqlQuotePair('`', '`');
            return true;
        }

        if (begin == '[')
        {
            pair = new SqlQuotePair('[', ']');
            return true;
        }

        if (begin == '"' && !_dialect.IsStringQuote(begin))
        {
            pair = new SqlQuotePair('"', '"');
            return true;
        }

        pair = default;
        return false;
    }

    private SqlToken ReadString()
    {
        // IMPORTANT: Token.Text for strings must be the logical value WITHOUT quotes.
        // Quotes are syntax and must not leak into LiteralExpr.Value.
        var quote = Read();
        var startPos = _pos - 1;

        var sb = new System.Text.StringBuilder();

        while (!Eof)
        {
            var ch = Read();

            if (_dialect.StringEscapeStyle == SqlStringEscapeStyle.backslash && ch == '\\')
            {
                // Backslash escapes the next char (including quotes and backslashes).
                if (Eof)
                    throw new InvalidOperationException($"String não fechada. pos={startPos}");

                sb.Append(Read());
                continue;
            }

            if (ch == quote)
            {
                if (_dialect.StringEscapeStyle != SqlStringEscapeStyle.backslash)
                {
                    // Doubled quote escape: '' inside single-quoted strings (or "" inside double-quoted strings)
                    if (!Eof && Peek() == quote)
                    {
                        Read(); // consume escaped quote
                        sb.Append(quote);
                        continue;
                    }
                }

                // end of string
                return new SqlToken(SqlTokenKind.String, sb.ToString(), startPos);
            }

            sb.Append(ch);
        }

        throw new InvalidOperationException($"String não fechada. pos={startPos}");
    }
    private SqlToken ReadNumber()
    {
        var startPos = _pos;
        if (Peek() == '-') Read();

        while (!Eof && char.IsDigit(Peek())) Read();

        if (!Eof && Peek() == '.')
        {
            Read();
            while (!Eof && char.IsDigit(Peek())) Read();
        }

        return new SqlToken(SqlTokenKind.Number, _sql[startPos.._pos], startPos);
    }

    private SqlToken ReadParameter()
    {
        var startPos = _pos;
        Read(); // @ : ?

        // '?' sozinho conta como um parâmetro também
        while (!Eof && IsIdentChar(Peek()))
            Read();

        return new SqlToken(SqlTokenKind.Parameter, _sql[startPos.._pos], startPos);
    }

    private SqlToken ReadIdentifierOrKeyword()
    {
        var startPos = _pos;

        if (IsStartOfQuotedIdentifier(Peek()))
        {
            var open = Read();

            if (!TryGetIdentifierQuote(open, out var pair))
                throw SqlUnsupported.ForDialect(_dialect, $"alias/identificadores com '{open}'");

            char close = pair.End;

            var start = _pos;
            var sb = new System.Text.StringBuilder();
            while (!Eof)
            {
                var ch = Read();
                if (ch == close)
                {
                    // Escaped close?
                    if (_dialect.AllowsDoubleQuoteIdentifiers && close == '"' && !Eof && Peek() == '"')
                    {
                        Read(); // consume escaped "
                        sb.Append('"');
                        continue;
                    }
                    if (_dialect.AllowsBacktickIdentifiers && close == '`' && !Eof && Peek() == '`')
                    {
                        Read(); // consume escaped `
                        sb.Append('`');
                        continue;
                    }
                    if (_dialect.AllowsBracketIdentifiers && close == ']' && !Eof && Peek() == ']')
                    {
                        Read(); // consume escaped ]
                        sb.Append(']');
                        continue;
                    }

                    // end
                    break;
                }

                sb.Append(ch);
            }

            if (Eof && (_pos == start))
                throw new InvalidOperationException($"Identificador quoted não fechado. quote={open}");

            var name = sb.ToString();
                        // NOTE: token Text should be the logical identifier name (without delimiters).
            // Delimiters are syntax and must not leak into the AST.
            // Dialect-specific acceptance is enforced by IsStartOfQuotedIdentifier.
            return new SqlToken(SqlTokenKind.Identifier, name, startPos);
        }

        Read(); // 1o char
        while (!Eof && IsIdentChar(Peek())) Read();

        var text = _sql[startPos.._pos];

        if (_dialect.IsKeyword(text))
            return new SqlToken(SqlTokenKind.Keyword, text.ToUpperInvariant(), startPos);

        return new SqlToken(SqlTokenKind.Identifier, text, startPos);
    }

    private bool TryReadOperator(out SqlToken token)
    {
        token = default!;

        // Greedy safeguard for tokens like "<=>" even when the dialect does not
        // explicitly list the full operator (prevents tokenizing as "<=" + ">").
        if (_pos + 3 <= _sql.Length
            && string.CompareOrdinal(_sql, _pos, "<=>", 0, 3) == 0)
        {
            var p = _pos;
            _pos += 3;
            token = new SqlToken(SqlTokenKind.Operator, "<=>", p);
            return true;
        }

        // Compat parser: operadores JSON podem ser aceitos conforme regra do dialeto.
        if (_dialect.SupportsJsonArrowOperators || _dialect.AllowsParserCrossDialectJsonOperators)
        {
            foreach (var op in new[] { "#>>", "->>", "#>", "->" })
            {
                if (_pos + op.Length <= _sql.Length && string.CompareOrdinal(_sql, _pos, op, 0, op.Length) == 0)
                {
                    var p = _pos;
                    _pos += op.Length;
                    token = new SqlToken(SqlTokenKind.Operator, op, p);
                    return true;
                }
            }
        }

        foreach (var op in _dialect.Operators)
        {
            if (_pos + op.Length <= _sql.Length && string.CompareOrdinal(_sql, _pos, op, 0, op.Length) == 0)
            {
                var p = _pos;
                _pos += op.Length;
                token = new SqlToken(SqlTokenKind.Operator, op, p);
                return true;
            }
        }

        return false;
    }

    private bool Eof => _pos >= _sql.Length;
    private char Peek(int offset = 0) => _pos + offset < _sql.Length ? _sql[_pos + offset] : '\0';
    private char Read() => _sql[_pos++];

    private void SkipWhiteSpace()
    {
        while (!Eof && char.IsWhiteSpace(Peek()))
            _pos++;
    }

    private bool IsIdentStart(char c)
        => char.IsLetter(c) || c == '_' || (_dialect.AllowsHashIdentifiers && c == '#');

    private bool IsIdentChar(char c)
        => char.IsLetterOrDigit(c) || c == '_' || c == '$' || (_dialect.AllowsHashIdentifiers && c == '#');

    private bool TrySkipComment()
    {
        if (Eof) return false;

        // -- comentário até fim da linha
        if (Peek() == '-' && Peek(1) == '-')
        {
            _pos += 2;
            while (!Eof && Peek() != '\n') _pos++;
            return true;
        }

        // /* bloco */
        if (Peek() == '/' && Peek(1) == '*')
        {
            _pos += 2;
            while (!Eof)
            {
                if (Peek() == '*' && Peek(1) == '/')
                {
                    _pos += 2;
                    break;
                }
                _pos++;
            }
            return true;
        }

        return false;
    }
}
