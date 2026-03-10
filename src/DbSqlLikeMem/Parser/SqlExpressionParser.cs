namespace DbSqlLikeMem;

internal sealed class SqlExpressionParser(
    IReadOnlyList<SqlToken> toks,
    ISqlDialect dialect,
    IDataParameterCollection? parameters = null
    )
{
    private readonly IReadOnlyList<SqlToken> _toks = toks
        ?? throw new ArgumentNullException(nameof(toks));
    private readonly ISqlDialect _dialect = dialect ?? throw new ArgumentNullException(nameof(dialect));
    private readonly IDataParameterCollection? _parameters = parameters;
    private int _i;

    /// <summary>
    /// EN: Implements ParseWhere.
    /// PT: Implementa ParseWhere.
    /// </summary>
    public static SqlExpr ParseWhere(
        string whereSql,
        ISqlDialect dialect)
        => ParseWhere(whereSql, dialect, null);

    /// <summary>
    /// EN: Parses a WHERE expression using the automatic dialect compatibility mode.
    /// PT: Faz o parsing de uma expressao WHERE usando o modo de compatibilidade automatica de dialeto.
    /// </summary>
    /// <param name="whereSql">EN: WHERE expression text. PT: Texto da expressao WHERE.</param>
    /// <returns>EN: Parsed expression AST. PT: AST da expressao parseada.</returns>
    public static SqlExpr ParseWhereAuto(string whereSql)
        => ParseWhere(whereSql, new AutoSqlDialect(), null);

    /// <summary>
    /// EN: Parses a WHERE expression using the automatic dialect compatibility mode and optional parameters.
    /// PT: Faz o parsing de uma expressao WHERE usando o modo de compatibilidade automatica de dialeto e parametros opcionais.
    /// </summary>
    /// <param name="whereSql">EN: WHERE expression text. PT: Texto da expressao WHERE.</param>
    /// <param name="parameters">EN: Optional command parameters used by parser paths that resolve parameterized values. PT: Parametros de comando opcionais usados por caminhos do parser que resolvem valores parametrizados.</param>
    /// <returns>EN: Parsed expression AST. PT: AST da expressao parseada.</returns>
    public static SqlExpr ParseWhereAuto(string whereSql, IDataParameterCollection? parameters)
        => ParseWhere(whereSql, new AutoSqlDialect(), parameters);

    public static SqlExpr ParseWhere(
        string whereSql,
        ISqlDialect dialect,
        IDataParameterCollection? parameters)
    {
        var d = dialect;
        var toks = new SqlTokenizer(whereSql, d).Tokenize();
        EnsureJsonArrowSupport(toks, d);
        var p = new SqlExpressionParser(toks, d, parameters);
        var expr = p.ParseExpression(0);
        expr = p.TryConsumeTrailingLikeEscape(expr);
        p.ExpectEnd();
        return expr;
    }

    /// <summary>
    /// EN: Implements ParseScalar.
    /// PT: Implementa ParseScalar.
    /// </summary>
    public static SqlExpr ParseScalar(string sql, ISqlDialect dialect)
        => ParseScalar(sql, dialect, null);

    /// <summary>
    /// EN: Parses a scalar expression using the automatic dialect compatibility mode.
    /// PT: Faz o parsing de uma expressao escalar usando o modo de compatibilidade automatica de dialeto.
    /// </summary>
    /// <param name="sql">EN: Scalar SQL expression to parse. PT: Expressao SQL escalar para parsear.</param>
    /// <returns>EN: Parsed expression AST. PT: AST da expressao parseada.</returns>
    public static SqlExpr ParseScalarAuto(string sql)
        => ParseScalar(sql, new AutoSqlDialect(), null);

    /// <summary>
    /// EN: Parses a scalar expression using the automatic dialect compatibility mode and optional parameters.
    /// PT: Faz o parsing de uma expressao escalar usando o modo de compatibilidade automatica de dialeto e parametros opcionais.
    /// </summary>
    /// <param name="sql">EN: Scalar SQL expression to parse. PT: Expressao SQL escalar para parsear.</param>
    /// <param name="parameters">EN: Optional command parameters used by parser paths that resolve parameterized values. PT: Parametros de comando opcionais usados por caminhos do parser que resolvem valores parametrizados.</param>
    /// <returns>EN: Parsed expression AST. PT: AST da expressao parseada.</returns>
    public static SqlExpr ParseScalarAuto(string sql, IDataParameterCollection? parameters)
        => ParseScalar(sql, new AutoSqlDialect(), parameters);

    public static SqlExpr ParseScalar(string sql, ISqlDialect dialect, IDataParameterCollection? parameters)
    {
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(sql, nameof(sql));
        ArgumentNullExceptionCompatible.ThrowIfNull(dialect, nameof(dialect));
        var d = dialect;
        var toks = new SqlTokenizer(sql, d).Tokenize();
        EnsureJsonArrowSupport(toks, d);
        var p = new SqlExpressionParser(toks, d, parameters);
        var expr = p.ParseExpression(0);
        expr = p.TryConsumeTrailingLikeEscape(expr);
        p.ExpectEnd();
        return expr;
    }

    private static void EnsureJsonArrowSupport(IReadOnlyList<SqlToken> toks, ISqlDialect dialect)
    {
        if (dialect.SupportsJsonArrowOperators || dialect.AllowsParserCrossDialectJsonOperators)
            return;

        if (toks.Any(t => t.Kind == SqlTokenKind.Operator
            && (t.Text == "->" || t.Text == "->>" || t.Text == "#>" || t.Text == "#>>")))
            throw SqlUnsupported.ForDialect(dialect, "JSON -> / ->> / #> / #>> operators");
    }

    // Pratt: parse com binding power
    /// <summary>
    /// EN: Implements ParseExpression.
    /// PT: Implementa ParseExpression.
    /// </summary>
    public SqlExpr ParseExpression(int minBp)
    {
        var left = ParsePrefix();

        while (true)
        {
            // postfix: IS [NOT] NULL
            if (TryParseIsNullPostfix(ref left)) continue;

            // NOT IN / NOT LIKE / NOT REGEXP / NOT BETWEEN
            if (TryParseNotInfix(ref left, minBp)) continue;

            // IN (...)
            if (TryParseInInfix(ref left, minBp)) continue;

            // LIKE
            if (TryParseLikeInfix(ref left, minBp)) continue;

        // REGEXP
        if (TryParseRegexpInfix(ref left, minBp)) continue;

            // PostgreSQL-style type cast: expr::type
            if (TryParseTypeCastInfix(ref left, minBp)) continue;

        // JSON -> / ->>
        if ((_dialect.SupportsJsonArrowOperators || _dialect.AllowsParserCrossDialectJsonOperators)
            && TryParseJsonArrowInfix(ref left, minBp)) continue;

            // * /
            if (TryParseMulDivInfix(ref left, minBp)) continue;

            // + -
            if (TryParseAddSubInfix(ref left, minBp)) continue;

            // BETWEEN / NOT BETWEEN (NOT BETWEEN é coberto no TryParseNotInfix)
            if (TryParseBetweenInfix(ref left, minBp)) continue;

            // comparações: = != <> >= <= > <
            if (TryParseComparisonInfix(ref left, minBp)) continue;

            // AND / OR
            // IMPORTANT: comparison operators must bind stronger than logical conjunction/disjunction.
            // Keep this after comparison parsing to preserve SQL precedence like:
            //   a = 1 OR b = 2 AND c = 3  => a = 1 OR (b = 2 AND c = 3)
            if (TryParseAndOrInfix(ref left, minBp)) continue;

            break;
        }

        return left;
    }

    #region PARSE EXPRESSIONS

    // ------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------

    private bool TryParseIsNullPostfix(ref SqlExpr left)
    {
        var t = Peek();
        if (!IsKeyword(t, "IS"))
            return false;

        var save = _i;
        Consume(); // IS

        bool neg = false;
        if (IsKeyword(Peek(), "NOT"))
        {
            Consume();
            neg = true;
        }

        if (IsKeyword(Peek(), "NULL"))
        {
            Consume();
            left = new IsNullExpr(left, neg);
            return true;
        }

        // não era IS NULL; volta
        _i = save;
        return false;
    }

    // ------------------------------------------------------------
    // NOT infix dispatcher + handlers (reduz complexidade)
    // ------------------------------------------------------------
    private bool TryParseNotInfix(ref SqlExpr left, int minBp)
    {
        var t = Peek();
        if (!IsKeyword(t, "NOT"))
            return false;

        var t2 = Peek(1);

        // Dispatch: NOT <op>
        if (IsKeyword(t2, "BETWEEN"))
            return TryParseNotBetween(ref left, minBp);

        if (IsKeyword(t2, "IN"))
            return TryParseNotIn(ref left, minBp);

        if (IsKeyword(t2, "LIKE") || IsKeywordOrIdentifierWord(t2, "ILIKE"))
            return TryParseNotLike(ref left, minBp);

        if (IsKeywordOrIdentifierWord(t2, "REGEXP"))
            return TryParseNotRegexp(ref left, minBp);

        return false;
    }

    #region NOT

    private bool TryParseNotBetween(ref SqlExpr left, int minBp)
    {
        if (!TryCheckBp(minBp, 50)) return false;

        Consume(); // NOT
        Consume(); // BETWEEN

        var low = ParseExpression(51);

        ExpectWord("AND"); // garante AND como keyword/identifier-word
        var high = ParseExpression(51);

        left = new BetweenExpr(left, low, high, Negated: true);
        return true;
    }

    private bool TryParseNotIn(ref SqlExpr left, int minBp)
    {
        if (!TryCheckBp(minBp, 50)) return false;

        var notTok = Peek();
        Consume(); // NOT
        Consume(); // IN

        if (left is RowExpr && !IsSymbol(Peek(), "("))
            throw Error("Row value IN requires parentheses", notTok);

        // NOT IN ( ... )  ou NOT IN ( (SELECT ...) )
        var payload = ParseInPayload(notTok, "NOT IN"); // retorna lista de SqlExpr ou SubqueryExpr embrulhado
        left = new UnaryExpr(SqlUnaryOp.Not, new InExpr(left, payload));
        return true;
    }

    private bool TryParseNotLike(ref SqlExpr left, int minBp)
    {
        if (!TryCheckBp(minBp, 50)) return false;

        Consume(); // NOT
        Consume(); // LIKE

        left = new UnaryExpr(SqlUnaryOp.Not, ParseLikeExpression(left, 51));
        return true;
    }

    private bool TryParseNotRegexp(ref SqlExpr left, int minBp)
    {
        if (!TryCheckBp(minBp, 50)) return false;

        Consume(); // NOT
        Consume(); // REGEXP

        var pattern = ParseExpression(51);
        left = new UnaryExpr(SqlUnaryOp.Not, new BinaryExpr(SqlBinaryOp.Regexp, left, pattern));
        return true;
    }

    // ------------------------------------------------------------
    // Shared helpers
    // ------------------------------------------------------------

    // Retorna false se não tem precedência suficiente
    private static bool TryCheckBp(int minBp, int lbp) => lbp >= minBp;

    // Parse do conteúdo de IN(...), lidando com:
    // - IN (SELECT ...)
    // - IN ((SELECT ...))  (parêntese extra)
    // - IN (expr, expr, ...)
    // Retorna IEnumerable<SqlExpr> para ser usado pelo InExpr
    private SqlExpr[] ParseInPayload(SqlToken contextToken, string contextLabel, int rbp = 51)
    {
        // Dapper: permite "IN @ids" (sem parênteses) para listas/arrays.
        // Então suportamos tanto:
        //   col IN (@p1,@p2)
        // quanto:
        //   col IN @ids
        if (!IsSymbol(Peek(), "("))
        {
            // Parseia uma única expressão (tipicamente ParameterExpr) com binding forte
            // para não engolir AND/OR subsequentes.
            var single = ParseExpression(rbp);
            return [single];
        }

        ExpectSymbol("(");

        // caso IN ( ( ... ) )  com parêntese extra
        bool extraParen = false;
        if (IsSymbol(Peek(), "("))
        {
            extraParen = true;
            Consume(); // '('
        }

        // ✅ MySQL: IN () e IN(()) são inválidos
        if (IsSymbol(Peek(), ")"))
            throw Error("IN requires at least one element or a subquery", contextToken);

        // subquery: SELECT ou WITH
        if (IsKeywordOrIdentifierWord(Peek(), "SELECT")
            || IsKeywordOrIdentifierWord(Peek(), "WITH"))
        {
            var subSql = ReadRawUntilMatchingParen(); // lê até fechar o ')' do SELECT/WITH

            if (extraParen) ExpectSymbol(")"); // fecha '(' extra
            ExpectSymbol(")");                 // fecha IN(...)

            return [ParseAndWrapSubquery(subSql, contextToken, contextLabel)];
        }

        // lista normal (tem que ter ao menos 1)
        var items = ParseExprListUntilParenClose(extraParen);

        // ✅ garantia extra: se alguém burlar por bug, explode igual
        if (items.Count == 0)
            throw Error("IN requires at least one element or a subquery", contextToken);

        return [.. items];
    }

    // Lê expr, expr, ... até fechar o ')'. Se tinha parêntese extra, fecha duas vezes.
    private List<SqlExpr> ParseExprListUntilParenClose(bool extraParen)
    {
        var items = new List<SqlExpr>();

        if (!IsSymbol(Peek(), ")"))
        {
            while (true)
            {
                items.Add(ParseExpression(0));
                if (!IsSymbol(Peek(), ",")) break;
                Consume(); // ','
            }
        }

        if (extraParen) ExpectSymbol(")");
        ExpectSymbol(")");
        return items;
    }

    #endregion

    private bool TryParseInInfix(ref SqlExpr left, int minBp)
    {
        var t = Peek();
        if (!IsKeyword(t, "IN"))
            return false;

        var (lbp, rbp) = (50, 51);
        if (lbp < minBp) return false;

        var inTok = t;
        Consume(); // IN

        if (left is RowExpr && !IsSymbol(Peek(), "("))
            throw Error("Row value IN requires parentheses", inTok);

        var payload = ParseInPayload(inTok, "IN");

        left = new InExpr(left, payload);
        return true;
    }

    private bool TryParseLikeInfix(ref SqlExpr left, int minBp)
    {
        var t = Peek();
        var negate = false;

        var caseInsensitive = false;

        if (IsKeyword(t, "NOT"))
        {
            var next = Peek(1);
            if (IsKeyword(next, "LIKE"))
            {
            }
            else if (IsKeywordOrIdentifierWord(next, "ILIKE"))
            {
                caseInsensitive = true;
            }
            else
                return false;
            negate = true;
        }
        else if (IsKeyword(t, "LIKE"))
        {
        }
        else if (IsKeywordOrIdentifierWord(t, "ILIKE"))
        {
            caseInsensitive = true;
        }
        else
        {
            return false;
        }

        var (lbp, rbp) = (50, 51);
        if (lbp < minBp) return false;

        if (negate)
        {
            Consume(); // NOT
            Consume(); // LIKE / ILIKE
        }
        else
        {
            Consume(); // LIKE / ILIKE
        }

        if (caseInsensitive && !_dialect.SupportsIlikeOperator)
            throw SqlUnsupported.ForDialect(_dialect, "ILIKE");

        var expr = (SqlExpr)ParseLikeExpression(left, rbp, caseInsensitive);
        left = negate ? new UnaryExpr(SqlUnaryOp.Not, expr) : expr;
        return true;
    }

    private LikeExpr ParseLikeExpression(SqlExpr left, int rbp, bool caseInsensitive = false)
    {
        var pattern = ParseExpression(rbp);
        return TryParseLikeEscape(new LikeExpr(left, pattern, null, caseInsensitive), rbp);
    }

    private bool TryParseRegexpInfix(ref SqlExpr left, int minBp)
    {
        var t = Peek();
        var negate = false;

        if (IsKeyword(t, "NOT"))
        {
            var next = Peek(1);
            if (!IsKeywordOrIdentifierWord(next, "REGEXP"))
                return false;
            negate = true;
        }
        else if (!IsKeywordOrIdentifierWord(t, "REGEXP"))
        {
            return false;
        }

        var (lbp, rbp) = (50, 51);
        if (lbp < minBp) return false;

        if (negate)
        {
            Consume(); // NOT
            Consume(); // REGEXP
        }
        else
        {
            Consume(); // REGEXP
        }

        var pattern = ParseExpression(rbp);
        var expr = (SqlExpr)new BinaryExpr(SqlBinaryOp.Regexp, left, pattern);
        left = negate ? new UnaryExpr(SqlUnaryOp.Not, expr) : expr;
        return true;
    }


    private bool TryParseJsonArrowInfix(ref SqlExpr left, int minBp)
    {
        var t = Peek();
        if (t.Kind != SqlTokenKind.Operator || (t.Text != "->" && t.Text != "->>" && t.Text != "#>" && t.Text != "#>>"))
            return false;

        if (!_dialect.SupportsJsonArrowOperators && !_dialect.AllowsParserCrossDialectJsonOperators)
            throw SqlUnsupported.ForDialect(_dialect, "JSON -> / ->> / #> / #>> operators");

        // MySQL: JSON extract operators bind tightly (treat like high precedence binary)
        const int lbp = 120;
        const int rbp = 121;
        if (lbp < minBp) return false;

        var op = Consume().Text;

        // right side: geralmente string '$.path', mas aceita expressão
        var right = ParseExpression(rbp);

        // Modela como nó neutro: o dialeto/executor decide como avaliar/imprimir.
        left = new JsonAccessExpr(left, right, Unquote: op == "->>" || op == "#>>");

        return true;
    }

    private bool TryParseTypeCastInfix(ref SqlExpr left, int minBp)
    {
        var t = Peek();
        if (t.Kind != SqlTokenKind.Operator || t.Text != "::")
            return false;

        const int lbp = 130;
        if (lbp < minBp) return false;

        Consume(); // ::

        var typeToks = new List<SqlToken>();
        var first = Peek();
        if (first.Kind != SqlTokenKind.Identifier && first.Kind != SqlTokenKind.Keyword)
            throw Error("Type name expected after '::'", first);

        typeToks.Add(Consume());

        if (IsSymbol(Peek(), "("))
        {
            var depth = 0;
            while (true)
            {
                var tok = Peek();
                if (tok.Kind == SqlTokenKind.EndOfFile)
                    throw Error("Type cast not closed", tok);

                if (tok.Kind == SqlTokenKind.Symbol && tok.Text == "(")
                    depth++;

                if (tok.Kind == SqlTokenKind.Symbol && tok.Text == ")")
                {
                    if (depth == 0)
                        break;
                    depth--;
                }

                typeToks.Add(Consume());
            }
        }

        var typeSql = string.Join(" ",
            typeToks.Select(TokenToSql)
        ).Trim();

        left = new CallExpr("CAST", [left, new RawSqlExpr(typeSql)]);
        return true;
    }

    private bool TryParseMulDivInfix(ref SqlExpr left, int minBp)
    {
        var t = Peek();
        if (t.Kind != SqlTokenKind.Operator || (t.Text != "*" && t.Text != "/"))
            return false;

        var (lbp, rbp) = (70, 71);
        if (lbp < minBp) return false;

        Consume(); // * or /
        var right = ParseExpression(rbp);

        var op = t.Text == "*" ? SqlBinaryOp.Multiply : SqlBinaryOp.Divide;
        left = new BinaryExpr(op, left, right);
        return true;
    }

    private bool TryParseAddSubInfix(ref SqlExpr left, int minBp)
    {
        var t = Peek();
        if (t.Kind != SqlTokenKind.Operator || (t.Text != "+" && t.Text != "-"))
            return false;

        var (lbp, rbp) = (60, 61);
        if (lbp < minBp) return false;

        Consume(); // + or -
        var right = ParseExpression(rbp);
        right = TryAttachCompactIntervalUnit(right);

        var op = t.Text == "+" ? SqlBinaryOp.Add : SqlBinaryOp.Subtract;
        left = new BinaryExpr(op, left, right);
        return true;
    }

    private SqlExpr TryAttachCompactIntervalUnit(SqlExpr expression)
    {
        if (expression is not LiteralExpr { Value: not null and not string } literal)
            return expression;

        var unitToken = Peek();
        if (!IsCompactIntervalUnit(unitToken))
            return expression;

        Consume();
        return new CallExpr("INTERVAL", [literal, new RawSqlExpr(unitToken.Text)]);
    }

    private static bool IsCompactIntervalUnit(SqlToken token)
    {
        if (token.Kind is not (SqlTokenKind.Identifier or SqlTokenKind.Keyword))
            return false;

        return token.Text.ToUpperInvariant() switch
        {
            "YEAR" or "YEARS"
            or "MONTH" or "MONTHS"
            or "DAY" or "DAYS"
            or "HOUR" or "HOURS"
            or "MINUTE" or "MINUTES"
            or "SECOND" or "SECONDS" => true,
            _ => false
        };
    }

    private bool TryParseBetweenInfix(ref SqlExpr left, int minBp)
    {
        var t = Peek();
        if (!IsKeyword(t, "BETWEEN"))
            return false;

        var (lbp, rbp) = (50, 51);
        if (lbp < minBp) return false;

        Consume(); // BETWEEN

        var low = ParseExpression(rbp);

        if (!IsKeyword(Peek(), "AND"))
            throw new InvalidOperationException("Esperava AND no BETWEEN");

        Consume(); // AND
        var high = ParseExpression(rbp);

        left = new BetweenExpr(left, low, high, Negated: false);
        return true;
    }

    private bool TryParseAndOrInfix(ref SqlExpr left, int minBp)
    {
        var t = Peek();
        if (!(IsKeyword(t, "AND") || IsKeyword(t, "OR")))
            return false;

        var op = IsKeyword(t, "AND") ? SqlBinaryOp.And : SqlBinaryOp.Or;
        var (lbp, rbp) = op == SqlBinaryOp.And ? (20, 21) : (10, 11);
        if (lbp < minBp) return false;

        Consume();
        var right = ParseExpression(rbp);
        left = new BinaryExpr(op, left, right);
        return true;
    }

    private bool TryParseComparisonInfix(ref SqlExpr left, int minBp)
    {
        var t = Peek();
        if (t.Kind != SqlTokenKind.Operator || !TryMapComparisonOp(t.Text, out var bop))
            return false;

        var (lbp, rbp) = (50, 51);
        if (lbp < minBp) return false;

        var opToken = Consume();

        if (TryParseQuantifiedComparisonRightSide(left, bop, opToken, out var quantifiedExpr))
        {
            left = quantifiedExpr;
            return true;
        }

        var right = ParseExpression(rbp);
        left = new BinaryExpr(bop, left, right);
        return true;
    }

    /// <summary>
    /// EN: Tries to parse quantified comparison right side (`ANY`/`SOME`/`ALL` with subquery) after a comparison operator.
    /// PT: Tenta parsear o lado direito de comparação quantificada (`ANY`/`SOME`/`ALL` com subquery) após operador de comparação.
    /// </summary>
    private bool TryParseQuantifiedComparisonRightSide(
        SqlExpr left,
        SqlBinaryOp op,
        SqlToken contextToken,
        out SqlExpr quantifiedExpr)
    {
        quantifiedExpr = default!;

        var qTok = Peek();
        if (!IsKeywordOrIdentifierWord(qTok, "ANY")
            && !IsKeywordOrIdentifierWord(qTok, "SOME")
            && !IsKeywordOrIdentifierWord(qTok, "ALL"))
            return false;

        var quantifier = IsKeywordOrIdentifierWord(qTok, "ANY")
                         || IsKeywordOrIdentifierWord(qTok, "SOME")
            ? SqlQuantifier.Any
            : SqlQuantifier.All;

        Consume(); // ANY | SOME | ALL
        ExpectSymbol("(");

        var hasExtraWrapperParen = false;
        if (IsSymbol(Peek(), "("))
        {
            hasExtraWrapperParen = true;
            Consume(); // optional wrapper '('
        }

        var subSql = ReadRawUntilMatchingParen();

        if (hasExtraWrapperParen)
            ExpectSymbol(")");

        ExpectSymbol(")");

        var subquery = SqlQueryParser.ParseSubqueryExprOrThrow(
            subSql,
            contextToken,
            $"{quantifier.ToString().ToUpperInvariant()} quantified comparison",
            _dialect);

        quantifiedExpr = new QuantifiedComparisonExpr(op, left, quantifier, subquery);
        return true;
    }

    #endregion

    private SqlExpr ParsePrefix()
    {
        var t = Peek();

        if (TryParseExists(t, out var ex)) return ex;
        if (TryParseCase(t, out var cs)) return cs;
        if (TryParseNot(t, out var nt)) return nt;
        if (TryParseStar(t, out var st)) return st;
        if (TryParseParenOrRow(t, out var pr)) return pr;
        if (TryParseNullTrueFalse(t, out var l1)) return l1;
        if (TryParseString(t, out var l2)) return l2;
        if (TryParseUnaryPlusMinus(t, out var upm)) return upm;
        if (TryParseNumber(t, out var l3)) return l3;
        if (TryParseParameter(t, out var p)) return p;
        if (TryParseIntervalLiteral(t, out var interval)) return interval;
        if (TryParseNextValueFor(t, out var nextValueFor)) return nextValueFor;
        if (TryParsePreviousValueFor(t, out var previousValueFor)) return previousValueFor;
        if (TryParseIdentifierOrCall(t, out var id)) return id;

        throw Error($"Token inesperado no prefix: {t.Kind} '{t.Text}'", t);
    }

    #region PARSE PREFIX
    // ----------------------------------------------------------------

    private bool TryParseExists(SqlToken t, out SqlExpr expr)
    {
        expr = default!;

        if (!IsKeywordOrIdentifierWord(t, "EXISTS"))
            return false;

        Consume(); // EXISTS
        ExpectSymbol("(");

        var subSql = ReadRawUntilMatchingParen();

        ExpectSymbol(")");

        // ✅ use o token t
        expr = new ExistsExpr(
            SqlQueryParser.ParseSubqueryExprOrThrow(subSql, t, "EXISTS", _dialect)
        );

        return true;
    }

    private bool TryParseCase(SqlToken t, out SqlExpr expr)
    {
        expr = default!;

        if (!IsKeywordOrIdentifierWord(t, "CASE"))
            return false;

        Consume(); // CASE

        SqlExpr? baseExpr = null;
        if (!IsKeywordOrIdentifierWord(Peek(), "WHEN"))
            baseExpr = ParseExpression(0);

        var whens = new List<CaseWhenThen>();
        while (IsKeywordOrIdentifierWord(Peek(), "WHEN"))
        {
            Consume(); // WHEN
            var whenExpr = ParseExpression(0);

            ExpectWord("THEN");
            var thenExpr = ParseExpression(0);

            whens.Add(new CaseWhenThen(whenExpr, thenExpr));
        }

        SqlExpr? elseExpr = null;
        if (IsKeywordOrIdentifierWord(Peek(), "ELSE"))
        {
            Consume(); // ELSE
            elseExpr = ParseExpression(0);
        }

        ExpectWord("END");
        expr = new CaseExpr(baseExpr, whens, elseExpr);
        return true;
    }

    private bool TryParseNot(SqlToken t, out SqlExpr expr)
    {
        expr = default!;

        if (!IsKeyword(t, "NOT"))
            return false;

        Consume();
        var rhs = ParseExpression(60);
        expr = new UnaryExpr(SqlUnaryOp.Not, rhs);
        return true;
    }

    private bool TryParseUnaryPlusMinus(SqlToken t, out SqlExpr expr)
    {
        expr = default!;
        if (t.Kind != SqlTokenKind.Operator || (t.Text != "+" && t.Text != "-"))
            return false;

        Consume(); // + or -
        var rhs = ParseExpression(80); // tighter than * / + -
        if (t.Text == "+")
        {
            // unary plus is a no-op
            expr = rhs;
            return true;
        }

        // unary minus: 0 - rhs (keeps AST simple)
        expr = new BinaryExpr(SqlBinaryOp.Subtract, new LiteralExpr(0m), rhs);
        return true;
    }

    private bool TryParseIntervalLiteral(SqlToken t, out SqlExpr expr)
    {
        expr = default!;

        if (!IsKeywordOrIdentifierWord(t, "INTERVAL"))
            return false;

        Consume(); // INTERVAL
        var next = Peek();
        if (next.Kind != SqlTokenKind.String)
            throw Error("INTERVAL requires a string literal", next);

        Consume();

        // PostgreSQL style: INTERVAL '1 day'
        // Oracle style:     INTERVAL '1' DAY
        var raw = next.Text;
        var unitTok = Peek();
        if (unitTok.Kind is SqlTokenKind.Keyword or SqlTokenKind.Identifier)
        {
            var unit = unitTok.Text.Trim();
            if (!string.IsNullOrEmpty(unit))
            {
                Consume();
                raw = $"{raw} {unit}";
            }
        }

        expr = new CallExpr("INTERVAL", [new LiteralExpr(raw)]);
        return true;
    }

    private bool TryParseNextValueFor(SqlToken t, out SqlExpr expr)
    {
        expr = default!;

        if (!IsKeywordOrIdentifierWord(t, "NEXT"))
            return false;

        if (!IsKeywordOrIdentifierWord(Peek(1), "VALUE")
            || !IsKeywordOrIdentifierWord(Peek(2), "FOR"))
            return false;

        if (!_dialect.SupportsNextValueForSequenceExpression)
            throw SqlUnsupported.ForDialect(_dialect, "NEXT VALUE FOR");

        Consume(); // NEXT
        Consume(); // VALUE
        Consume(); // FOR

        var sequenceToken = Peek();
        if (sequenceToken.Kind is not SqlTokenKind.Identifier and not SqlTokenKind.Keyword)
            throw Error("NEXT VALUE FOR requires a sequence name.", sequenceToken);

        Consume();
        expr = new CallExpr("NEXT_VALUE_FOR", [ParseIdentifierChainOrColumn(sequenceToken.Text)]);
        return true;
    }

    private bool TryParsePreviousValueFor(SqlToken t, out SqlExpr expr)
    {
        expr = default!;

        if (!IsKeywordOrIdentifierWord(t, "PREVIOUS"))
            return false;

        if (!IsKeywordOrIdentifierWord(Peek(1), "VALUE")
            || !IsKeywordOrIdentifierWord(Peek(2), "FOR"))
            return false;

        if (!_dialect.SupportsPreviousValueForSequenceExpression)
            throw SqlUnsupported.ForDialect(_dialect, "PREVIOUS VALUE FOR");

        Consume(); // PREVIOUS
        Consume(); // VALUE
        Consume(); // FOR

        var sequenceToken = Peek();
        if (sequenceToken.Kind is not SqlTokenKind.Identifier and not SqlTokenKind.Keyword)
            throw Error("PREVIOUS VALUE FOR requires a sequence name.", sequenceToken);

        Consume();
        expr = new CallExpr("PREVIOUS_VALUE_FOR", [ParseIdentifierChainOrColumn(sequenceToken.Text)]);
        return true;
    }


    private bool TryParseStar(SqlToken t, out SqlExpr expr)
    {
        expr = default!;

        if (t.Kind != SqlTokenKind.Operator || t.Text != "*")
            return false;

        Consume();
        expr = new StarExpr();
        return true;
    }

    private bool TryParseParenOrRow(SqlToken t, out SqlExpr expr)
    {
        expr = default!;

        if (!IsSymbol(t, "("))
            return false;

        Consume(); // '('

        // ✅ scalar subquery: (SELECT ... ) / (WITH ... )
        if (IsKeywordOrIdentifierWord(Peek(), "SELECT") || IsKeywordOrIdentifierWord(Peek(), "WITH"))
        {
            var subSql = ReadRawUntilMatchingParen(); // lê até antes do ')'
            ExpectSymbol(")");

            expr = SqlQueryParser.ParseSubqueryExprOrThrow(subSql, t, "SCALAR SUBQUERY", _dialect);
            return true;
        }

        var first = ParseExpression(0);

        if (IsSymbol(Peek(), ","))
        {
            var items = new List<SqlExpr> { first };
            while (IsSymbol(Peek(), ","))
            {
                Consume();
                items.Add(ParseExpression(0));
            }

            ExpectSymbol(")");
            expr = new RowExpr(items);
            return true;
        }

        ExpectSymbol(")");
        expr = first;
        return true;
    }

    private bool TryParseNullTrueFalse(SqlToken t, out SqlExpr expr)
    {
        expr = default!;

        if (t.Kind != SqlTokenKind.Keyword)
            return false;

        if (!(IsKeyword(t, "NULL") || IsKeyword(t, "TRUE") || IsKeyword(t, "FALSE")))
            return false;

        Consume();

        object? val =
            IsKeyword(t, "NULL")
            ? null
            : IsKeyword(t, "TRUE");

        expr = new LiteralExpr(val);
        return true;
    }

    private bool TryParseString(SqlToken t, out SqlExpr expr)
    {
        expr = default!;

        if (t.Kind != SqlTokenKind.String)
            return false;

        Consume();
        expr = new LiteralExpr(t.Text);
        return true;
    }

    private bool TryParseNumber(SqlToken t, out SqlExpr expr)
    {
        expr = default!;

        if (t.Kind != SqlTokenKind.Number)
            return false;

        Consume();

        if (TryParseNumericLiteralValue(t.Text, out var numericValue))
        {
            expr = new LiteralExpr(numericValue);
            return true;
        }

        throw Error($"Número inválido: {t.Text}", t);
    }

    private static bool TryParseNumericLiteralValue(string text, out object numericValue)
    {
        numericValue = default!;

        var normalized = text.Trim();
        if (normalized.Length == 0)
            return false;

        var hasExponent =
            normalized.IndexOf('e') >= 0
            || normalized.IndexOf('E') >= 0;
        var hasDecimalPoint = normalized.IndexOf('.') >= 0;

        if (!hasDecimalPoint && !hasExponent)
        {
            if (int.TryParse(normalized, NumberStyles.Integer, CultureInfo.InvariantCulture, out var intValue))
            {
                numericValue = intValue;
                return true;
            }

            if (long.TryParse(normalized, NumberStyles.Integer, CultureInfo.InvariantCulture, out var longValue))
            {
                numericValue = longValue;
                return true;
            }
        }

        if (hasExponent
            && double.TryParse(normalized, NumberStyles.Float, CultureInfo.InvariantCulture, out var doubleValue))
        {
            numericValue = doubleValue;
            return true;
        }

        if (decimal.TryParse(normalized, NumberStyles.Any, CultureInfo.InvariantCulture, out var decimalValue))
        {
            numericValue = decimalValue;
            return true;
        }

        if (!double.TryParse(normalized, NumberStyles.Float, CultureInfo.InvariantCulture, out var fallbackDouble))
            return false;

        numericValue = fallbackDouble;
        return true;
    }

    private bool TryParseParameter(SqlToken t, out SqlExpr expr)
    {
        expr = default!;

        if (t.Kind != SqlTokenKind.Parameter)
            return false;

        Consume();
        expr = new ParameterExpr(t.Text);
        return true;
    }

    private bool TryParseIdentifierOrCall(SqlToken t, out SqlExpr expr)
    {
        expr = default!;

        if (!(t.Kind == SqlTokenKind.Identifier || t.Kind == SqlTokenKind.Keyword))
            return false;

        Consume();
        var name = t.Text;

        // function call: name(...)
        if (IsSymbol(Peek(), "(")
            || (IsSymbol(Peek(), ".")
                && Peek(1).Kind is SqlTokenKind.Identifier or SqlTokenKind.Keyword
                && IsSymbol(Peek(2), "(")))
        {
            EnsureTemporalIdentifierDoesNotAllowParentheses(name);
            var call = ParseCallAfterName(name);
            if (TryParseMatchAgainstInfix(call, out var matchAgainstExpr))
            {
                expr = matchAgainstExpr;
                return true;
            }
            call = ParseWithinGroupOrderByIfPresent(call);

            // ✅ Window function: ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...)
            if (IsKeywordOrIdentifierWord(Peek(), "OVER"))
            {
                EnsureWindowFunctionSupport(call.Name);
                EnsureWindowFunctionArguments(call.Name, call.Args);

                Consume(); // OVER
                var spec = ParseWindowSpec();
                EnsureWindowSpecSupport(call.Name, spec);
                expr = new WindowFunctionExpr(call.Name, call.Args, spec, call.Distinct);
                return true;
            }

            expr = call;
            return true;
        }

        EnsureTemporalCallIdentifierRequiresParentheses(name);
        expr = ParseIdentifierChainOrColumn(name);
        return true;
    }

    private bool TryParseMatchAgainstInfix(CallExpr call, out SqlExpr expr)
    {
        expr = default!;
        if (!call.Name.Equals("MATCH", StringComparison.OrdinalIgnoreCase))
            return false;

        if (!IsKeywordOrIdentifierWord(Peek(), "AGAINST"))
            return false;

        EnsureMatchAgainstSupport();

        var againstToken = Consume(); // AGAINST
        ExpectSymbol("(");

        var payloadTokens = ReadTokensUntilMatchingParen(
            "AGAINST clause was not closed for MATCH(...).");
        ExpectSymbol(")");

        var (queryTokens, modeTokens) = SplitMatchAgainstPayload(payloadTokens);
        if (queryTokens.Count == 0)
            throw Error("MATCH ... AGAINST requires a search expression.", againstToken);

        var queryExpr = ParseStandaloneExpression(queryTokens, againstToken, "MATCH ... AGAINST search expression");

        var args = new List<SqlExpr>
        {
            new RowExpr(call.Args),
            queryExpr
        };

        if (modeTokens.Count > 0)
        {
            var modeSql = ParseAndValidateMatchAgainstMode(modeTokens, againstToken);
            args.Add(new RawSqlExpr(modeSql));
        }

        expr = new CallExpr("MATCH_AGAINST", args);
        return true;
    }

    private void EnsureMatchAgainstSupport()
    {
        if (_dialect.SupportsMatchAgainstPredicate)
            return;

        throw SqlUnsupported.ForDialect(_dialect, "MATCH ... AGAINST full-text predicate");
    }

    private string ParseAndValidateMatchAgainstMode(
        IReadOnlyList<SqlToken> modeTokens,
        SqlToken contextToken)
    {
        if (modeTokens.Count == 0)
            return string.Empty;

        var words = modeTokens
            .Select(t => t.Text.ToUpperInvariant())
            .ToArray();

        if (WordsEqual(words, "IN", "BOOLEAN", "MODE")
            || WordsEqual(words, "IN", "NATURAL", "LANGUAGE", "MODE")
            || WordsEqual(words, "IN", "NATURAL", "LANGUAGE", "MODE", "WITH", "QUERY", "EXPANSION")
            || WordsEqual(words, "WITH", "QUERY", "EXPANSION"))
            return string.Join(" ", modeTokens.Select(TokenToSql)).Trim();

        throw Error(
            "Unsupported AGAINST mode. Supported forms: IN BOOLEAN MODE, IN NATURAL LANGUAGE MODE, IN NATURAL LANGUAGE MODE WITH QUERY EXPANSION, WITH QUERY EXPANSION.",
            contextToken);
    }

    private static bool WordsEqual(IReadOnlyList<string> actual, params string[] expected)
    {
        if (actual.Count != expected.Length)
            return false;

        for (var i = 0; i < expected.Length; i++)
        {
            if (!actual[i].Equals(expected[i], StringComparison.Ordinal))
                return false;
        }

        return true;
    }

    private static (IReadOnlyList<SqlToken> QueryTokens, IReadOnlyList<SqlToken> ModeTokens) SplitMatchAgainstPayload(
        IReadOnlyList<SqlToken> payloadTokens)
    {
        if (payloadTokens.Count == 0)
            return ([], []);

        var depth = 0;
        var splitAt = -1;
        for (var i = 0; i < payloadTokens.Count; i++)
        {
            var token = payloadTokens[i];
            if (token.Kind == SqlTokenKind.Symbol && token.Text == "(")
            {
                depth++;
                continue;
            }

            if (token.Kind == SqlTokenKind.Symbol && token.Text == ")")
            {
                if (depth > 0)
                    depth--;
                continue;
            }

            if (depth != 0)
                continue;

            if (IsKeywordOrIdentifierWord(token, "IN")
                && i + 1 < payloadTokens.Count
                && (IsKeywordOrIdentifierWord(payloadTokens[i + 1], "BOOLEAN")
                    || IsKeywordOrIdentifierWord(payloadTokens[i + 1], "NATURAL")
                    || IsKeywordOrIdentifierWord(payloadTokens[i + 1], "QUERY")))
            {
                splitAt = i;
                break;
            }

            if (IsKeywordOrIdentifierWord(token, "WITH")
                && i + 1 < payloadTokens.Count
                && IsKeywordOrIdentifierWord(payloadTokens[i + 1], "QUERY"))
            {
                splitAt = i;
                break;
            }
        }

        if (splitAt < 0)
            return (payloadTokens, []);

        return (
            payloadTokens.Take(splitAt).ToArray(),
            payloadTokens.Skip(splitAt).ToArray());
    }

    private SqlExpr ParseStandaloneExpression(
        IReadOnlyList<SqlToken> tokens,
        SqlToken contextToken,
        string contextLabel)
    {
        try
        {
            var localTokens = tokens.Concat([SqlToken.EOF]).ToArray();
            var parser = new SqlExpressionParser(localTokens, _dialect);
            var parsed = parser.ParseExpression(0);
            parser.ExpectEnd();
            return parsed;
        }
        catch (Exception ex)
        {
            throw Error($"Invalid {contextLabel}: {ex.Message}", contextToken);
        }
    }

    /// <summary>
    /// EN: Prevents identifier-only temporal tokens from being called with parentheses in the active dialect.
    /// PT: Impede que tokens temporais somente-identificador sejam chamados com parênteses no dialeto ativo.
    /// </summary>
    /// <param name="identifier">EN: Function/token name parsed before call syntax. PT: Nome da função/token parseado antes da sintaxe de chamada.</param>
    private void EnsureTemporalIdentifierDoesNotAllowParentheses(string identifier)
    {
        if (!_dialect.TemporalFunctionIdentifierNames.Any(name => name.Equals(identifier, StringComparison.OrdinalIgnoreCase)))
            return;

        throw Error($"Temporal function token '{identifier}' must be used without parentheses.", Peek());
    }

    /// <summary>
    /// EN: Enforces parentheses for temporal identifiers that are call-only in the active dialect.
    /// PT: Exige parênteses para identificadores temporais que são apenas-invocáveis no dialeto ativo.
    /// </summary>
    /// <param name="identifier">EN: Identifier token parsed as a potential scalar expression. PT: Token identificador parseado como expressão escalar potencial.</param>
    private void EnsureTemporalCallIdentifierRequiresParentheses(string identifier)
    {
        if (_dialect.TemporalFunctionIdentifierNames.Any(name => name.Equals(identifier, StringComparison.OrdinalIgnoreCase)))
            return;

        if (!_dialect.TemporalFunctionCallNames.Any(name => name.Equals(identifier, StringComparison.OrdinalIgnoreCase)))
            return;

        throw Error($"Temporal function '{identifier}' requires parentheses '{identifier}()'.", Peek());
    }


    /// <summary>
    /// EN: Validates whether a window function name is supported by the current dialect/version.
    /// PT: Valida se o nome da função de janela é suportado pelo dialeto/versão atual.
    /// </summary>
    private void EnsureWindowFunctionSupport(string functionName)
    {
        if (!_dialect.SupportsWindowFunctions || !_dialect.SupportsWindowFunction(functionName))
            throw SqlUnsupported.ForDialect(_dialect, $"window functions ({functionName})");
    }



    /// <summary>
    /// EN: Validates argument count and basic literal semantics for supported window functions.
    /// PT: Valida a quantidade de argumentos e semântica literal básica para funções de janela suportadas.
    /// </summary>
    private void EnsureWindowFunctionArguments(string functionName, IReadOnlyList<SqlExpr> args)
    {
        var argCount = args.Count;
        if (argCount < 0)
            throw Error("Invalid window function argument count.", Peek());

        if (!_dialect.TryGetWindowFunctionArgumentArity(functionName, out var minArgs, out var maxArgs))
            return;

        if (minArgs == maxArgs && argCount != minArgs)
        {
            var message = minArgs == 0
                ? $"Window function '{functionName}' does not accept arguments."
                : $"Window function '{functionName}' requires exactly {minArgs} argument{(minArgs == 1 ? "" : "s")}.";
            throw Error(message, Peek());
        }

        if (argCount < minArgs || argCount > maxArgs)
            throw Error($"Window function '{functionName}' requires between {minArgs} and {maxArgs} arguments.", Peek());

        EnsureWindowFunctionArgumentLiteralRanges(functionName, args);
    }

    /// <summary>
    /// EN: Validates literal-only value ranges for selected window function arguments.
    /// PT: Valida intervalos de valores apenas para literais em argumentos selecionados de funções de janela.
    /// </summary>
    private void EnsureWindowFunctionArgumentLiteralRanges(string functionName, IReadOnlyList<SqlExpr> args)
    {
        if (functionName.Equals("NTILE", StringComparison.OrdinalIgnoreCase)
            && args.Count >= 1
            && TryReadIntegralLiteral(args[0], out var ntileBuckets)
            && ntileBuckets <= 0)
            throw Error("Window function 'NTILE' requires a positive bucket count.", Peek());

        if ((functionName.Equals("LAG", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("LEAD", StringComparison.OrdinalIgnoreCase))
            && args.Count >= 2
            && TryReadIntegralLiteral(args[1], out var lagLeadOffset)
            && lagLeadOffset < 0)
            throw Error($"Window function '{functionName}' requires a non-negative offset.", Peek());

        if (functionName.Equals("NTH_VALUE", StringComparison.OrdinalIgnoreCase)
            && args.Count >= 2
            && TryReadIntegralLiteral(args[1], out var nthIndex)
            && nthIndex <= 0)
            throw Error("Window function 'NTH_VALUE' requires position argument greater than zero.", Peek());
    }

    /// <summary>
    /// EN: Attempts to read an integer literal value from an expression argument.
    /// PT: Tenta ler um valor literal inteiro de um argumento de expressão.
    /// </summary>
    private static bool TryReadIntegralLiteral(SqlExpr expr, out long value)
    {
        value = default;
        if (expr is not LiteralExpr { Value: not null and not DBNull and IConvertible literalValue })
            return false;

        try
        {
            value = Convert.ToInt64(literalValue, CultureInfo.InvariantCulture);
            return true;
        }
        catch
        {
            return false;
        }
    }

    /// <summary>
    /// EN: Validates semantic window specification requirements for each supported function.
    /// PT: Valida os requisitos semânticos da especificação de janela para cada função suportada.
    /// </summary>
    private void EnsureWindowSpecSupport(string functionName, WindowSpec spec)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(spec, nameof(spec));

        if (_dialect.RequiresOrderByInWindowFunction(functionName) && spec.OrderBy.Count == 0)
            throw Error($"Window function '{functionName}' requires ORDER BY in OVER clause.", Peek());

        if (spec.Frame is not null)
            EnsureWindowFrameSemanticRange(spec.Frame);
    }

    /// <summary>
    /// EN: Validates whether a parsed window frame has a coherent start/end ordering.
    /// PT: Valida se um frame de janela interpretado possui ordenação coerente de início/fim.
    /// </summary>
    private void EnsureWindowFrameSemanticRange(WindowFrameSpec frame)
    {
        var startRank = GetWindowFrameBoundRank(frame.Start);
        var endRank = GetWindowFrameBoundRank(frame.End);

        if (startRank > endRank)
            throw Error("Window frame start bound cannot be greater than end bound.", Peek());
    }

    /// <summary>
    /// EN: Converts a window frame bound into an ordered rank for semantic comparison.
    /// PT: Converte um limite de frame de janela em rank ordenável para comparação semântica.
    /// </summary>
    private static long GetWindowFrameBoundRank(WindowFrameBound bound)
    {
        return bound.Kind switch
        {
            WindowFrameBoundKind.UnboundedPreceding => long.MinValue,
            WindowFrameBoundKind.Preceding => -bound.Offset.GetValueOrDefault(),
            WindowFrameBoundKind.CurrentRow => 0,
            WindowFrameBoundKind.Following => bound.Offset.GetValueOrDefault(),
            WindowFrameBoundKind.UnboundedFollowing => long.MaxValue,
            _ => 0
        };
    }

    private CallExpr ParseCallAfterName(string name)
    {
        if (IsSymbol(Peek(), ".")
            && Peek(1).Kind is SqlTokenKind.Identifier or SqlTokenKind.Keyword
            && IsSymbol(Peek(2), "("))
        {
            Consume(); // .
            name = Consume().Text;
        }

        if (name.Equals("JSON_VALUE", StringComparison.OrdinalIgnoreCase)
            && !_dialect.SupportsJsonValueFunction)
        {
            throw SqlUnsupported.ForDialect(_dialect, "JSON_VALUE");
        }

        if (name.Equals("JSON_QUERY", StringComparison.OrdinalIgnoreCase)
            && !_dialect.SupportsJsonQueryFunction)
        {
            throw SqlUnsupported.ForDialect(_dialect, "JSON_QUERY");
        }

        if (name.Equals("OPENJSON", StringComparison.OrdinalIgnoreCase)
            && !_dialect.SupportsOpenJsonFunction)
        {
            throw SqlUnsupported.ForDialect(_dialect, "OPENJSON");
        }

        if (name.Equals("JSON_TABLE", StringComparison.OrdinalIgnoreCase)
            && !_dialect.SupportsJsonTableFunction)
        {
            throw SqlUnsupported.ForDialect(_dialect, "JSON_TABLE");
        }

        if (name.Equals("JSON_EXTRACT", StringComparison.OrdinalIgnoreCase)
            && !_dialect.SupportsJsonExtractFunction)
        {
            throw SqlUnsupported.ForDialect(_dialect, "JSON_EXTRACT");
        }

        if ((name.Equals("DATE_ADD", StringComparison.OrdinalIgnoreCase)
                || name.Equals("DATEADD", StringComparison.OrdinalIgnoreCase)
                || name.Equals("TIMESTAMPADD", StringComparison.OrdinalIgnoreCase))
            && !_dialect.SupportsDateAddFunction(name))
        {
            throw SqlUnsupported.ForDialect(_dialect, name.ToUpperInvariant());
        }

        if ((name.Equals("GROUP_CONCAT", StringComparison.OrdinalIgnoreCase)
                || name.Equals("STRING_AGG", StringComparison.OrdinalIgnoreCase)
                || name.Equals("LISTAGG", StringComparison.OrdinalIgnoreCase))
            && !_dialect.SupportsStringAggregateFunction(name))
        {
            throw SqlUnsupported.ForDialect(_dialect, name.ToUpperInvariant());
        }

        if ((name.Equals("NEXTVAL", StringComparison.OrdinalIgnoreCase)
                || name.Equals("CURRVAL", StringComparison.OrdinalIgnoreCase)
                || name.Equals("SETVAL", StringComparison.OrdinalIgnoreCase)
                || name.Equals("LASTVAL", StringComparison.OrdinalIgnoreCase))
            && !_dialect.SupportsSequenceFunctionCall(name))
        {
            throw SqlUnsupported.ForDialect(_dialect, name.ToUpperInvariant());
        }

        if ((name.Equals("FOUND_ROWS", StringComparison.OrdinalIgnoreCase)
                || name.Equals("ROW_COUNT", StringComparison.OrdinalIgnoreCase)
                || name.Equals("CHANGES", StringComparison.OrdinalIgnoreCase)
                || name.Equals("ROWCOUNT", StringComparison.OrdinalIgnoreCase))
            && !_dialect.SupportsLastFoundRowsFunction(name))
        {
            throw SqlUnsupported.ForDialect(_dialect, name.ToUpperInvariant());
        }

        Consume(); // '('

        // ================================
        // CAST(expr AS TYPE) — sintaxe especial
        // ================================
        if (name.Equals("CAST", StringComparison.OrdinalIgnoreCase))
        {
            // expr
            var inner = ParseExpression(0);

            if (!IsKeywordOrIdentifierWord(Peek(), "AS"))
                throw Error("CAST requires AS", Peek());

            Consume(); // AS

            // lê o TYPE até fechar ')'
            var typeToks = new List<SqlToken>();
            int depth = 0;

            while (true)
            {
                var t = Peek();

                if (t.Kind == SqlTokenKind.EndOfFile)
                    throw Error("CAST type not closed", t);

                if (t.Kind == SqlTokenKind.Symbol && t.Text == "(")
                    depth++;

                if (t.Kind == SqlTokenKind.Symbol && t.Text == ")")
                {
                    if (depth == 0)
                        break;
                    depth--;
                }

                typeToks.Add(Consume());
            }

            ExpectSymbol(")");

            var typeSql = string.Join(" ",
                typeToks.Select(TokenToSql)
            ).Trim();

            return new CallExpr(
                "CAST",
                [
                inner,
                new RawSqlExpr(typeSql)
                ]
            );
        }

        // ================================
        // TRY_CAST(expr AS TYPE) — sintaxe especial
        // ================================
        if (name.Equals("TRY_CAST", StringComparison.OrdinalIgnoreCase))
        {
            var inner = ParseExpression(0);

            if (!IsKeywordOrIdentifierWord(Peek(), "AS"))
                throw Error("TRY_CAST requires AS", Peek());

            Consume(); // AS

            var typeToks = new List<SqlToken>();
            int depth = 0;

            while (true)
            {
                var t = Peek();

                if (t.Kind == SqlTokenKind.EndOfFile)
                    throw Error("TRY_CAST type not closed", t);

                if (t.Kind == SqlTokenKind.Symbol && t.Text == "(")
                    depth++;

                if (t.Kind == SqlTokenKind.Symbol && t.Text == ")")
                {
                    if (depth == 0)
                        break;
                    depth--;
                }

                typeToks.Add(Consume());
            }

            ExpectSymbol(")");

            var typeSql = string.Join(" ", typeToks.Select(TokenToSql)).Trim();
            return new CallExpr("TRY_CAST", [inner, new RawSqlExpr(typeSql)]);
        }

        // ================================
        // Funções normais
        // ================================

        var distinct = false;
        if (IsKeywordOrIdentifierWord(Peek(), "DISTINCT"))
        {
            Consume();
            distinct = true;

            // MySQL does not allow duplicated DISTINCT in functions: COUNT(DISTINCT DISTINCT id)
            if (IsKeywordOrIdentifierWord(Peek(), "DISTINCT"))
                throw Error("duplicated DISTINCT", Peek());

            if (IsSymbol(Peek(), ")"))
                throw Error("DISTINCT requires an expression", Peek());
        }

        var args = new List<SqlExpr>();
        if (!IsSymbol(Peek(), ")"))
        {
            while (true)
            {
                // MySQL: DATE_ADD(x, INTERVAL 1 DAY) etc.
                if (IsKeywordOrIdentifierWord(Peek(), "INTERVAL"))
                {
                    Consume(); // INTERVAL
                    var n = ParseExpression(0);
                    // unit (DAY/HOUR/...)
                    var unitTok = Peek();
                    if (!(unitTok.Kind == SqlTokenKind.Identifier || unitTok.Kind == SqlTokenKind.Keyword))
                        throw Error("INTERVAL requires unit", unitTok);
                    Consume();
                    args.Add(new CallExpr("INTERVAL", [n, new RawSqlExpr(unitTok.Text)]));
                }
                else
                {
                    args.Add(ShouldUseNativeStringAggregateArgumentBoundaries(name)
                        ? ParseStringAggregateFunctionArgument(name)
                        : ParseExpression(0));
                }

                if (!IsSymbol(Peek(), ","))
                    break;

                Consume();
            }

            // Oracle: JSON_VALUE(json_doc, path RETURNING NUMBER)
            if (name.Equals("JSON_VALUE", StringComparison.OrdinalIgnoreCase)
                && IsKeywordOrIdentifierWord(Peek(), "RETURNING"))
            {
                if (!_dialect.SupportsJsonValueReturningClause)
                    throw SqlUnsupported.ForDialect(_dialect, "JSON_VALUE ... RETURNING");

                Consume(); // RETURNING

                var typeToks = new List<SqlToken>();
                int depth = 0;

                while (true)
                {
                    var t = Peek();

                    if (t.Kind == SqlTokenKind.EndOfFile)
                        throw Error("JSON_VALUE RETURNING type not closed", t);

                    if (t.Kind == SqlTokenKind.Symbol && t.Text == "(")
                        depth++;

                    if (t.Kind == SqlTokenKind.Symbol && t.Text == ")")
                    {
                        if (depth == 0)
                            break;
                        depth--;
                    }

                    typeToks.Add(Consume());
                }

                var typeSql = string.Join(" ", typeToks.Select(TokenToSql)).Trim();
                args.Add(new RawSqlExpr($"RETURNING {typeSql}"));
            }
        }

        var aggregateOrderBy = ParseAggregateOrderByInsideCallIfPresent(name);
        ParseAggregateSeparatorKeywordIfPresent(name, args);

        ExpectSymbol(")");
        return new CallExpr(name, args, distinct, aggregateOrderBy);
    }

    private IReadOnlyList<WindowOrderItem>? ParseAggregateOrderByInsideCallIfPresent(string functionName)
    {
        if (!IsKeywordOrIdentifierWord(Peek(), "ORDER"))
            return null;

        var normalizedName = functionName.ToUpperInvariant();
        if (normalizedName is not "GROUP_CONCAT" and not "STRING_AGG" and not "LISTAGG")
            throw SqlUnsupported.ForDialect(_dialect, $"aggregate ORDER BY for function '{functionName}'");

        if (!_dialect.SupportsAggregateOrderByForStringAggregates)
            throw SqlUnsupported.ForDialect(_dialect, "aggregate ORDER BY");

        if (!_dialect.SupportsAggregateOrderByStringAggregateFunction(functionName))
            throw SqlUnsupported.ForDialect(_dialect, $"aggregate ORDER BY for function '{functionName}'");

        Consume(); // ORDER
        if (!IsKeywordOrIdentifierWord(Peek(), "BY"))
            throw Error("aggregate ORDER BY requires BY", Peek());
        Consume();

        var allowSeparatorTerminator =
            _dialect.SupportsAggregateSeparatorKeywordForStringAggregates
            && _dialect.SupportsAggregateSeparatorKeywordStringAggregateFunction(functionName);

        var orderBy = ParseStringAggregateOrderByItems("aggregate ORDER BY", allowSeparatorTerminator);

        if (allowSeparatorTerminator
            && IsKeywordOrIdentifierWord(Peek(), "SEPARATOR")
            && IsSymbol(Peek(1), ")"))
        {
            return orderBy;
        }

        return orderBy;
    }

    private bool ShouldUseNativeStringAggregateArgumentBoundaries(string functionName)
        => _dialect.SupportsAggregateOrderByStringAggregateFunction(functionName)
            || _dialect.SupportsAggregateSeparatorKeywordStringAggregateFunction(functionName);

    private void ParseAggregateSeparatorKeywordIfPresent(string functionName, List<SqlExpr> args)
    {
        if (!IsKeywordOrIdentifierWord(Peek(), "SEPARATOR"))
            return;

        var normalizedName = functionName.ToUpperInvariant();
        if (normalizedName is not "GROUP_CONCAT" and not "STRING_AGG" and not "LISTAGG")
            throw SqlUnsupported.ForDialect(_dialect, $"aggregate separator keyword for function '{functionName}'");

        if (!_dialect.SupportsAggregateSeparatorKeywordForStringAggregates)
            throw SqlUnsupported.ForDialect(_dialect, "aggregate separator keyword");

        if (!_dialect.SupportsAggregateSeparatorKeywordStringAggregateFunction(functionName))
            throw SqlUnsupported.ForDialect(_dialect, $"aggregate separator keyword for function '{functionName}'");

        Consume(); // SEPARATOR

        if (IsSymbol(Peek(), ")"))
            throw Error("aggregate separator keyword requires an expression", Peek());

        var separatorExpr = ParseExpression(0);
        if (args.Count == 0)
        {
            args.Add(separatorExpr);
            return;
        }

        if (args.Count == 1)
        {
            args.Add(separatorExpr);
            return;
        }

        args[1] = separatorExpr;
    }

    private SqlExpr ParseStringAggregateFunctionArgument(string functionName)
    {
        var start = _i;
        var depth = 0;

        while (true)
        {
            var token = Peek();

            if (token.Kind == SqlTokenKind.EndOfFile)
                throw Error($"function '{functionName}' argument not closed", token);

            if (depth == 0)
            {
                if (IsSymbol(token, ",")
                    || IsSymbol(token, ")")
                    || (_dialect.SupportsAggregateOrderByStringAggregateFunction(functionName)
                        && IsKeywordOrIdentifierWord(token, "ORDER"))
                    || (_dialect.SupportsAggregateSeparatorKeywordStringAggregateFunction(functionName)
                        && IsKeywordOrIdentifierWord(token, "SEPARATOR")))
                {
                    break;
                }
            }

            if (IsSymbol(token, "("))
            {
                depth++;
                Consume();
                continue;
            }

            if (IsSymbol(token, ")"))
            {
                if (depth == 0)
                    break;

                depth--;
                Consume();
                continue;
            }

            Consume();
        }

        if (_i == start)
            throw Error($"function '{functionName}' requires an expression", Peek());

        var sql = string.Join(" ", _toks.Skip(start).Take(_i - start).Select(TokenToSql)).Trim();
        return ParseScalar(sql, _dialect, _parameters);
    }

    private CallExpr ParseWithinGroupOrderByIfPresent(CallExpr call)
    {
        if (!IsKeywordOrIdentifierWord(Peek(), "WITHIN"))
            return call;

        var normalizedName = call.Name.ToUpperInvariant();
        if (normalizedName is not "GROUP_CONCAT" and not "STRING_AGG" and not "LISTAGG")
        {
            throw SqlUnsupported.ForDialect(
                _dialect,
                $"ordered-set aggregate syntax WITHIN GROUP for function '{call.Name}'");
        }

        if (!_dialect.SupportsWithinGroupForStringAggregates)
            throw SqlUnsupported.ForDialect(_dialect, "ordered-set aggregate syntax WITHIN GROUP");

        if (!_dialect.SupportsWithinGroupStringAggregateFunction(call.Name))
            throw SqlUnsupported.ForDialect(_dialect, $"ordered-set aggregate syntax WITHIN GROUP for function '{call.Name}'");

        Consume(); // WITHIN
        ExpectWord("GROUP");
        ExpectSymbol("(");

        if (!IsKeywordOrIdentifierWord(Peek(), "ORDER"))
            throw Error("WITHIN GROUP requires ORDER BY", Peek());
        Consume();

        if (!IsKeywordOrIdentifierWord(Peek(), "BY"))
            throw Error("WITHIN GROUP requires ORDER BY", Peek());
        Consume();

        var orderBy = ParseStringAggregateOrderByItems("WITHIN GROUP ORDER BY");

        ExpectSymbol(")");
        return call with { WithinGroupOrderBy = orderBy };
    }

    private List<WindowOrderItem> ParseStringAggregateOrderByItems(string context, bool allowSeparatorTerminator = false)
    {
        var start = _i;
        var depth = 0;

        while (true)
        {
            var token = Peek();

            if (token.Kind == SqlTokenKind.EndOfFile)
                throw Error($"{context} expression not closed", token);

            if (depth == 0)
            {
                if (IsSymbol(token, ")")
                    || (allowSeparatorTerminator && IsKeywordOrIdentifierWord(token, "SEPARATOR")))
                {
                    break;
                }
            }

            if (IsSymbol(token, "("))
            {
                depth++;
                Consume();
                continue;
            }

            if (IsSymbol(token, ")"))
            {
                if (depth == 0)
                    break;

                depth--;
                Consume();
                continue;
            }

            Consume();
        }

        var payloadTokens = _toks.Skip(start).Take(_i - start).ToList();
        if (payloadTokens.Count == 0)
            throw Error($"{context} requires at least one expression", Peek());

        var items = new List<List<SqlToken>>();
        var current = new List<SqlToken>();
        depth = 0;

        foreach (var token in payloadTokens)
        {
            if (depth == 0 && IsSymbol(token, ","))
            {
                if (current.Count == 0)
                    throw Error($"{context} has an unexpected comma before expression", token);

                items.Add(current);
                current = [];
                continue;
            }

            if (IsSymbol(token, "("))
                depth++;
            else if (IsSymbol(token, ")"))
                depth--;

            current.Add(token);
        }

        if (current.Count == 0)
            throw Error($"{context} has a trailing comma without expression", Peek());

        items.Add(current);

        var orderBy = new List<WindowOrderItem>(items.Count);
        foreach (var itemTokens in items)
        {
            var desc = false;
            if (itemTokens.Count > 0 && IsKeywordOrIdentifierWord(itemTokens[^1], "DESC"))
            {
                desc = true;
                itemTokens.RemoveAt(itemTokens.Count - 1);
            }
            else if (itemTokens.Count > 0 && IsKeywordOrIdentifierWord(itemTokens[^1], "ASC"))
            {
                itemTokens.RemoveAt(itemTokens.Count - 1);
            }

            if (itemTokens.Count == 0)
                throw Error($"{context} requires at least one expression", Peek());

            for (var i = 0; i < itemTokens.Count; i++)
            {
                if (!IsKeywordOrIdentifierWord(itemTokens[i], "ASC")
                    && !IsKeywordOrIdentifierWord(itemTokens[i], "DESC"))
                {
                    continue;
                }

                throw Error($"{context} requires commas between expressions", itemTokens[i]);
            }

            var sql = string.Join(" ", itemTokens.Select(TokenToSql)).Trim();
            orderBy.Add(new WindowOrderItem(ParseScalar(sql, _dialect, _parameters), desc));
        }

        return orderBy;
    }

    private WindowSpec ParseWindowSpec()
    {
        // OVER ( ... )
        ExpectSymbol("(");

        var parts = new List<SqlExpr>();
        var order = new List<WindowOrderItem>();
        WindowFrameSpec? frame = null;

        // PARTITION BY ...
        if (IsKeywordOrIdentifierWord(Peek(), "PARTITION"))
        {
            Consume(); // PARTITION
            if (!IsKeywordOrIdentifierWord(Peek(), "BY"))
                throw Error("Esperava BY após PARTITION", Peek());
            Consume(); // BY

            parts.AddRange(ParseExprListUntilOrderOrParenClose());
        }

        // ORDER BY ...
        if (IsKeywordOrIdentifierWord(Peek(), "ORDER"))
        {
            Consume(); // ORDER
            if (!IsKeywordOrIdentifierWord(Peek(), "BY"))
                throw Error("Esperava BY após ORDER", Peek());
            Consume(); // BY

            while (true)
            {
                var e = ParseExpression(0);

                var desc = false;
                if (IsKeywordOrIdentifierWord(Peek(), "DESC"))
                {
                    Consume();
                    desc = true;
                }
                else if (IsKeywordOrIdentifierWord(Peek(), "ASC"))
                {
                    Consume();
                }

                order.Add(new WindowOrderItem(e, desc));

                if (!IsSymbol(Peek(), ","))
                    break;
                Consume();
            }
        }

        if (IsKeywordOrIdentifierWord(Peek(), "ROWS")
            || IsKeywordOrIdentifierWord(Peek(), "RANGE")
            || IsKeywordOrIdentifierWord(Peek(), "GROUPS"))
        {
            if (!_dialect.SupportsWindowFrameClause)
                throw SqlUnsupported.ForDialect(_dialect, "window frame clause (ROWS/RANGE/GROUPS)");

            frame = ParseWindowFrameClause();
        }

        ExpectSymbol(")");
        return new WindowSpec(parts, order, frame);
    }

    /// <summary>
    /// EN: Parses a SQL window frame clause (ROWS/RANGE/GROUPS).
    /// PT: Faz o parse de uma cláusula de frame de janela SQL (ROWS/RANGE/GROUPS).
    /// </summary>
    private WindowFrameSpec ParseWindowFrameClause()
    {
        var unit = ParseWindowFrameUnit();

        WindowFrameBound start;
        WindowFrameBound end;
        if (IsKeywordOrIdentifierWord(Peek(), "BETWEEN"))
        {
            Consume(); // BETWEEN
            start = ParseWindowFrameBound();
            if (!IsKeywordOrIdentifierWord(Peek(), "AND"))
                throw Error("Expected AND in window frame clause.", Peek());
            Consume(); // AND
            end = ParseWindowFrameBound();
        }
        else
        {
            start = ParseWindowFrameBound();
            end = new WindowFrameBound(WindowFrameBoundKind.CurrentRow, null);
        }

        return new WindowFrameSpec(unit, start, end);
    }

    private WindowFrameUnit ParseWindowFrameUnit()
    {
        if (IsKeywordOrIdentifierWord(Peek(), "ROWS"))
        {
            Consume();
            return WindowFrameUnit.Rows;
        }

        if (IsKeywordOrIdentifierWord(Peek(), "RANGE"))
        {
            Consume();
            return WindowFrameUnit.Range;
        }

        if (IsKeywordOrIdentifierWord(Peek(), "GROUPS"))
        {
            Consume();
            return WindowFrameUnit.Groups;
        }

        throw Error("Expected ROWS, RANGE or GROUPS in window frame clause.", Peek());
    }

    private WindowFrameBound ParseWindowFrameBound()
    {
        if (IsKeywordOrIdentifierWord(Peek(), "UNBOUNDED"))
        {
            Consume();
            if (IsKeywordOrIdentifierWord(Peek(), "PRECEDING"))
            {
                Consume();
                return new WindowFrameBound(WindowFrameBoundKind.UnboundedPreceding, null);
            }

            if (IsKeywordOrIdentifierWord(Peek(), "FOLLOWING"))
            {
                Consume();
                return new WindowFrameBound(WindowFrameBoundKind.UnboundedFollowing, null);
            }

            throw Error("Expected PRECEDING or FOLLOWING after UNBOUNDED in window frame clause.", Peek());
        }

        if (IsKeywordOrIdentifierWord(Peek(), "CURRENT"))
        {
            Consume();
            if (!IsKeywordOrIdentifierWord(Peek(), "ROW"))
                throw Error("Expected ROW after CURRENT in window frame clause.", Peek());
            Consume();
            return new WindowFrameBound(WindowFrameBoundKind.CurrentRow, null);
        }

        var boundExpr = ParseExpression(0);
        if (!TryReadIntegralLiteral(boundExpr, out var offset) || offset < 0 || offset > int.MaxValue)
            throw Error("Expected a non-negative integer literal in window frame bound.", Peek());

        if (IsKeywordOrIdentifierWord(Peek(), "PRECEDING"))
        {
            Consume();
            return new WindowFrameBound(WindowFrameBoundKind.Preceding, (int)offset);
        }

        if (IsKeywordOrIdentifierWord(Peek(), "FOLLOWING"))
        {
            Consume();
            return new WindowFrameBound(WindowFrameBoundKind.Following, (int)offset);
        }

        throw Error("Expected PRECEDING or FOLLOWING in window frame bound.", Peek());
    }

    private IReadOnlyList<SqlExpr> ParseExprListUntilOrderOrParenClose()
    {
        var items = new List<SqlExpr>();

        if (IsSymbol(Peek(), ")") || IsKeywordOrIdentifierWord(Peek(), "ORDER"))
            return items;

        while (true)
        {
            items.Add(ParseExpression(0));

            if (IsSymbol(Peek(), ","))
            {
                Consume();
                continue;
            }

            break;
        }

        return items;
    }

    private SqlExpr ParseIdentifierChainOrColumn(string first)
    {
        var parts = new List<string> { first };

        while (IsSymbol(Peek(), "."))
        {
            Consume(); // '.'
            var t = Peek();

            // ✅ suporta alias.* (asterisco é Operator no tokenizer)
            if ((t.Kind == SqlTokenKind.Operator || t.Kind == SqlTokenKind.Symbol) && t.Text == "*")
            {
                Consume();
                parts.Add("*");
                break;
            }

            if (t.Kind != SqlTokenKind.Identifier && t.Kind != SqlTokenKind.Keyword)
                throw Error("Esperava identificador após '.'", t);

            parts.Add(Consume().Text);
        }

        if (TryBuildSequenceDotCall(parts, out var sequenceCall))
            return sequenceCall;

        return parts.Count switch
        {
            1 => new IdentifierExpr(parts[0]),                 // col
            2 => new ColumnExpr(parts[0], parts[1]),           // alias.col  OR table.col  OR alias.*
            _ => new RawSqlExpr(string.Join(".", parts))       // db.table.col (por enquanto)
        };
    }

    private bool TryBuildSequenceDotCall(
        IReadOnlyList<string> parts,
        out SqlExpr expr)
    {
        expr = default!;
        if (parts.Count < 2)
            return false;

        var suffix = parts[^1];
        if (!suffix.Equals("NEXTVAL", StringComparison.OrdinalIgnoreCase)
            && !suffix.Equals("CURRVAL", StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        if (!_dialect.SupportsSequenceDotValueExpression(suffix))
            throw SqlUnsupported.ForDialect(_dialect, suffix.ToUpperInvariant());

        var targetParts = parts.Take(parts.Count - 1).ToArray();
        SqlExpr target = targetParts.Length switch
        {
            1 => new IdentifierExpr(targetParts[0]),
            2 => new ColumnExpr(targetParts[0], targetParts[1]),
            _ => new RawSqlExpr(string.Join(".", targetParts))
        };

        expr = new CallExpr(suffix.ToUpperInvariant(), [target]);
        return true;
    }

    #endregion

    private SqlToken Peek(int offset = 0)
    {
        var idx = _i + offset;
        if (idx < 0) idx = 0;
        return idx < _toks.Count ? _toks[idx] : SqlToken.EOF;
    }

    private SqlToken Consume() => _toks[_i++];

    private void ExpectSymbol(string sym)
    {
        var t = Peek();
        if (t.Kind != SqlTokenKind.Symbol || t.Text != sym)
            throw Error($"Esperava símbolo '{sym}', veio {t.Kind} '{t.Text}'", t);
        _i++;
    }

    private void ExpectWord(string word)
    {
        var t = Peek();
        if (!IsKeywordOrIdentifierWord(t, word))
            throw Error($"Esperava palavra '{word}', veio {t.Kind} '{t.Text}'", t);
        _i++;
    }

    private void ExpectEnd()
    {
        var t = Peek();
        if (t.Kind != SqlTokenKind.EndOfFile)
            throw Error($"Esperava fim da expressão, veio {t.Kind} '{t.Text}'", t);
        _i++;
    }

    private SubqueryExpr ParseAndWrapSubquery(
        string subSql,
        SqlToken contextToken,
        string contextLabel)
    {
        try
        {
            return SqlQueryParser.ParseSubqueryExprOrThrow(subSql, contextToken, contextLabel, _dialect);
        }
        catch (Exception ex)
        {
            // mantém teu padrão de erro com token/posição
            throw Error($"Subquery inválida ({contextLabel}): {ex.Message}", contextToken);
        }
    }

    private static bool IsKeyword(SqlToken t, string kw)
        => t.Kind == SqlTokenKind.Keyword && t.Text.Equals(kw, StringComparison.OrdinalIgnoreCase);

    private static bool IsSymbol(SqlToken t, string sym)
        => t.Kind == SqlTokenKind.Symbol && t.Text == sym;

    private static InvalidOperationException Error(string msg, SqlToken t)
        => new($"{msg} (pos {t.Position})");

    private bool TryMapComparisonOp(string op, out SqlBinaryOp bop)
    {
        // Dialect can define extra operators (ex: MySQL <=>).
        if (_dialect.TryMapBinaryOperator(op, out bop))
        {
            if (bop == SqlBinaryOp.NullSafeEq)
                return _dialect.SupportsNullSafeEq;

            return bop is SqlBinaryOp.Eq
                or SqlBinaryOp.Neq
                or SqlBinaryOp.Greater
                or SqlBinaryOp.GreaterOrEqual
                or SqlBinaryOp.Less
                or SqlBinaryOp.LessOrEqual;
        }

        bop = default;
        return false;
    }

    private static bool IsKeywordOrIdentifierWord(SqlToken t, string word)
        => t.Text.Equals(word, StringComparison.OrdinalIgnoreCase);

    private SqlExpr TryConsumeTrailingLikeEscape(SqlExpr expr)
    {
        if (!_dialect.SupportsLikeEscapeClause || !IsKeywordOrIdentifierWord(Peek(), "ESCAPE"))
            return expr;

        return expr switch
        {
            LikeExpr like when like.Escape is null => TryParseLikeEscape(like, 51),
            UnaryExpr { Op: SqlUnaryOp.Not, Expr: LikeExpr like } unary when like.Escape is null
                => unary with { Expr = TryParseLikeEscape(like, 51) },
            _ => expr
        };
    }

    private LikeExpr TryParseLikeEscape(LikeExpr like, int rbp)
    {
        if (!_dialect.SupportsLikeEscapeClause || !IsKeywordOrIdentifierWord(Peek(), "ESCAPE"))
            return like;

        var escapeToken = Peek();
        Consume(); // ESCAPE
        var escape = ParseExpression(rbp);
        ValidateLikeEscapeExpression(escape, escapeToken);
        return like with { Escape = escape };
    }

    private void ValidateLikeEscapeExpression(SqlExpr escape, SqlToken escapeToken)
    {
        if (_dialect.LikeEscapeExpressionMustBeSingleCharacter
            && escape is LiteralExpr { Value: string escapeText }
            && escapeText.Length != 1)
        {
            throw Error("LIKE ESCAPE requires a single character expression.", escapeToken);
        }

        if (_dialect.LikeEscapeExpressionMustBeSingleCharacter
            && escape is ParameterExpr parameterEscape
            && TryResolveParameterString(parameterEscape.Name, out var parameterEscapeText)
            && parameterEscapeText is not null
            && parameterEscapeText.Length != 1)
        {
            throw Error("LIKE ESCAPE requires a single character expression.", escapeToken);
        }
    }

    private bool TryResolveParameterString(string parameterToken, out string? value)
    {
        value = null;
        if (_parameters is null)
            return false;

        var normalized = parameterToken.TrimStart('@', ':', '?');
        foreach (IDataParameter parameter in _parameters)
        {
            var name = (parameter.ParameterName ?? string.Empty).TrimStart('@', ':', '?');
            if (!string.Equals(name, normalized, StringComparison.OrdinalIgnoreCase))
                continue;

            if (parameter.Value is null || parameter.Value == DBNull.Value)
                return true;

            value = Convert.ToString(parameter.Value, CultureInfo.InvariantCulture);
            return true;
        }

        return false;
    }

    private string ReadRawUntilMatchingParen()
    {
        // Lê tokens até o ')' que fecha o nível atual de parênteses (depth começa em 1 fora daqui)
        // Aqui estamos logo após "IN (" e talvez um "(" extra já consumido.
        // Vamos ler até encontrar ')' no depth 0 relativo.
        int depth = 0;
        var parts = new List<string>();

        while (true)
        {
            var t = Peek();

            if (t.Kind == SqlTokenKind.EndOfFile)
                throw Error("Subquery não fechada dentro de IN(...)", t);

            // controla parênteses
            if (t.Kind == SqlTokenKind.Symbol && t.Text == "(") depth++;
            if (t.Kind == SqlTokenKind.Symbol && t.Text == ")")
            {
                if (depth == 0) break; // para ANTES do ')'
                depth--;
            }

            parts.Add(TokenToSql(t));
            Consume();
        }

        return string.Join(" ", parts).Trim();
    }

    private IReadOnlyList<SqlToken> ReadTokensUntilMatchingParen(string eofError)
    {
        var depth = 0;
        var tokens = new List<SqlToken>();
        while (true)
        {
            var t = Peek();
            if (t.Kind == SqlTokenKind.EndOfFile)
                throw Error(eofError, t);

            if (t.Kind == SqlTokenKind.Symbol && t.Text == "(")
                depth++;

            if (t.Kind == SqlTokenKind.Symbol && t.Text == ")")
            {
                if (depth == 0)
                    break;
                depth--;
            }

            tokens.Add(t);
            Consume();
        }

        return tokens;
    }

    private static string TokenToSql(SqlToken t)
    {
        // reconstrói SQL “ok” pra debug/parse posterior
        return t.Kind switch
        {
            SqlTokenKind.String => $"'{t.Text.Replace("'", "\\'")}'",
            _ => t.Text
        };
    }
}
