namespace DbSqlLikeMem;

internal sealed class SqlExpressionParser(SqlExpressionParserContext context)
{
    private readonly SqlExpressionParserContext _context = context
        ?? throw new ArgumentNullException(nameof(context));
    private int _i
    {
        get => _context.Index;
        set => _context.Index = value;
    }

    /// <summary>
    /// EN: Parses a WHERE expression using the provided dialect and no command parameters.
    /// PT-br: Faz o parsing de uma expressao WHERE usando o dialeto informado e sem parametros de comando.
    /// </summary>
    /// <param name="whereSql">EN: WHERE expression text. PT-br: Texto da expressao WHERE.</param>
    /// <param name="db"></param>
    /// <param name="dialect">EN: Dialect that controls tokenizer/parser behavior and feature gates. PT-br: Dialeto que controla o comportamento do tokenizer/parser e os gates de recursos.</param>
    /// <returns>EN: Parsed expression AST. PT-br: AST da expressao parseada.</returns>
    public static SqlExpr ParseWhere(
        string whereSql,
        DbMock db,
        ISqlDialect dialect)
        => ParseWhere(whereSql, db, dialect, null);

    /// <summary>
    /// EN: Parses a WHERE expression using the provided dialect, parameters, and optional custom function resolver.
    /// PT-br: Faz o parsing de uma expressao WHERE usando o dialeto informado, parametros e um resolvedor opcional de funcoes customizadas.
    /// </summary>
    /// <param name="whereSql">EN: WHERE expression text. PT-br: Texto da expressao WHERE.</param>
    /// <param name="db"></param>
    /// <param name="dialect">EN: Dialect that controls tokenizer/parser behavior and feature gates. PT-br: Dialeto que controla o comportamento do tokenizer/parser e os gates de recursos.</param>
    /// <param name="parameters">EN: Optional command parameters used by parser paths that resolve parameterized values. PT-br: Parametros de comando opcionais usados por caminhos do parser que resolvem valores parametrizados.</param>
    /// <param name="customFunctionSupported">EN: Optional custom function resolver used to accept schema-defined functions during validation. PT-br: Resolver opcional de funcoes customizadas usado para aceitar funcoes definidas no schema durante a validacao.</param>
    /// <returns>EN: Parsed expression AST. PT-br: AST da expressao parseada.</returns>
    public static SqlExpr ParseWhere(
        string whereSql,
        DbMock db,
        ISqlDialect dialect,
        IDataParameterCollection? parameters,
        Func<string, bool>? customFunctionSupported = null)
        => ParseCore(whereSql, db, dialect, parameters, customFunctionSupported);

    /// <summary>
    /// EN: Parses a scalar expression using the provided dialect and no command parameters.
    /// PT-br: Faz o parsing de uma expressao escalar usando o dialeto informado e sem parametros de comando.
    /// </summary>
    /// <param name="sql">EN: Scalar SQL expression to parse. PT-br: Expressao SQL escalar para parsear.</param>
    /// <param name="db"></param>
    /// <param name="dialect">EN: Dialect that controls tokenizer/parser behavior and feature gates. PT-br: Dialeto que controla o comportamento do tokenizer/parser e os gates de recursos.</param>
    /// <returns>EN: Parsed expression AST. PT-br: AST da expressao parseada.</returns>
    public static SqlExpr ParseScalar(
        string sql,
        DbMock db,
        ISqlDialect dialect)
        => ParseScalar(sql, db, dialect, null);

    /// <summary>
    /// EN: Parses a scalar expression using the provided dialect and no command parameters.
    /// PT-br: Faz o parsing de uma expressao escalar usando o dialeto informado e sem parametros de comando.
    /// </summary>
    /// <param name="sql">EN: Scalar SQL expression to parse. PT-br: Expressao SQL escalar para parsear.</param>
    /// <param name="db"></param>
    /// <returns>EN: Parsed expression AST. PT-br: AST da expressao parseada.</returns>
    public static SqlExpr ParseScalar(
        string sql,
        DbMock db)
        => ParseScalar(sql, db, AutoDialectFactory.Create(), null);

    /// <summary>
    /// EN: Parses a scalar expression using the provided dialect, parameters, and optional custom function resolver.
    /// PT-br: Faz o parsing de uma expressao escalar usando o dialeto informado, parametros e um resolvedor opcional de funcoes customizadas.
    /// </summary>
    /// <param name="sql">EN: Scalar SQL expression to parse. PT-br: Expressao SQL escalar para parsear.</param>
    /// <param name="db"></param>
    /// <param name="dialect">EN: Dialect that controls tokenizer/parser behavior and feature gates. PT-br: Dialeto que controla o comportamento do tokenizer/parser e os gates de recursos.</param>
    /// <param name="parameters">EN: Optional command parameters used by parser paths that resolve parameterized values. PT-br: Parametros de comando opcionais usados por caminhos do parser que resolvem valores parametrizados.</param>
    /// <param name="customFunctionSupported">EN: Optional custom function resolver used to accept schema-defined functions during validation. PT-br: Resolver opcional de funcoes customizadas usado para aceitar funcoes definidas no schema durante a validacao.</param>
    /// <returns>EN: Parsed expression AST. PT-br: AST da expressao parseada.</returns>
    public static SqlExpr ParseScalar(
        string sql,
        DbMock db,
        ISqlDialect dialect,
        IDataParameterCollection? parameters,
        Func<string, bool>? customFunctionSupported = null)
        => ParseCore(sql, db, dialect, parameters, customFunctionSupported);

    private static SqlExpr ParseCore(
        string sql,
        DbMock db,
        ISqlDialect dialect,
        IDataParameterCollection? parameters,
        Func<string, bool>? customFunctionSupported)
    {
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(sql, nameof(sql));
        ArgumentNullExceptionCompatible.ThrowIfNull(dialect, nameof(dialect));
        var toks = new SqlTokenizer(sql, dialect).Tokenize();
        EnsureJsonArrowSupport(toks, dialect);
        var ctx = new SqlExpressionParserContext(toks, db, dialect, parameters, customFunctionSupported);
        var p = new SqlExpressionParser(ctx);
        var expr = p.ParseExpression(0);
        expr = p.TryConsumeTrailingLikeEscape(expr);
        p._context.ExpectEnd();
        return expr;
    }

    private static void EnsureJsonArrowSupport(IReadOnlyList<SqlToken> toks, ISqlDialect dialect)
    {
        if (dialect.SupportsJsonArrowOperators || dialect.AllowsParserCrossDialectJsonOperators)
            return;

        foreach (var token in toks)
        {
            if (token.Kind == SqlTokenKind.Operator
                && (token.Text == "->" || token.Text == "->>" || token.Text == "#>" || token.Text == "#>>"))
            {
                throw SqlUnsupported.NotSupported(dialect, "JSON -> / ->> / #> / #>> operators");
            }
        }
    }

    // Pratt: parse com binding power
    /// <summary>
    /// EN: Implements ParseExpression.
    /// PT-br: Implementa ParseExpression.
    /// </summary>
    public SqlExpr ParseExpression(int minBp)
        => ParseExpression(minBp, allowIsNullPostfix: true);

    private SqlExpr ParseExpression(int minBp, bool allowIsNullPostfix)
    {
        var left = ParsePrefix();

        while (true)
        {
            // postfix: IS [NOT] NULL
            if (allowIsNullPostfix && TryParseIsNullPostfix(ref left)) continue;

            // NOT IN / NOT LIKE / NOT REGEXP / NOT BETWEEN
            if (TryParseNotInfix(ref left, minBp)) continue;

            // IN (...)
            if (TryParseInInfix(ref left, minBp)) continue;

            // LIKE
            if (TryParseLikeInfix(ref left, minBp)) continue;

            // SOUNDS LIKE
            if (TryParseSoundsLikeInfix(ref left, minBp)) continue;

            // REGEXP
            if (TryParseRegexpInfix(ref left, minBp)) continue;

            // PostgreSQL-style type cast: expr::type
            if (TryParseTypeCastInfix(ref left, minBp)) continue;

            // JSON -> / ->>
            if ((_context.Dialect.SupportsJsonArrowOperators || _context.Dialect.AllowsParserCrossDialectJsonOperators)
                && TryParseJsonArrowInfix(ref left, minBp)) continue;

            // * /
            if (TryParseMulDivInfix(ref left, minBp)) continue;

            // + -
            if (TryParseAddSubInfix(ref left, minBp)) continue;

            // BETWEEN / NOT BETWEEN (NOT BETWEEN é coberto no TryParseNotInfix)
            if (TryParseBetweenInfix(ref left, minBp)) continue;

            // MEMBER OF
            if (TryParseMemberOfInfix(ref left, minBp)) continue;

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
        var t = _context.Peek();
        if (!SqlExpressionParserContext.IsKeyword(t, SqlConst.IS))
            return false;

        var save = _i;
        _context.Consume(); // IS

        bool neg = false;
        if (_context.IsKeywordOrIdentifierWord(SqlConst.NOT))
        {
            _context.Consume();
            neg = true;
        }

        if (_context.IsKeyword(SqlConst.NULL))
        {
            _context.Consume();
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
        var t = _context.Peek();
        if (!SqlExpressionParserContext.IsKeywordOrIdentifierWord(t, SqlConst.NOT))
            return false;

        var t2 = _context.Peek(1);

        // Dispatch: NOT <op>
        if (SqlExpressionParserContext.IsKeyword(t2, SqlConst.BETWEEN))
            return TryParseNotBetween(ref left, minBp);

        if (SqlExpressionParserContext.IsKeyword(t2, SqlConst.IN))
            return TryParseNotIn(ref left, minBp);

        if (SqlExpressionParserContext.IsKeyword(t2, "LIKE") || SqlExpressionParserContext.IsKeywordOrIdentifierWord(t2, SqlConst.ILIKE))
            return TryParseNotLike(ref left, minBp);

        if (SqlExpressionParserContext.IsKeywordOrIdentifierWord(t2, "REGEXP"))
            return TryParseNotRegexp(ref left, minBp);

        return false;
    }

    #region NOT

    private bool TryParseNotBetween(ref SqlExpr left, int minBp)
    {
        if (!TryCheckBp(minBp, 50)) return false;

        _context.Consume(); // NOT
        _context.Consume(); // BETWEEN

        var low = ParseExpression(51);

        _context.ExpectWord(SqlConst.AND); // garante AND como keyword/identifier-word
        var high = ParseExpression(51);

        left = new BetweenExpr(left, low, high, Negated: true);
        return true;
    }

    private bool TryParseNotIn(ref SqlExpr left, int minBp)
    {
        if (!TryCheckBp(minBp, 50)) return false;

        var notTok = _context.Peek();
        _context.Consume(); // NOT
        _context.Consume(); // IN

        if (left is RowExpr && !_context.IsSymbol("("))
            throw Error("Row value IN requires parentheses", notTok);

        // NOT IN ( ... )  ou NOT IN ( (SELECT ...) )
        var payload = ParseInPayload(notTok, "NOT IN"); // retorna lista de SqlExpr ou SubqueryExpr embrulhado
        left = new UnaryExpr(SqlUnaryOp.Not, new InExpr(left, payload));
        return true;
    }

    private bool TryParseNotLike(ref SqlExpr left, int minBp)
    {
        if (!TryCheckBp(minBp, 50)) return false;

        _context.Consume(); // NOT
        _context.Consume(); // LIKE

        left = new UnaryExpr(SqlUnaryOp.Not, ParseLikeExpression(left, 51));
        return true;
    }

    private bool TryParseNotRegexp(ref SqlExpr left, int minBp)
    {
        if (!TryCheckBp(minBp, 50)) return false;

        _context.Consume(); // NOT
        _context.Consume(); // REGEXP

        var pattern = ParseExpression(51);
        left = new UnaryExpr(SqlUnaryOp.Not, new BinaryExpr(SqlBinaryOp.Regexp, left, pattern));
        return true;
    }

    // ------------------------------------------------------------
    // Shared helpers
    // ------------------------------------------------------------

    // Retorna false se não tem precedência suficiente
    private static bool TryCheckBp(int minBp, int lbp) => lbp >= minBp;

    // ParseCreateView do conteúdo de IN(...), lidando com:
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
        if (!_context.IsSymbol("("))
        {
            // Parseia uma única expressão (tipicamente ParameterExpr) com binding forte
            // para não engolir AND/OR subsequentes.
            var single = ParseExpression(rbp);
            return [single];
        }

        _context.ExpectSymbol("(");

        // caso IN ( ( ... ) )  com parêntese extra
        bool extraParen = false;
        if (_context.IsSymbol("("))
        {
            extraParen = true;
            _context.Consume(); // '('
        }

        // ✅ MySQL: IN () e IN(()) são inválidos
        if (_context.IsSymbol(")"))
            throw Error("IN requires at least one element or a subquery", contextToken);

        // subquery: SELECT ou WITH
        if (_context.IsKeywordOrIdentifierWord(SqlConst.SELECT)
            || _context.IsKeywordOrIdentifierWord(SqlConst.WITH))
        {
            var subSql = ReadRawUntilMatchingParen(); // lê até fechar o ')' do SELECT/WITH

            if (extraParen) _context.ExpectSymbol(")"); // fecha '(' extra
            _context.ExpectSymbol(")");                 // fecha IN(...)

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

        if (!_context.IsSymbol(")"))
        {
            while (true)
            {
                items.Add(ParseExpression(0));
                if (!_context.IsSymbol(",")) break;
                _context.Consume(); // ','
            }
        }

        if (extraParen) _context.ExpectSymbol(")");
        _context.ExpectSymbol(")");
        return items;
    }

    #endregion

    private bool TryParseInInfix(ref SqlExpr left, int minBp)
    {
        var t = _context.Peek();
        if (!SqlExpressionParserContext.IsKeyword(t, SqlConst.IN))
            return false;

        var (lbp, rbp) = (50, 51);
        if (lbp < minBp) return false;

        var inTok = t;
        _context.Consume(); // IN

        if (left is RowExpr && !_context.IsSymbol("("))
            throw Error("Row value IN requires parentheses", inTok);

        var payload = ParseInPayload(inTok, SqlConst.IN);

        left = new InExpr(left, payload);
        return true;
    }

    private bool TryParseLikeInfix(ref SqlExpr left, int minBp)
    {
        var t = _context.Peek();
        var negate = false;

        var caseInsensitive = false;

        if (SqlExpressionParserContext.IsKeywordOrIdentifierWord(t, SqlConst.NOT))
        {
            var next = _context.Peek(1);
            if (SqlExpressionParserContext.IsKeyword(next, "LIKE"))
            {
            }
            else if (SqlExpressionParserContext.IsKeywordOrIdentifierWord(next, SqlConst.ILIKE))
            {
                caseInsensitive = true;
            }
            else
                return false;
            negate = true;
        }
        else if (SqlExpressionParserContext.IsKeyword(t, "LIKE"))
        {
        }
        else if (SqlExpressionParserContext.IsKeywordOrIdentifierWord(t, SqlConst.ILIKE))
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
            _context.Consume(); // NOT
            _context.Consume(); // LIKE / ILIKE
        }
        else
        {
            _context.Consume(); // LIKE / ILIKE
        }

        if (caseInsensitive && !_context.Dialect.SupportsIlikeOperator)
            throw SqlUnsupported.NotSupported(_context.Dialect, SqlConst.ILIKE);

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
        var t = _context.Peek();
        var negate = false;

        if (SqlExpressionParserContext.IsKeywordOrIdentifierWord(t, SqlConst.NOT))
        {
            var next = _context.Peek(1);
            if (!SqlExpressionParserContext.IsKeywordOrIdentifierWord(next, "REGEXP"))
                return false;
            negate = true;
        }
        else if (!SqlExpressionParserContext.IsKeywordOrIdentifierWord(t, "REGEXP"))
        {
            return false;
        }

        var (lbp, rbp) = (50, 51);
        if (lbp < minBp) return false;

        if (negate)
        {
            _context.Consume(); // NOT
            _context.Consume(); // REGEXP
        }
        else
        {
            _context.Consume(); // REGEXP
        }

        var pattern = ParseExpression(rbp);
        var expr = (SqlExpr)new BinaryExpr(SqlBinaryOp.Regexp, left, pattern);
        left = negate ? new UnaryExpr(SqlUnaryOp.Not, expr) : expr;
        return true;
    }

    private bool TryParseSoundsLikeInfix(ref SqlExpr left, int minBp)
    {
        var t = _context.Peek();
        if (!SqlExpressionParserContext.IsKeywordOrIdentifierWord(t, "SOUNDS"))
            return false;

        var next = _context.Peek(1);
        if (!SqlExpressionParserContext.IsKeywordOrIdentifierWord(next, "LIKE"))
            return false;

        const int lbp = 50;
        const int rbp = 51;
        if (lbp < minBp) return false;

        _context.Consume(); // SOUNDS
        _context.Consume(); // LIKE

        var pattern = ParseExpression(rbp);
        left = new BinaryExpr(SqlBinaryOp.SoundLike, left, pattern);
        return true;
    }


    private bool TryParseJsonArrowInfix(ref SqlExpr left, int minBp)
    {
        var t = _context.Peek();
        if (t.Kind != SqlTokenKind.Operator || (t.Text != "->" && t.Text != "->>" && t.Text != "#>" && t.Text != "#>>"))
            return false;

        if (!_context.Dialect.SupportsJsonArrowOperators && !_context.Dialect.AllowsParserCrossDialectJsonOperators)
            throw SqlUnsupported.NotSupported(_context.Dialect, "JSON -> / ->> / #> / #>> operators");

        // MySQL: JSON extract operators bind tightly (treat like high precedence binary)
        const int lbp = 120;
        const int rbp = 121;
        if (lbp < minBp) return false;

        var op = _context.Consume().Text;

        // right side: geralmente string '$.path', mas aceita expressão
        var right = ParseExpression(rbp, allowIsNullPostfix: false);

        // Modela como nó neutro: o dialeto/executor decide como avaliar/imprimir.
        left = new JsonAccessExpr(left, right, Unquote: op == "->>" || op == "#>>");

        return true;
    }

    private bool TryParseTypeCastInfix(ref SqlExpr left, int minBp)
    {
        var t = _context.Peek();
        if (t.Kind != SqlTokenKind.Operator || t.Text != "::")
            return false;

        const int lbp = 130;
        if (lbp < minBp) return false;

        _context.Consume(); // ::

        var typeToks = new List<SqlToken>();
        var first = _context.Peek();
        if (first.Kind != SqlTokenKind.Identifier && first.Kind != SqlTokenKind.Keyword)
            throw Error("Type name expected after '::'", first);

        typeToks.Add(_context.Consume());

        if (_context.IsSymbol("("))
        {
            var depth = 0;
            while (true)
            {
                var tok = _context.Peek();
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

                typeToks.Add(_context.Consume());
            }
        }

        var typeSql = string.Join(" ",
            typeToks.Select(TokenToSql)
        ).Trim();

        left = new CallExpr("CAST", [left, new RawSqlExpr(typeSql)])
            .BindScalarFunctionDefinition(_context.Dialect);
        return true;
    }

    private bool TryParseMulDivInfix(ref SqlExpr left, int minBp)
    {
        var t = _context.Peek();
        if (t.Kind != SqlTokenKind.Operator || (t.Text != "*" && t.Text != "/"))
            return false;

        var (lbp, rbp) = (70, 71);
        if (lbp < minBp) return false;

        _context.Consume(); // * or /
        var right = ParseExpression(rbp);

        var op = t.Text == "*" ? SqlBinaryOp.Multiply : SqlBinaryOp.Divide;
        left = new BinaryExpr(op, left, right);
        return true;
    }

    private bool TryParseAddSubInfix(ref SqlExpr left, int minBp)
    {
        var t = _context.Peek();
        var isPipeConcat = t.Kind == SqlTokenKind.Operator
            && t.Text == "||"
            && _context.Dialect.SupportsPipeConcatOperator;

        if (t.Kind != SqlTokenKind.Operator || (t.Text != "+" && t.Text != "-" && !isPipeConcat))
            return false;

        var (lbp, rbp) = (60, 61);
        if (lbp < minBp) return false;

        _context.Consume(); // +, - or ||
        var right = ParseExpression(rbp);
        var op = t.Text == "+"
            ? SqlBinaryOp.Add
            : t.Text == "-"
                ? SqlBinaryOp.Subtract
                : SqlBinaryOp.Concat;

        if (op is SqlBinaryOp.Add or SqlBinaryOp.Subtract)
            right = TryAttachCompactIntervalUnit(right);
        left = new BinaryExpr(op, left, right);
        return true;
    }

    private SqlExpr TryAttachCompactIntervalUnit(SqlExpr expression)
    {
        if (expression is not LiteralExpr { Value: not null and not string } literal)
            return expression;

        var unitToken = _context.Peek();
        if (!IsCompactIntervalUnit(unitToken))
            return expression;

        _context.Consume();
        return new CallExpr("INTERVAL", [literal, new RawSqlExpr(unitToken.Text)])
            .BindScalarFunctionDefinition(_context.Dialect);
    }

    private static bool IsCompactIntervalUnit(SqlToken token)
    {
        if (token.Kind is not (SqlTokenKind.Identifier or SqlTokenKind.Keyword))
            return false;

        return token.Text.Equals(SqlConst.YEAR, StringComparison.OrdinalIgnoreCase)
            || token.Text.Equals("YEARS", StringComparison.OrdinalIgnoreCase)
            || token.Text.Equals("MONTH", StringComparison.OrdinalIgnoreCase)
            || token.Text.Equals("MONTHS", StringComparison.OrdinalIgnoreCase)
            || token.Text.Equals("DAY", StringComparison.OrdinalIgnoreCase)
            || token.Text.Equals("DAYS", StringComparison.OrdinalIgnoreCase)
            || token.Text.Equals("HOUR", StringComparison.OrdinalIgnoreCase)
            || token.Text.Equals("HOURS", StringComparison.OrdinalIgnoreCase)
            || token.Text.Equals("MINUTE", StringComparison.OrdinalIgnoreCase)
            || token.Text.Equals("MINUTES", StringComparison.OrdinalIgnoreCase)
            || token.Text.Equals("SECOND", StringComparison.OrdinalIgnoreCase)
            || token.Text.Equals("SECONDS", StringComparison.OrdinalIgnoreCase);
    }

    private bool TryParseBetweenInfix(ref SqlExpr left, int minBp)
    {
        var t = _context.Peek();
        if (!SqlExpressionParserContext.IsKeyword(t, SqlConst.BETWEEN))
            return false;

        var (lbp, rbp) = (50, 51);
        if (lbp < minBp) return false;

        _context.Consume(); // BETWEEN

        var low = ParseExpression(rbp);

        if (!_context.IsKeyword(SqlConst.AND))
            throw new InvalidOperationException("Esperava AND no BETWEEN");

        _context.Consume(); // AND
        var high = ParseExpression(rbp);

        left = new BetweenExpr(left, low, high, Negated: false);
        return true;
    }

    private bool TryParseMemberOfInfix(ref SqlExpr left, int minBp)
    {
        var t = _context.Peek();
        if (!SqlExpressionParserContext.IsKeywordOrIdentifierWord(t, "MEMBER"))
            return false;

        var (lbp, rbp) = (50, 51);
        if (lbp < minBp) return false;

        if (!_context.Dialect.TryGetScalarFunctionDefinition("MEMBER_OF", out var definition)
            || definition is null
            || !definition.AllowsCall)
        {
            throw SqlUnsupported.NotSupported(_context.Dialect, "MEMBER OF");
        }

        _context.Consume(); // MEMBER
        _context.ExpectWord("OF");

        var right = ParseExpression(rbp);
        left = new FunctionCallExpr("MEMBER_OF", [left, right])
            .BindScalarFunctionDefinition(_context.Dialect);
        return true;
    }

    private bool TryParseAndOrInfix(ref SqlExpr left, int minBp)
    {
        var t = _context.Peek();
        var isPipeOr = t.Kind == SqlTokenKind.Operator
            && t.Text == "||"
            && !_context.Dialect.SupportsPipeConcatOperator;

        if (!(SqlExpressionParserContext.IsKeyword(t, SqlConst.AND)
            || SqlExpressionParserContext.IsKeyword(t, SqlConst.OR)
            || isPipeOr))
            return false;

        var op = SqlExpressionParserContext.IsKeyword(t, SqlConst.AND) ? SqlBinaryOp.And : SqlBinaryOp.Or;
        var (lbp, rbp) = op == SqlBinaryOp.And ? (20, 21) : (10, 11);
        if (lbp < minBp) return false;

        _context.Consume(); // AND, OR or ||
        var right = ParseExpression(rbp);
        left = new BinaryExpr(op, left, right);
        return true;
    }

    private bool TryParseComparisonInfix(ref SqlExpr left, int minBp)
    {
        var t = _context.Peek();
        if (t.Kind != SqlTokenKind.Operator || !TryMapComparisonOp(t.Text, out var bop))
            return false;

        var (lbp, rbp) = (50, 51);
        if (lbp < minBp) return false;

        var opToken = _context.Consume();

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
    /// PT-br: Tenta parsear o lado direito de comparação quantificada (`ANY`/`SOME`/`ALL` com subquery) após operador de comparação.
    /// </summary>
    private bool TryParseQuantifiedComparisonRightSide(
        SqlExpr left,
        SqlBinaryOp op,
        SqlToken contextToken,
        out SqlExpr quantifiedExpr)
    {
        quantifiedExpr = default!;

        var qTok = _context.Peek();
        if (!SqlExpressionParserContext.IsKeywordOrIdentifierWord(qTok, "ANY")
            && !SqlExpressionParserContext.IsKeywordOrIdentifierWord(qTok, "SOME")
            && !SqlExpressionParserContext.IsKeywordOrIdentifierWord(qTok, SqlConst.ALL))
            return false;

        var quantifier = SqlExpressionParserContext.IsKeywordOrIdentifierWord(qTok, "ANY")
                         || SqlExpressionParserContext.IsKeywordOrIdentifierWord(qTok, "SOME")
            ? SqlQuantifier.Any
            : SqlQuantifier.All;

        _context.Consume(); // ANY | SOME | ALL
        _context.ExpectSymbol("(");

        var hasExtraWrapperParen = false;
        if (_context.IsSymbol("("))
        {
            hasExtraWrapperParen = true;
            _context.Consume(); // optional wrapper '('
        }

        var subSql = ReadRawUntilMatchingParen();

        if (hasExtraWrapperParen)
            _context.ExpectSymbol(")");

        _context.ExpectSymbol(")");

        var subquery = SqlQueryParser.ParseSubqueryExprOrThrow(
            subSql,
            contextToken,
            quantifier is SqlQuantifier.Any ? "ANY quantified comparison" : "ALL quantified comparison",
            _context.Db,
            _context.Dialect);

        quantifiedExpr = new QuantifiedComparisonExpr(op, left, quantifier, subquery);
        return true;
    }

    #endregion

    private SqlExpr ParsePrefix()
    {
        var t = _context.Peek();

        if (TryParseTypedDateTimeLiteral(t, out var typedLiteral)) return typedLiteral;
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

    private bool TryParseTypedDateTimeLiteral(SqlToken t, out SqlExpr expr)
    {
        expr = default!;

        if (!SqlExpressionParserContext.IsKeywordOrIdentifierWord(t, "DATE")
            && !SqlExpressionParserContext.IsKeywordOrIdentifierWord(t, "TIMESTAMP"))
            return false;

        if (_context.Peek(1).Kind != SqlTokenKind.String)
            return false;

        _context.Consume(); // DATE or TIMESTAMP
        var literalToken = _context.Peek();
        _context.Consume();

        expr = new LiteralExpr(literalToken.Text);
        return true;
    }

    #region PARSE PREFIX
    // ----------------------------------------------------------------

    private bool TryParseExists(SqlToken t, out SqlExpr expr)
    {
        expr = default!;

        if (!SqlExpressionParserContext.IsKeywordOrIdentifierWord(t, SqlConst.EXISTS))
            return false;

        expr = ParseExistsExpr(t);

        return true;
    }

    private bool TryParseCase(SqlToken t, out SqlExpr expr)
    {
        expr = default!;

        if (!SqlExpressionParserContext.IsKeywordOrIdentifierWord(t, "CASE"))
            return false;

        _context.Consume(); // CASE

        SqlExpr? baseExpr = null;
        if (!_context.IsKeywordOrIdentifierWord(SqlConst.WHEN))
        {
            var baseTokens = _context.ReadTokensUntilTopLevelStop(SqlConst.WHEN);
            if (baseTokens.Count == 0)
                throw Error("CASE expression requires a base expression or WHEN clause", t);

            baseExpr = ParseStandaloneExpression(baseTokens, baseTokens[0], "CASE base expression");
        }

        var whens = new List<CaseWhenThen>();
        while (_context.IsKeywordOrIdentifierWord(SqlConst.WHEN))
        {
            _context.Consume(); // WHEN
            var whenTokens = _context.ReadTokensUntilTopLevelStop(SqlConst.THEN);
            if (whenTokens.Count == 0)
                throw Error("CASE WHEN expression requires a condition", _context.Peek());

            var whenExpr = ParseStandaloneExpression(whenTokens, whenTokens[0], "CASE WHEN expression");

            _context.ExpectWord(SqlConst.THEN);
            var thenTokens = _context.ReadTokensUntilTopLevelStop(SqlConst.WHEN, SqlConst.ELSE, SqlConst.END);
            if (thenTokens.Count == 0)
                throw Error("CASE THEN expression requires a value", _context.Peek());

            var thenExpr = ParseStandaloneExpression(thenTokens, thenTokens[0], "CASE THEN expression");

            whens.Add(new CaseWhenThen(whenExpr, thenExpr));
        }

        SqlExpr? elseExpr = null;
        if (_context.IsKeywordOrIdentifierWord(SqlConst.ELSE))
        {
            _context.Consume(); // ELSE
            var elseTokens = _context.ReadTokensUntilTopLevelStop(SqlConst.END);
            if (elseTokens.Count == 0)
                throw Error("CASE ELSE expression requires a value", _context.Peek());

            elseExpr = ParseStandaloneExpression(elseTokens, elseTokens[0], "CASE ELSE expression");
        }

        _context.ExpectWord(SqlConst.END);
        expr = new CaseExpr(baseExpr, whens, elseExpr);
        return true;
    }

    private bool TryParseNot(SqlToken t, out SqlExpr expr)
    {
        expr = default!;

        if (!SqlExpressionParserContext.IsKeywordOrIdentifierWord(t, SqlConst.NOT))
            return false;

        _context.Consume();

        if (_context.IsKeywordOrIdentifierWord(SqlConst.EXISTS))
        {
            var existsToken = _context.Peek();
            expr = new UnaryExpr(SqlUnaryOp.Not, ParseExistsExpr(existsToken));
            return true;
        }

        var rhs = ParseExpression(60);
        expr = new UnaryExpr(SqlUnaryOp.Not, rhs);
        return true;
    }

    private bool TryParseUnaryPlusMinus(SqlToken t, out SqlExpr expr)
    {
        expr = default!;
        if (t.Kind != SqlTokenKind.Operator || (t.Text != "+" && t.Text != "-"))
            return false;

        _context.Consume(); // + or -
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

    private SqlExpr ParseExistsExpr(SqlToken contextToken)
    {
        _context.Consume(); // EXISTS
        _context.ExpectSymbol("(");

        var subSql = ReadRawUntilMatchingParen();

        _context.ExpectSymbol(")");

        return new ExistsExpr(
            SqlQueryParser.ParseSubqueryExprOrThrow(subSql, contextToken, SqlConst.EXISTS, _context.Db, _context.Dialect)
        );
    }

    private bool TryParseIntervalLiteral(SqlToken t, out SqlExpr expr)
    {
        expr = default!;

        if (!SqlExpressionParserContext.IsKeywordOrIdentifierWord(t, "INTERVAL"))
            return false;

        _context.Consume(); // INTERVAL
        var next = _context.Peek();
        if (next.Kind != SqlTokenKind.String)
            throw Error("INTERVAL requires a string literal", next);

        _context.Consume();

        // PostgreSQL style: INTERVAL '1 day'
        // Oracle style:     INTERVAL '1' DAY
        var raw = next.Text;
        var unitTok = _context.Peek();
        if (unitTok.Kind is SqlTokenKind.Keyword or SqlTokenKind.Identifier)
        {
            var unit = unitTok.Text.Trim();
            if (!string.IsNullOrEmpty(unit))
            {
                _context.Consume();
                raw = $"{raw} {unit}";
            }
        }

        expr = new CallExpr("INTERVAL", [new LiteralExpr(raw)])
            .BindScalarFunctionDefinition(_context.Dialect);
        return true;
    }

    private bool TryParseNextValueFor(SqlToken t, out SqlExpr expr)
    {
        expr = default!;

        if (!SqlExpressionParserContext.IsKeywordOrIdentifierWord(t, SqlConst.NEXT))
            return false;

        if (!SqlExpressionParserContext.IsKeywordOrIdentifierWord(_context.Peek(1), SqlConst.VALUE)
            || !SqlExpressionParserContext.IsKeywordOrIdentifierWord(_context.Peek(2), SqlConst.FOR))
            return false;

        if (!_context.Dialect.SupportsNextValueForSequenceExpression)
            throw SqlUnsupported.NotSupported(_context.Dialect, "NEXT VALUE FOR");

        _context.Consume(); // NEXT
        _context.Consume(); // VALUE
        _context.Consume(); // FOR

        var sequenceToken = _context.Peek();
        if (sequenceToken.Kind is not SqlTokenKind.Identifier and not SqlTokenKind.Keyword)
            throw Error("NEXT VALUE FOR requires a sequence name.", sequenceToken);

        _context.Consume();
        expr = new CallExpr("NEXT_VALUE_FOR", [ParseIdentifierChainOrColumn(sequenceToken.Text)])
            .BindScalarFunctionDefinition(_context.Dialect);
        return true;
    }

    private bool TryParsePreviousValueFor(SqlToken t, out SqlExpr expr)
    {
        expr = default!;

        if (!SqlExpressionParserContext.IsKeywordOrIdentifierWord(t, SqlConst.PREVIOUS))
            return false;

        if (!SqlExpressionParserContext.IsKeywordOrIdentifierWord(_context.Peek(1), SqlConst.VALUE)
            || !SqlExpressionParserContext.IsKeywordOrIdentifierWord(_context.Peek(2), SqlConst.FOR))
            return false;

        if (!_context.Dialect.SupportsPreviousValueForSequenceExpression)
            throw SqlUnsupported.NotSupported(_context.Dialect, "PREVIOUS VALUE FOR");

        _context.Consume(); // PREVIOUS
        _context.Consume(); // VALUE
        _context.Consume(); // FOR

        var sequenceToken = _context.Peek();
        if (sequenceToken.Kind is not SqlTokenKind.Identifier and not SqlTokenKind.Keyword)
            throw Error("PREVIOUS VALUE FOR requires a sequence name.", sequenceToken);

        _context.Consume();
        expr = new CallExpr("PREVIOUS_VALUE_FOR", [ParseIdentifierChainOrColumn(sequenceToken.Text)])
            .BindScalarFunctionDefinition(_context.Dialect);
        return true;
    }


    private bool TryParseStar(SqlToken t, out SqlExpr expr)
    {
        expr = default!;

        if (t.Kind != SqlTokenKind.Operator || t.Text != "*")
            return false;

        _context.Consume();
        expr = new StarExpr();
        return true;
    }

    private bool TryParseParenOrRow(SqlToken t, out SqlExpr expr)
    {
        expr = default!;

        if (!SqlExpressionParserContext.IsSymbol(t, "("))
            return false;

        _context.Consume(); // '('

        // ✅ scalar subquery: (SELECT ... ) / (WITH ... )
        if (_context.IsKeywordOrIdentifierWord(SqlConst.SELECT) || _context.IsKeywordOrIdentifierWord(SqlConst.WITH))
        {
            var subSql = ReadRawUntilMatchingParen(); // lê até antes do ')'
            _context.ExpectSymbol(")");

            expr = SqlQueryParser.ParseSubqueryExprOrThrow(subSql, t, "SCALAR SUBQUERY", _context.Db, _context.Dialect);
            return true;
        }

        var first = ParseExpression(0);

        if (_context.IsSymbol(","))
        {
            var items = new List<SqlExpr> { first };
            while (_context.IsSymbol(","))
            {
                _context.Consume();
                items.Add(ParseExpression(0));
            }

            _context.ExpectSymbol(")");
            expr = new RowExpr(items);
            return true;
        }

        _context.ExpectSymbol(")");
        expr = first;
        return true;
    }

    private bool TryParseNullTrueFalse(SqlToken t, out SqlExpr expr)
    {
        expr = default!;

        if (t.Kind != SqlTokenKind.Keyword)
            return false;

        if (!(SqlExpressionParserContext.IsKeyword(t, SqlConst.NULL)
            || SqlExpressionParserContext.IsKeyword(t, SqlConst.TRUE)
            || SqlExpressionParserContext.IsKeyword(t, SqlConst.FALSE)))
            return false;

        _context.Consume();

        object? val =
            SqlExpressionParserContext.IsKeyword(t, SqlConst.NULL)
            ? null
            : SqlExpressionParserContext.IsKeyword(t, SqlConst.TRUE);

        expr = new LiteralExpr(val);
        return true;
    }

    private bool TryParseString(SqlToken t, out SqlExpr expr)
    {
        expr = default!;

        if (t.Kind != SqlTokenKind.String)
            return false;

        _context.Consume();
        expr = new LiteralExpr(t.Text);
        return true;
    }

    private bool TryParseNumber(SqlToken t, out SqlExpr expr)
    {
        expr = default!;

        if (t.Kind != SqlTokenKind.Number)
            return false;

        _context.Consume();

        if (TryParseHexBinaryLiteralValue(t.Text, out var binaryValue))
        {
            expr = new LiteralExpr(binaryValue);
            return true;
        }

        if (TryParseNumericLiteralValue(t.Text, out var numericValue))
        {
            expr = new LiteralExpr(numericValue);
            return true;
        }

        throw Error($"Número inválido: {t.Text}", t);
    }

    private static bool TryParseHexBinaryLiteralValue(string text, out byte[] binaryValue)
    {
        binaryValue = [];

        var normalized = text.AsSpan().Trim();
        if (!normalized.StartsWith("0x", StringComparison.OrdinalIgnoreCase))
            return false;

        var hex = normalized[2..];
        if (hex.Length == 0 || hex.Length % 2 != 0)
            return false;

        var buffer = new byte[hex.Length / 2];
        for (var i = 0; i < hex.Length; i += 2)
        {
            if (!ReadOnlySpanCompatibility.TryParseByte(hex.Slice(i, 2), NumberStyles.HexNumber, CultureInfo.InvariantCulture, out var part))
                return false;

            buffer[i / 2] = part;
        }

        binaryValue = buffer;
        return true;
    }

    private static bool TryParseNumericLiteralValue(string text, out object numericValue)
    {
        numericValue = default!;

        var normalized = text.Trim();
        if (normalized.Length == 0)
            return false;

        if (!HasValidNumericLiteralSyntax(normalized, out var hasDecimalPoint, out var hasExponent))
            return false;

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

    private static bool HasValidNumericLiteralSyntax(
        string text,
        out bool hasDecimalPoint,
        out bool hasExponent)
    {
        hasDecimalPoint = false;
        hasExponent = false;

        var index = 0;
        if (text[index] is '+' or '-')
        {
            index++;
            if (index == text.Length)
                return false;
        }

        var digitsBeforeDecimal = 0;
        while (index < text.Length && char.IsDigit(text[index]))
        {
            index++;
            digitsBeforeDecimal++;
        }

        if (index < text.Length && text[index] == '.')
        {
            hasDecimalPoint = true;
            index++;

            while (index < text.Length && char.IsDigit(text[index]))
                index++;
        }

        if (digitsBeforeDecimal == 0 && !hasDecimalPoint)
            return false;

        if (index < text.Length && (text[index] == 'e' || text[index] == 'E'))
        {
            hasExponent = true;
            index++;

            if (index < text.Length && (text[index] is '+' or '-'))
                index++;

            var exponentDigits = 0;
            while (index < text.Length && char.IsDigit(text[index]))
            {
                index++;
                exponentDigits++;
            }

            if (exponentDigits == 0)
                return false;
        }

        return index == text.Length;
    }

    private bool TryParseParameter(SqlToken t, out SqlExpr expr)
    {
        expr = default!;

        if (t.Kind != SqlTokenKind.Parameter)
            return false;

        _context.Consume();
        expr = new ParameterExpr(t.Text);
        return true;
    }

    private bool TryParseIdentifierOrCall(SqlToken t, out SqlExpr expr)
    {
        expr = default!;

        if (!(t.Kind == SqlTokenKind.Identifier || t.Kind == SqlTokenKind.Keyword))
            return false;

        _context.Consume();
        var name = t.Text;

        if (_context.Peek().Kind is SqlTokenKind.Identifier or SqlTokenKind.Keyword)
        {
            var compositeName = $"{name} {_context.Peek().Text}";
            if (_context.Dialect.AllowsTemporalIdentifier(compositeName))
            {
                _context.Consume();
                name = compositeName;
            }
        }

        // function call: name(...)
        if (_context.IsSymbol("(")
            || (_context.IsSymbol(".")
                && _context.Peek(1).Kind is SqlTokenKind.Identifier or SqlTokenKind.Keyword
                && SqlExpressionParserContext.IsSymbol(_context.Peek(2), "(")))
        {
            SqlTemporalExpressionParserHelper.EnsureTemporalIdentifierDoesNotAllowParentheses(
                _context,
                name,
                $"Temporal function token '{name}' must be used without parentheses.",
                _context.Peek());
            var call = ParseCallAfterName(name);
            if (_context.TryParseMatchAgainstInfix(
                call,
                ParseStandaloneExpression,
                out var matchAgainstExpr))
            {
                expr = matchAgainstExpr;
                return true;
            }
            call = ParseWithinGroupOrderByIfPresent(call);
            call = ParseAggregateFilterIfPresent(call);

            // ✅ Window function: ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...)
            if (_context.IsKeywordOrIdentifierWord("OVER"))
            {
                _context.EnsureWindowFunctionSupport(call.Name);
                _context.EnsureWindowFunctionArguments(call.Name, call.Args, _context.Peek());

                _context.Consume(); // OVER
                var spec = _context.ParseWindowSpec(
                    ParseExpression,
                    ParseExprListUntilOrderOrParenClose);
                _context.EnsureWindowSpecSupport(call.Name, spec, _context.Peek());
                expr = new WindowFunctionExpr(call.Name, call.Args, spec, call.Distinct)
                    .BindWindowFunctionDefinition(_context.Dialect);
                return true;
            }

            expr = call;
            return true;
        }

        SqlTemporalExpressionParserHelper.EnsureTemporalCallIdentifierRequiresParentheses(
            _context,
            name,
            $"Temporal function '{name}' requires parentheses '{name}()'.",
            _context.Peek());
        expr = ParseIdentifierChainOrColumn(name);
        return true;
    }
    private SqlExpr ParseStandaloneExpression(
        IReadOnlyList<SqlToken> tokens,
        SqlToken contextToken,
        string contextLabel)
    {
        try
        {
            var localSql = _context.TokensToSql(tokens);
            var localTokens = new SqlTokenizer(localSql, _context.Dialect).Tokenize();
            var parser = new SqlExpressionParser(
                new SqlExpressionParserContext(
                    localTokens,
                    _context.Db,
                    _context.Dialect,
                    _context.Parameters,
                    _context.CustomFunctionSupported));
            var parsed = parser.ParseExpression(0);
            parser._context.ExpectEnd();
            return parsed;
        }
        catch (Exception ex)
        {
            throw Error($"Invalid {contextLabel}: {ex.Message}", contextToken);
        }
    }

    private CallExpr ParseCallAfterName(string name)
    {
        if (_context.IsSymbol(".")
            && _context.Peek(1).Kind is SqlTokenKind.Identifier or SqlTokenKind.Keyword
            && SqlExpressionParserContext.IsSymbol(_context.Peek(2), "("))
        {
            _context.Consume(); // .
            name = _context.Consume().Text;
        }

        _context.EnsureSupported(name);

        _context.Consume(); // '('

        // ================================
        // EXTRACT(field FROM expr) — sintaxe especial
        // ================================
        if (name.Equals("EXTRACT", StringComparison.OrdinalIgnoreCase))
        {
            var unitTok = _context.Peek();
            if (unitTok.Kind is not (SqlTokenKind.Identifier or SqlTokenKind.Keyword))
                throw Error("EXTRACT requires a unit", unitTok);

            _context.Consume(); // unit

            if (!_context.IsKeywordOrIdentifierWord(SqlConst.FROM))
                throw Error("EXTRACT requires FROM", _context.Peek());

            _context.Consume(); // FROM

            SqlExpr inner;
            if (_context.IsSymbol("("))
            {
                _context.Consume(); // optional '('
                inner = ParseExpression(0);
                _context.ExpectSymbol(")");
            }
            else
            {
                inner = ParseExpression(0);
            }

            _context.ExpectSymbol(")");
            return new CallExpr("EXTRACT", [new RawSqlExpr(unitTok.Text), inner])
                .BindScalarFunctionDefinition(_context.Dialect);
        }

        // ================================
        // CAST(expr AS TYPE) — sintaxe especial
        if (_context.TryParseSpecialCall(
            name,
            ParseExpression,
            out var specialCall))
            return specialCall;

        // ================================
        // Funções normais
        // ================================

        var distinct = false;
        if (_context.IsKeywordOrIdentifierWord(SqlConst.DISTINCT))
        {
            _context.Consume();
            distinct = true;

            // MySQL does not allow duplicated DISTINCT in functions: COUNT(DISTINCT DISTINCT id)
            if (_context.IsKeywordOrIdentifierWord(SqlConst.DISTINCT))
                throw Error("duplicated DISTINCT", _context.Peek());

            if (_context.IsSymbol(")"))
                throw Error("DISTINCT requires an expression", _context.Peek());
        }

        var args = new List<SqlExpr>();
        if (!_context.IsSymbol(")"))
        {
            var expectsLeadingTemporalUnit = name.Equals("TIMESTAMPADD", StringComparison.OrdinalIgnoreCase)
                || name.Equals("TIMESTAMPDIFF", StringComparison.OrdinalIgnoreCase)
                || name.Equals("DATEDIFF_BIG", StringComparison.OrdinalIgnoreCase)
                || (_context.Dialect.SupportsSqlServerDateFunction(name)
                    && (name.Equals("DATEADD", StringComparison.OrdinalIgnoreCase)
                        || name.Equals("DATEDIFF", StringComparison.OrdinalIgnoreCase)
                        || name.Equals("DATEPART", StringComparison.OrdinalIgnoreCase)
                        || name.Equals("DATENAME", StringComparison.OrdinalIgnoreCase)));

            if (expectsLeadingTemporalUnit)
            {
                var unitTok = _context.Peek();
                if (unitTok.Kind is not (SqlTokenKind.Identifier or SqlTokenKind.Keyword or SqlTokenKind.Number))
                    throw Error($"{name} requires a unit", unitTok);

                _context.Consume(); // unit
                args.Add(new RawSqlExpr(unitTok.Text));

                if (!_context.IsSymbol(","))
                    throw Error($"{name} requires a comma after the unit", _context.Peek());

                _context.Consume(); // ,
            }

            while (true)
            {
                // MySQL: DATE_ADD(x, INTERVAL 1 DAY) etc.
                if (_context.IsKeywordOrIdentifierWord("INTERVAL"))
                {
                    _context.Consume(); // INTERVAL
                    var n = ParseExpression(0);
                    // unit (DAY/HOUR/...)
                    var unitTok = _context.Peek();
                    if (!(unitTok.Kind == SqlTokenKind.Identifier || unitTok.Kind == SqlTokenKind.Keyword))
                        throw Error("INTERVAL requires unit", unitTok);
                    _context.Consume();
                    args.Add(new CallExpr("INTERVAL", [n, new RawSqlExpr(unitTok.Text)])
                        .BindScalarFunctionDefinition(_context.Dialect));
                }
                else
                {
                    args.Add(ShouldUseNativeStringAggregateArgumentBoundaries(name)
                        ? ParseStringAggregateFunctionArgument(name)
                        : ParseExpression(0));
                }

                if (!_context.IsSymbol(","))
                    break;

                _context.Consume();
            }

            // Oracle: JSON_VALUE(json_doc, path RETURNING NUMBER)
            if (name.Equals("JSON_VALUE", StringComparison.OrdinalIgnoreCase)
                && _context.IsKeywordOrIdentifierWord(SqlConst.RETURNING))
            {
                if (!_context.Dialect.SupportsJsonValueReturningClause)
                    throw SqlUnsupported.NotSupported(_context.Dialect, "JSON_VALUE ... RETURNING");

                _context.Consume(); // RETURNING

                var typeToks = new List<SqlToken>();
                int depth = 0;

                while (true)
                {
                    var t = _context.Peek();

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

                    typeToks.Add(_context.Consume());
                }

                var typeSql = string.Join(" ", typeToks.Select(TokenToSql)).Trim();
                args.Add(new RawSqlExpr($"RETURNING {typeSql}"));
            }
        }

        var aggregateOrderBy = ParseAggregateOrderByInsideCallIfPresent(name);
        ParseAggregateSeparatorKeywordIfPresent(name, args);

        _context.ExpectSymbol(")");
        ValidateStringAggregateDistinctUsage(name, distinct, args);

        return new CallExpr(name, args, distinct, aggregateOrderBy)
            .BindScalarFunctionDefinition(_context.Dialect);
    }

    private IReadOnlyList<WindowOrderItem>? ParseAggregateOrderByInsideCallIfPresent(string functionName)
    {
        if (!_context.IsKeywordOrIdentifierWord(SqlConst.ORDER))
            return null;

        if (!functionName.Equals(SqlConst.GROUP_CONCAT, StringComparison.OrdinalIgnoreCase)
            && !functionName.Equals(SqlConst.STRING_AGG, StringComparison.OrdinalIgnoreCase)
            && !functionName.Equals(SqlConst.LISTAGG, StringComparison.OrdinalIgnoreCase)
            && !functionName.Equals(SqlConst.LIST, StringComparison.OrdinalIgnoreCase))
            throw SqlUnsupported.NotSupported(_context.Dialect, $"aggregate ORDER BY for function '{functionName}'");

        if (!_context.Dialect.SupportsAggregateOrderByForStringAggregates)
            throw SqlUnsupported.NotSupported(_context.Dialect, "aggregate ORDER BY");

        if (!_context.Dialect.SupportsAggregateOrderByStringAggregateFunction(functionName))
            throw SqlUnsupported.NotSupported(_context.Dialect, $"aggregate ORDER BY for function '{functionName}'");

        _context.Consume(); // ORDER
        if (!_context.IsKeywordOrIdentifierWord(SqlConst.BY))
            throw Error("aggregate ORDER BY requires BY", _context.Peek());
        _context.Consume();

        var allowSeparatorTerminator =
            _context.Dialect.SupportsAggregateSeparatorKeywordForStringAggregates
            && _context.Dialect.SupportsAggregateSeparatorKeywordStringAggregateFunction(functionName);

        var orderBy = ParseStringAggregateOrderByItems("aggregate ORDER BY", allowSeparatorTerminator);

        if (allowSeparatorTerminator
            && _context.IsKeywordOrIdentifierWord("SEPARATOR")
            && SqlExpressionParserContext.IsSymbol(_context.Peek(1), ")"))
        {
            return orderBy;
        }

        return orderBy;
    }

    private bool ShouldUseNativeStringAggregateArgumentBoundaries(string functionName)
        => _context.Dialect.SupportsAggregateOrderByStringAggregateFunction(functionName)
            || _context.Dialect.SupportsAggregateSeparatorKeywordStringAggregateFunction(functionName);

    private void ParseAggregateSeparatorKeywordIfPresent(string functionName, List<SqlExpr> args)
    {
        if (!_context.IsKeywordOrIdentifierWord("SEPARATOR"))
            return;

        if (!functionName.Equals(SqlConst.GROUP_CONCAT, StringComparison.OrdinalIgnoreCase)
            && !functionName.Equals(SqlConst.STRING_AGG, StringComparison.OrdinalIgnoreCase)
            && !functionName.Equals(SqlConst.LISTAGG, StringComparison.OrdinalIgnoreCase)
            && !functionName.Equals(SqlConst.LIST, StringComparison.OrdinalIgnoreCase))
            throw SqlUnsupported.NotSupported(_context.Dialect, $"aggregate separator keyword for function '{functionName}'");

        if (!_context.Dialect.SupportsAggregateSeparatorKeywordForStringAggregates)
            throw SqlUnsupported.NotSupported(_context.Dialect, "aggregate separator keyword");

        if (!_context.Dialect.SupportsAggregateSeparatorKeywordStringAggregateFunction(functionName))
            throw SqlUnsupported.NotSupported(_context.Dialect, $"aggregate separator keyword for function '{functionName}'");

        _context.Consume(); // SEPARATOR

        if (_context.IsSymbol(")"))
            throw Error("aggregate separator keyword requires an expression", _context.Peek());

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

    private void ValidateStringAggregateDistinctUsage(string functionName, bool distinct, IReadOnlyList<SqlExpr> args)
    {
        if (!distinct)
            return;

        var dialectName = _context.Dialect.Name;

        if (functionName.Equals(SqlConst.STRING_AGG, StringComparison.OrdinalIgnoreCase)
            && (dialectName.Equals("sqlserver", StringComparison.OrdinalIgnoreCase)
                || dialectName.Equals("sqlazure", StringComparison.OrdinalIgnoreCase)))
        {
            throw SqlUnsupported.NotSupported(_context.Dialect, "DISTINCT in STRING_AGG");
        }

        if (functionName.Equals(SqlConst.GROUP_CONCAT, StringComparison.OrdinalIgnoreCase)
            && dialectName.Equals("sqlite", StringComparison.OrdinalIgnoreCase)
            && args.Count > 1)
        {
            throw SqlUnsupported.NotSupported(_context.Dialect, "DISTINCT with multiple arguments in GROUP_CONCAT");
        }
    }

    private SqlExpr ParseStringAggregateFunctionArgument(string functionName)
    {
        var start = _i;
        var depth = 0;

        while (true)
        {
            var token = _context.Peek();

            if (token.Kind == SqlTokenKind.EndOfFile)
                throw Error($"function '{functionName}' argument not closed", token);

            if (depth == 0)
            {
                if (SqlExpressionParserContext.IsSymbol(token, ",")
                    || SqlExpressionParserContext.IsSymbol(token, ")")
                    || (_context.Dialect.SupportsAggregateOrderByStringAggregateFunction(functionName)
                        && SqlExpressionParserContext.IsKeywordOrIdentifierWord(token, SqlConst.ORDER))
                    || (_context.Dialect.SupportsAggregateSeparatorKeywordStringAggregateFunction(functionName)
                        && SqlExpressionParserContext.IsKeywordOrIdentifierWord(token, "SEPARATOR")))
                {
                    break;
                }
            }

            if (SqlExpressionParserContext.IsSymbol(token, "("))
            {
                depth++;
                _context.Consume();
                continue;
            }

            if (SqlExpressionParserContext.IsSymbol(token, ")"))
            {
                if (depth == 0)
                    break;

                depth--;
                _context.Consume();
                continue;
            }

            _context.Consume();
        }

        if (_i == start)
            throw Error($"function '{functionName}' requires an expression", _context.Peek());

        var sql = string.Join(" ", _context.Toks.Skip(start).Take(_i - start).Select(TokenToSql)).Trim();
        return ParseScalar(sql, _context.Db, _context.Dialect, _context.Parameters, _context.CustomFunctionSupported);
    }

    private CallExpr ParseWithinGroupOrderByIfPresent(CallExpr call)
    {
        if (!_context.IsKeywordOrIdentifierWord(SqlConst.WITHIN))
            return call;

        if (!call.Name.Equals(SqlConst.GROUP_CONCAT, StringComparison.OrdinalIgnoreCase)
            && !call.Name.Equals(SqlConst.STRING_AGG, StringComparison.OrdinalIgnoreCase)
            && !call.Name.Equals(SqlConst.LISTAGG, StringComparison.OrdinalIgnoreCase)
            && !call.Name.Equals(SqlConst.LIST, StringComparison.OrdinalIgnoreCase))
        {
            throw SqlUnsupported.NotSupported(
                _context.Dialect,
                $"ordered-set aggregate syntax WITHIN GROUP for function '{call.Name}'");
        }

        if (!_context.Dialect.SupportsWithinGroupForStringAggregates)
            throw SqlUnsupported.NotSupported(_context.Dialect, "ordered-set aggregate syntax WITHIN GROUP");

        if (!_context.Dialect.SupportsWithinGroupStringAggregateFunction(call.Name))
            throw SqlUnsupported.NotSupported(_context.Dialect, $"ordered-set aggregate syntax WITHIN GROUP for function '{call.Name}'");

        _context.Consume(); // WITHIN
        _context.ExpectWord(SqlConst.GROUP);
        _context.ExpectSymbol("(");

        if (!_context.IsKeywordOrIdentifierWord(SqlConst.ORDER))
            throw Error("WITHIN GROUP requires ORDER BY", _context.Peek());
        _context.Consume();

        if (!_context.IsKeywordOrIdentifierWord(SqlConst.BY))
            throw Error("WITHIN GROUP requires ORDER BY", _context.Peek());
        _context.Consume();

        var orderBy = ParseStringAggregateOrderByItems("WITHIN GROUP ORDER BY");

        _context.ExpectSymbol(")");
        return call with { WithinGroupOrderBy = orderBy };
    }

    private CallExpr ParseAggregateFilterIfPresent(CallExpr call)
    {
        if (!_context.IsKeywordOrIdentifierWord("FILTER"))
            return call;

        _context.Consume(); // FILTER
        _context.ExpectSymbol("(");

        if (!_context.IsKeywordOrIdentifierWord(SqlConst.WHERE))
            throw Error("FILTER requires WHERE", _context.Peek());

        _context.Consume(); // WHERE
        var filterSql = ReadRawUntilMatchingParen();
        _context.ExpectSymbol(")");

        if (string.IsNullOrWhiteSpace(filterSql))
            throw Error("FILTER requires an expression", _context.Peek());

        var filterExpr = ParseWhere(filterSql, _context.Db, _context.Dialect, _context.Parameters, _context.CustomFunctionSupported);
        return call with { Filter = filterExpr };
    }

    private List<WindowOrderItem> ParseStringAggregateOrderByItems(string context, bool allowSeparatorTerminator = false)
    {
        var start = _i;
        var depth = 0;

        while (true)
        {
            var token = _context.Peek();

            if (token.Kind == SqlTokenKind.EndOfFile)
                throw Error($"{context} expression not closed", token);

            if (depth == 0)
            {
                if (SqlExpressionParserContext.IsSymbol(token, ")")
                    || (allowSeparatorTerminator && SqlExpressionParserContext.IsKeywordOrIdentifierWord(token, "SEPARATOR")))
                {
                    break;
                }
            }

            if (SqlExpressionParserContext.IsSymbol(token, "("))
            {
                depth++;
                _context.Consume();
                continue;
            }

            if (SqlExpressionParserContext.IsSymbol(token, ")"))
            {
                if (depth == 0)
                    break;

                depth--;
                _context.Consume();
                continue;
            }

            _context.Consume();
        }

        var payloadTokens = _context.Toks.Skip(start).Take(_i - start).ToList();
        if (payloadTokens.Count == 0)
            throw Error($"{context} requires at least one expression", _context.Peek());

        var items = new List<List<SqlToken>>();
        var current = new List<SqlToken>();
        depth = 0;

        foreach (var token in payloadTokens)
        {
            if (depth == 0 && SqlExpressionParserContext.IsSymbol(token, ","))
            {
                if (current.Count == 0)
                    throw Error($"{context} has an unexpected comma before expression", token);

                items.Add(current);
                current = [];
                continue;
            }

            if (SqlExpressionParserContext.IsSymbol(token, "("))
                depth++;
            else if (SqlExpressionParserContext.IsSymbol(token, ")"))
                depth--;

            current.Add(token);
        }

        if (current.Count == 0)
            throw Error($"{context} has a trailing comma without expression", _context.Peek());

        items.Add(current);

        var orderBy = new List<WindowOrderItem>(items.Count);
        foreach (var itemTokens in items)
        {
            var desc = false;
            if (itemTokens.Count > 0 && SqlExpressionParserContext.IsKeywordOrIdentifierWord(itemTokens[^1], "DESC"))
            {
                desc = true;
                itemTokens.RemoveAt(itemTokens.Count - 1);
            }
            else if (itemTokens.Count > 0 && SqlExpressionParserContext.IsKeywordOrIdentifierWord(itemTokens[^1], "ASC"))
            {
                itemTokens.RemoveAt(itemTokens.Count - 1);
            }

            if (itemTokens.Count == 0)
                throw Error($"{context} requires at least one expression", _context.Peek());

            for (var i = 0; i < itemTokens.Count; i++)
            {
                if (!SqlExpressionParserContext.IsKeywordOrIdentifierWord(itemTokens[i], "ASC")
                    && !SqlExpressionParserContext.IsKeywordOrIdentifierWord(itemTokens[i], "DESC"))
                {
                    continue;
                }

                throw Error($"{context} requires commas between expressions", itemTokens[i]);
            }

            var sql = string.Join(" ", itemTokens.Select(TokenToSql)).Trim();
            orderBy.Add(new WindowOrderItem(ParseScalar(sql, _context.Db, _context.Dialect, _context.Parameters, _context.CustomFunctionSupported), desc));
        }

        return orderBy;
    }

    private IReadOnlyList<SqlExpr> ParseExprListUntilOrderOrParenClose()
    {
        var items = new List<SqlExpr>();

        if (_context.IsSymbol(")") || _context.IsKeywordOrIdentifierWord(SqlConst.ORDER))
            return items;

        while (true)
        {
            items.Add(ParseExpression(0));

            if (_context.IsSymbol(","))
            {
                _context.Consume();
                continue;
            }

            break;
        }

        return items;
    }

    private SqlExpr ParseIdentifierChainOrColumn(string first)
    {
        var parts = new List<string> { first };

        while (_context.IsSymbol("."))
        {
            _context.Consume(); // '.'
            var t = _context.Peek();

            // ✅ suporta alias.* (asterisco é Operator no tokenizer)
            if ((t.Kind == SqlTokenKind.Operator || t.Kind == SqlTokenKind.Symbol) && t.Text == "*")
            {
                _context.Consume();
                parts.Add("*");
                break;
            }

            if (t.Kind != SqlTokenKind.Identifier && t.Kind != SqlTokenKind.Keyword)
                throw Error("Esperava identificador após '.'", t);

            parts.Add(_context.Consume().Text);
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
        if (!suffix.Equals(SqlConst.NEXTVAL, StringComparison.OrdinalIgnoreCase)
            && !suffix.Equals(SqlConst.CURRVAL, StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        if (!_context.Dialect.SupportsSequenceDotValueExpression(suffix))
            throw SqlUnsupported.NotSupported(_context.Dialect, suffix);

        var targetParts = parts.Take(parts.Count - 1).ToArray();
        SqlExpr target = targetParts.Length switch
        {
            1 => new IdentifierExpr(targetParts[0]),
            2 => new ColumnExpr(targetParts[0], targetParts[1]),
            _ => new RawSqlExpr(string.Join(".", targetParts))
        };

        expr = new CallExpr(suffix, [target])
            .BindScalarFunctionDefinition(_context.Dialect);
        return true;
    }

    #endregion

    private SubqueryExpr ParseAndWrapSubquery(
        string subSql,
        SqlToken contextToken,
        string contextLabel)
    {
        try
        {
            return SqlQueryParser.ParseSubqueryExprOrThrow(subSql, contextToken, contextLabel, _context.Db, _context.Dialect);
        }
        catch (Exception ex)
        {
            // mantém teu padrão de erro com token/posição
            throw Error($"Subquery inválida ({contextLabel}): {ex.Message}", contextToken);
        }
    }

    private InvalidOperationException Error(string msg, SqlToken t)
        => _context.Error(msg, t);

    private bool TryMapComparisonOp(string op, out SqlBinaryOp bop)
    {
        // Dialect can define extra operators (ex: MySQL <=>).
        if (_context.Dialect.TryMapBinaryOperator(op, out bop))
        {
            if (bop == SqlBinaryOp.NullSafeEq)
                return _context.Dialect.SupportsNullSafeEq;

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

    private SqlExpr TryConsumeTrailingLikeEscape(SqlExpr expr)
    {
        if (!_context.Dialect.SupportsLikeEscapeClause || !_context.IsKeywordOrIdentifierWord("ESCAPE"))
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
        if (!_context.Dialect.SupportsLikeEscapeClause || !_context.IsKeywordOrIdentifierWord("ESCAPE"))
            return like;

        var escapeToken = _context.Peek();
        _context.Consume(); // ESCAPE
        var escape = ParseExpression(rbp);
        ValidateLikeEscapeExpression(escape, escapeToken);
        return like with { Escape = escape };
    }

    private void ValidateLikeEscapeExpression(SqlExpr escape, SqlToken escapeToken)
    {
        if (_context.Dialect.LikeEscapeExpressionMustBeSingleCharacter
            && escape is LiteralExpr { Value: string escapeText }
            && escapeText.Length != 1)
        {
            throw Error("LIKE ESCAPE requires a single character expression.", escapeToken);
        }

        if (_context.Dialect.LikeEscapeExpressionMustBeSingleCharacter
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
        if (_context.Parameters is null)
            return false;

        var normalized = parameterToken.TrimStart('@', ':', '?');
        foreach (IDataParameter parameter in _context.Parameters)
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
        var parts = new List<SqlToken>();

        while (true)
        {
            var t = _context.Peek();

            if (t.Kind == SqlTokenKind.EndOfFile)
                throw Error("Subquery não fechada dentro de IN(...)", t);

            // controla parênteses
            if (t.Kind == SqlTokenKind.Symbol && t.Text == "(") depth++;
            if (t.Kind == SqlTokenKind.Symbol && t.Text == ")")
            {
                if (depth == 0) break; // para ANTES do ')'
                depth--;
            }

            parts.Add(_context.Consume());
        }

        return _context.TokensToSql(parts).Trim();
    }

    private IReadOnlyList<SqlToken> ReadTokensUntilMatchingParen(string eofError)
    {
        var depth = 0;
        var tokens = new List<SqlToken>();
        while (true)
        {
            var t = _context.Peek();
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
            _context.Consume();
        }

        return tokens;
    }

    private string TokenToSql(SqlToken t)
        => _context.TokenToSql(t);
}
