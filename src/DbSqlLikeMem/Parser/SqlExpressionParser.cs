namespace DbSqlLikeMem;

internal sealed class SqlExpressionParser(
    IReadOnlyList<SqlToken> toks,
    ISqlDialect dialect
    )
{
    private readonly IReadOnlyList<SqlToken> _toks = toks
        ?? throw new ArgumentNullException(nameof(toks));
    private readonly ISqlDialect _dialect = dialect ?? throw new ArgumentNullException(nameof(dialect));
    private int _i;

    public static SqlExpr ParseWhere(
        string whereSql,
        ISqlDialect dialect)
    {
        var d = dialect;
        var toks = new SqlTokenizer(whereSql, d).Tokenize();
        var p = new SqlExpressionParser(toks, d);
        var expr = p.ParseExpression(0);
        p.ExpectEnd();
        return expr;
    }

    public static SqlExpr ParseScalar(string sql, ISqlDialect dialect)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(sql);
        ArgumentNullException.ThrowIfNull(dialect);
        var d = dialect;
        var toks = new SqlTokenizer(sql, d).Tokenize();
        var p = new SqlExpressionParser(toks, d);
        var expr = p.ParseExpression(0);
        p.ExpectEnd();
        return expr;
    }

    // Pratt: parse com binding power
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
        if (_dialect.SupportsJsonArrowOperators
            && TryParseJsonArrowInfix(ref left, minBp)) continue;

            // * /
            if (TryParseMulDivInfix(ref left, minBp)) continue;

            // + -
            if (TryParseAddSubInfix(ref left, minBp)) continue;

            // BETWEEN / NOT BETWEEN (NOT BETWEEN é coberto no TryParseNotInfix)
            if (TryParseBetweenInfix(ref left, minBp)) continue;

            // AND / OR
            if (TryParseAndOrInfix(ref left, minBp)) continue;

            // comparações: = != <> >= <= > <
            if (TryParseComparisonInfix(ref left, minBp)) continue;

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

        if (IsKeyword(t2, "LIKE"))
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

        var pattern = ParseExpression(51);
        left = new UnaryExpr(SqlUnaryOp.Not, new LikeExpr(left, pattern));
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
        if (!IsKeyword(t, "LIKE"))
            return false;

        var (lbp, rbp) = (50, 51);
        if (lbp < minBp) return false;

        Consume(); // LIKE
        var pattern = ParseExpression(rbp);
        left = new LikeExpr(left, pattern);
        return true;
    }

    private bool TryParseRegexpInfix(ref SqlExpr left, int minBp)
    {
        var t = Peek();
        if (!IsKeywordOrIdentifierWord(t, "REGEXP"))
            return false;

        var (lbp, rbp) = (50, 51);
        if (lbp < minBp) return false;

        Consume(); // REGEXP
        var pattern = ParseExpression(rbp);
        left = new BinaryExpr(SqlBinaryOp.Regexp, left, pattern);
        return true;
    }


    private bool TryParseJsonArrowInfix(ref SqlExpr left, int minBp)
    {
        var t = Peek();
        if (t.Kind != SqlTokenKind.Operator || (t.Text != "->" && t.Text != "->>" && t.Text != "#>" && t.Text != "#>>"))
            return false;

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
        const int rbp = 131;
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

        var op = t.Text == "+" ? SqlBinaryOp.Add : SqlBinaryOp.Subtract;
        left = new BinaryExpr(op, left, right);
        return true;
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

        Consume();
        var right = ParseExpression(rbp);
        left = new BinaryExpr(bop, left, right);
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
        expr = new CallExpr("INTERVAL", [new LiteralExpr(next.Text)]);
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

        if (!decimal.TryParse(t.Text, NumberStyles.Any, CultureInfo.InvariantCulture, out var d))
            throw Error($"Número inválido: {t.Text}", t);

        expr = new LiteralExpr(d);
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
        if (IsSymbol(Peek(), "("))
        {
            var call = ParseCallAfterName(name);

            // ✅ Window function: ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...)
            if (IsKeywordOrIdentifierWord(Peek(), "OVER"))
            {
                Consume(); // OVER
                var spec = ParseWindowSpec();
                expr = new WindowFunctionExpr(call.Name, call.Args, spec, call.Distinct);
                return true;
            }

            expr = call;
            return true;
        }

        expr = ParseIdentifierChainOrColumn(name);
        return true;
    }

    private CallExpr ParseCallAfterName(string name)
    {
        Console.WriteLine($"[DEBUG] ParseCallAfterName: {name}");
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
                    args.Add(ParseExpression(0));
                }

                if (!IsSymbol(Peek(), ","))
                    break;

                Consume();
            }
        }

        ExpectSymbol(")");
        return new CallExpr(name, args, distinct);
    }

    private WindowSpec ParseWindowSpec()
    {
        // OVER ( ... )
        ExpectSymbol("(");

        var parts = new List<SqlExpr>();
        var order = new List<WindowOrderItem>();

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

        ExpectSymbol(")");
        return new WindowSpec(parts, order);
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

        return parts.Count switch
        {
            1 => new IdentifierExpr(parts[0]),                 // col
            2 => new ColumnExpr(parts[0], parts[1]),           // alias.col  OR table.col  OR alias.*
            _ => new RawSqlExpr(string.Join(".", parts))       // db.table.col (por enquanto)
        };
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
            return bop is SqlBinaryOp.Eq
                or SqlBinaryOp.Neq
                or SqlBinaryOp.Greater
                or SqlBinaryOp.GreaterOrEqual
                or SqlBinaryOp.Less
                or SqlBinaryOp.LessOrEqual
                or SqlBinaryOp.NullSafeEq;
        }

        bop = default;
        return false;
    }

    private static bool IsKeywordOrIdentifierWord(SqlToken t, string word)
        => t.Text.Equals(word, StringComparison.OrdinalIgnoreCase);

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

    private static string TokenToSql(SqlToken t)
    {
        // reconstrói SQL “ok” pra debug/parse posterior
        return t.Kind switch
        {
            SqlTokenKind.String => $"'{t.Text.Replace("'", "\\'", StringComparison.OrdinalIgnoreCase)}'",
            _ => t.Text
        };
    }
}
