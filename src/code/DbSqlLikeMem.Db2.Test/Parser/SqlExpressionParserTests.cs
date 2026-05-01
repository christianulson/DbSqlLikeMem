namespace DbSqlLikeMem.Db2.Test.Parser;

/// <summary>
/// EN: Covers WHERE expression parsing for the Db2 dialect.
/// PT-br: Cobre o parsing de expressoes WHERE para o dialeto Db2.
/// </summary>
public sealed class SqlExpressionParserTests(
    ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{

    // ----------- Smoke tests: todas as expressões reais encontradas no zip (suportadas) -----------

    /// <summary>
    /// EN: Verifies the parser accepts supported real-world WHERE expressions.
    /// PT-br: Verifica se o parser aceita expressoes WHERE reais suportadas.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataByDb2Version(nameof(WhereExpressions_Supported))]
    public void ParseWhere_ShouldNotThrow_ForSupportedRealWorldExpressions(string whereExpr, int version)
    {
        Console.WriteLine("Where: @\"" + whereExpr + "\"");

        var ex = Record.Exception(() => SqlExpressionParser.ParseWhere(whereExpr, new Db2DbMock(), Get(version, v => new Db2Dialect(v))));
        Assert.Null(ex);
    }


    /// <summary>
    /// EN: Provides supported WHERE expressions used in parser tests.
    /// PT-br: Fornece expressões WHERE suportadas usadas nos testes do parser.
    /// </summary>
    public static IEnumerable<object[]> WhereExpressions_Supported()
    {
        yield return new object[] { "Id = 1" };
        yield return new object[] { "id = 1" };
        yield return new object[] { "id = 1 OR id = 2 AND name = 'Bob'" };
        yield return new object[] { "id = 1 OR id = 3" };
        yield return new object[] { "(id = 1 OR id = 2) AND email IS NULL" };
        yield return new object[] { "(o.userId = u.id OR o.userId = 0)" };
        yield return new object[] { "u.id IN (1,2)" };
        yield return new object[] { "id = 2" };
        yield return new object[] { "name = 'john'" };
        yield return new object[] { "id = '2'" };
        yield return new object[] { "id IN (1,3)" };
        yield return new object[] { "email IS NOT NULL" };
        yield return new object[] { "id >= 2 AND id <= 3" };
        yield return new object[] { "id != 2" };
        yield return new object[] { "name LIKE '%oh%'" };
        yield return new object[] { "id = 1 aNd name = 'John'" };
        yield return new object[] { "o.UserId = u.Id" };
        yield return new object[] { "a = @p" };
        yield return new object[] { "a IS NULL" };
        yield return new object[] { "a>=@p" };
        yield return new object[] { "a <= @p" };
        yield return new object[] { "a < @p and b = 1" };
        yield return new object[] { "a IS NULL and b = 1" };
        yield return new object[] { "a = @p2 and b = @p" };
        yield return new object[] { "a = @p2 and b IS NULL" };
        yield return new object[] { "a in (@ids)" };
        yield return new object[] { @"a in (@ids_0,@ids_1,@ids_2)" };
        yield return new object[] { "g in (@gids)" };
        yield return new object[] { "s in (@ss)" };
        yield return new object[] { "(a) in (@rows)" };
        yield return new object[] { "id in (@rows)" };
        yield return new object[] { "id in (@row)" };
        yield return new object[] { "id = 999" };
        yield return new object[] { "id = 10" };
        yield return new object[] { "id = @id" };
        yield return new object[] { "id = 42" };
        yield return new object[] { "grp = 'X' AND id = 1" };
        yield return new object[] { "Id = @Id" };
        yield return new object[] { "U.Id = @Id" };
        yield return new object[] { "U.Id = UT.UserId" };
        yield return new object[] { "first = @f AND second = @s" };
        yield return new object[] { "name LIKE 'a%'" };
        yield return new object[] { "u.id = o.userId" };
        yield return new object[] { "u.id = o.userId AND o.status = 'paid'" };
        yield return new object[] { "EXISTS (SELECT 1 FROM orders o WHERE o.UserId = u.Id)" };
        yield return new object[] { "NOT EXISTS (SELECT 1 FROM orders o WHERE o.UserId = u.Id)" };
        yield return new object[] { "(a,b) in (@rows)" };
        yield return new object[] { "(a) in ((SELECT 1 WHERE 0))" };
        yield return new object[] { "a in ((SELECT 1 WHERE 0))" };
        yield return new object[] { "EXISTS (SELECT 1 FROM orders o WHERE o.UserId = u.Id AND o.Amount >= 100)" };
    }

    // ----------- Negative tests: coisas que aparecem nos testes atuais mas NÃO fazem parte do subset -----------

    /// <summary>
    /// EN: Verifies unsupported WHERE expressions raise an error.
    /// PT-br: Verifica se expressoes WHERE nao suportadas geram erro.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataByDb2Version(nameof(WhereExpressions_Unsupported))]
    public void ParseWhere_ShouldThrow_ForUnsupportedExpressions(string whereExpr, int version)
    {
        Console.WriteLine("Where: @\"" + whereExpr + "\"");

        var ex = Assert.ThrowsAny<Exception>(() => SqlExpressionParser.ParseWhere(whereExpr, new Db2DbMock(), Get(version, v => new Db2Dialect(v))));
        Assert.True(ex is InvalidOperationException or NotSupportedException);
    }

    /// <summary>
    /// EN: Provides unsupported WHERE expressions used in parser tests.
    /// PT-br: Fornece expressões WHERE não suportadas usadas nos testes do parser.
    /// </summary>
    public static IEnumerable<object[]> WhereExpressions_Unsupported()
    {
        yield return new object[] { "(a,b) in @rows" };
        yield return new object[] { "Active = 1) u" };
        yield return new object[] { "Amount > 50) o ON o.UserId = u.Id" };
        yield return new object[] { "a=@p, b=2" };
        yield return new object[] { "a>@p)" };
        yield return new object[] { "aIS NULL" };
        yield return new object[] { "aIS NULL)" };
        yield return new object[] { "aIS NULL, b=2" };
        yield return new object[] { "id <= 2)" };
        yield return new object[] { "MATCH(title) AGAINST('x' IN BOOLEAN MODE)" };
        yield return new object[] { "JSON_TABLE(col, '$[*]' COLUMNS(x INT PATH '$'))" };
    }

    // ----------- Regras (cenários extra) -----------

    /// <summary>
    /// EN: Verifies OR binds looser than AND.
    /// PT-br: Verifica se OR tem precedencia menor que AND.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataDb2Version]
    public void Precedence_OR_ShouldBindLooserThan_AND(int version)
    {
        // id = 1 OR id = 2 AND name = 'Bob'
        // esperado: OR( id=1 , AND(id=2, name='Bob') )
        var ast = SqlExpressionParser.ParseWhere("id = 1 OR id = 2 AND name = 'Bob'", new Db2DbMock(), Get(version, v => new Db2Dialect(v)));

        var or = Assert.IsType<BinaryExpr>(ast);
        Assert.Equal(SqlBinaryOp.Or, or.Op);

        var leftEq = Assert.IsType<BinaryExpr>(or.Left);
        Assert.Equal(SqlBinaryOp.Eq, leftEq.Op);

        var and = Assert.IsType<BinaryExpr>(or.Right);
        Assert.Equal(SqlBinaryOp.And, and.Op);

        var andLeft = Assert.IsType<BinaryExpr>(and.Left);
        Assert.Equal(SqlBinaryOp.Eq, andLeft.Op);

        var andRight = Assert.IsType<BinaryExpr>(and.Right);
        Assert.Equal(SqlBinaryOp.Eq, andRight.Op);
    }

    /// <summary>
    /// EN: Verifies parentheses override operator precedence.
    /// PT-br: Verifica se parenteses sobrescrevem a precedencia dos operadores.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataDb2Version]
    public void Parentheses_ShouldOverridePrecedence(int version)
    {
        // (id = 1 OR id = 2) AND email IS NULL
        var ast = SqlExpressionParser.ParseWhere("(id = 1 OR id = 2) AND email IS NULL", new Db2DbMock(), Get(version, v => new Db2Dialect(v)));

        var and = Assert.IsType<BinaryExpr>(ast);
        Assert.Equal(SqlBinaryOp.And, and.Op);

        var or = Assert.IsType<BinaryExpr>(and.Left);
        Assert.Equal(SqlBinaryOp.Or, or.Op);

        var isNull = Assert.IsType<IsNullExpr>(and.Right);
        Assert.False(isNull.Negated);
    }

    /// <summary>
    /// EN: Verifies NOT expressions are parsed correctly.
    /// PT-br: Verifica se expressoes NOT sao parseadas corretamente.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataDb2Version]
    public void Not_ShouldWork(int version)
    {
        var ast = SqlExpressionParser.ParseWhere("NOT (id = 1 OR id = 2)", new Db2DbMock(), Get(version, v => new Db2Dialect(v)));

        var not = Assert.IsType<UnaryExpr>(ast);
        Assert.Equal(SqlUnaryOp.Not, not.Op);

        var or = Assert.IsType<BinaryExpr>(not.Expr);
        Assert.Equal(SqlBinaryOp.Or, or.Op);
    }

    /// <summary>
    /// EN: Verifies IS NOT NULL is parsed as a negated IS NULL expression.
    /// PT-br: Verifica se IS NOT NULL e parseado como uma expressao IS NULL negada.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataDb2Version]
    public void IsNotNull_ShouldProduce_IsNullExpr_Negated(int version)
    {
        var ast = SqlExpressionParser.ParseWhere("email IS NOT NULL", new Db2DbMock(), Get(version, v => new Db2Dialect(v)));
        var n = Assert.IsType<IsNullExpr>(ast);
        Assert.True(n.Negated);
    }

    /// <summary>
    /// EN: Verifies IN lists are parsed correctly.
    /// PT-br: Verifica se listas IN sao parseadas corretamente.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataDb2Version]
    public void In_ShouldParse_List(int version)
    {
        var ast = SqlExpressionParser.ParseWhere("u.id IN (1,2,3)", new Db2DbMock(), Get(version, v => new Db2Dialect(v)));
        var ins = Assert.IsType<InExpr>(ast);
        Assert.Equal(3, ins.Items.Count);
    }

    /// <summary>
    /// EN: Verifies LIKE expressions are parsed correctly.
    /// PT-br: Verifica se expressoes LIKE sao parseadas corretamente.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataDb2Version]
    public void Like_ShouldParse(int version)
    {
        var ast = SqlExpressionParser.ParseWhere("name LIKE '%oh%'", new Db2DbMock(), Get(version, v => new Db2Dialect(v)));
        var like = Assert.IsType<LikeExpr>(ast);
        Assert.NotNull(like.Pattern);
    }

    /// <summary>
    /// EN: Verifies LIKE preserves the optional ESCAPE expression in the AST.
    /// PT-br: Verifica se LIKE preserva a expressão opcional ESCAPE na AST.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataDb2Version]
    public void Like_WithEscapeClause_ShouldParseEscapeExpression(int version)
    {
        var ast = SqlExpressionParser.ParseWhere("name LIKE 'Jo#_%' ESCAPE '#'", new Db2DbMock(), Get(version, v => new Db2Dialect(v)));
        var like = Assert.IsType<LikeExpr>(ast);
        var escape = Assert.IsType<LiteralExpr>(like.Escape);

        escape.Value.Should().Be("#");
    }

    /// <summary>
    /// EN: Verifies LIKE rejects literal ESCAPE expressions with more than one character.
    /// PT-br: Verifica se LIKE rejeita expressões literais ESCAPE com mais de um caractere.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataDb2Version]
    public void Like_WithMultiCharacterEscapeLiteral_ShouldThrowActionableError(int version)
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlExpressionParser.ParseWhere("name LIKE 'Jo#_%' ESCAPE '##'", new Db2DbMock(), Get(version, v => new Db2Dialect(v))));

        Assert.Contains("single character", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Verifies LIKE rejects parameter ESCAPE expressions with more than one character.
    /// PT-br: Verifica se LIKE rejeita expressões ESCAPE parametrizadas com mais de um caractere.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataDb2Version]
    public void Like_WithMultiCharacterEscapeParameter_ShouldThrowActionableError(int version)
    {
        var parameters = new Db2DataParameterCollectionMock();
        parameters.AddWithValue("@esc", "##");

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlExpressionParser.ParseWhere("name LIKE 'Jo#_%' ESCAPE @esc", new Db2DbMock(), Get(version, v => new Db2Dialect(v)), parameters));

        Assert.Contains("single character", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Verifies DB2 accepts NEXT VALUE FOR sequence expressions.
    /// PT-br: Verifica se o DB2 aceita expressoes NEXT VALUE FOR.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataDb2Version]
    public void SequenceExpression_NextValueFor_ShouldParse(int version)
    {
        var expr = SqlExpressionParser.ParseScalar("NEXT VALUE FOR sales.seq_orders", new Db2DbMock(), Get(version, v => new Db2Dialect(v)));
        var call = Assert.IsType<CallExpr>(expr);

        Assert.Equal("NEXT_VALUE_FOR", call.Name, StringComparer.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Verifies DB2 accepts PREVIOUS VALUE FOR sequence expressions.
    /// PT-br: Verifica se o DB2 aceita expressoes PREVIOUS VALUE FOR.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataDb2Version]
    public void SequenceExpression_PreviousValueFor_ShouldParse(int version)
    {
        var expr = SqlExpressionParser.ParseScalar("PREVIOUS VALUE FOR sales.seq_orders", new Db2DbMock(), Get(version, v => new Db2Dialect(v)));
        var call = Assert.IsType<CallExpr>(expr);

        Assert.Equal("PREVIOUS_VALUE_FOR", call.Name, StringComparer.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Verifies aliased column identifiers are parsed correctly.
    /// PT-br: Verifica se identificadores de coluna com alias sao parseados corretamente.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataDb2Version]
    public void Identifier_WithAliasDotColumn_ShouldParse(int version)
    {
        var ast = SqlExpressionParser.ParseWhere("u.id = o.userId", new Db2DbMock(), Get(version, v => new Db2Dialect(v)));
        var eq = Assert.IsType<BinaryExpr>(ast);
        Assert.Equal(SqlBinaryOp.Eq, eq.Op);

        var l = Assert.IsType<ColumnExpr>(eq.Left);
        var r = Assert.IsType<ColumnExpr>(eq.Right);

        Assert.Equal("u", l.Qualifier);
        Assert.Equal("id", l.Name);

        Assert.Equal("o", r.Qualifier);
        Assert.Equal("userId", r.Name);
    }

    /// <summary>
    /// EN: Verifies parameter tokens are parsed correctly.
    /// PT-br: Verifica se tokens de parametro sao parseados corretamente.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataDb2Version]
    public void Parameter_Tokens_ShouldParse(int version)
    {
        var d = Get(version, v => new Db2Dialect(v));
        var db = new Db2DbMock();
        Assert.NotNull(SqlExpressionParser.ParseWhere("a = @p", db, d));
        Assert.NotNull(SqlExpressionParser.ParseWhere("a = :p", db, d));
        Assert.NotNull(SqlExpressionParser.ParseWhere("a = ?", db, d));
    }

    /// <summary>
    /// EN: Verifies backticked identifiers are parsed correctly.
    /// PT-br: Verifica se identificadores entre backticks sao parseados corretamente.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataDb2Version]
    public void Backtick_Identifier_ShouldThrow(int version)
    {
        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlExpressionParser.ParseWhere("`DeletedDtt` IS NULL", new Db2DbMock(), Get(version, v => new Db2Dialect(v))));

        Assert.Contains("alias/identificadores", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("'`'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Verifies double-quoted string literals are parsed correctly.
    /// PT-br: Verifica se literais de string entre aspas duplas sao parseados corretamente.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataDb2Version]
    public void DoubleQuoted_Identifier_ShouldParse(int version)
    {
        var ast = SqlExpressionParser.ParseWhere("name = \"John\"", new Db2DbMock(), Get(version, v => new Db2Dialect(v)));
        var eq = Assert.IsType<BinaryExpr>(ast);
        var id = Assert.IsType<IdentifierExpr>(eq.Right);
        Assert.Equal("John", id.Name);
    }

    /// <summary>
    /// EN: Verifies the null-safe operator is rejected.
    /// PT-br: Verifica se o operador null-safe e rejeitado.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataDb2Version]
    public void NullSafe_Operator_ShouldThrow(int version)
    {
        Assert.ThrowsAny<InvalidOperationException>(() =>
            SqlExpressionParser.ParseWhere("a <=> b", new Db2DbMock(), Get(version, v => new Db2Dialect(v))));
    }

    /// <summary>
    /// EN: Verifies the printer is stable for a simple expression.
    /// PT-br: Verifica se o printer e estavel para uma expressao simples.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataDb2Version]
    public void Printer_ShouldBeStable_ForSimpleExpression(int version)
    {
        var ast = SqlExpressionParser.ParseWhere("a = 1 AND b = 2", new Db2DbMock(), Get(version, v => new Db2Dialect(v)));
        var s = SqlExprPrinter.Print(ast);

        // só uma checagem básica de que não está vazio e contém operadores esperados
        Assert.Contains(SqlConst.AND, s, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("=", s, StringComparison.OrdinalIgnoreCase);
    }
}
