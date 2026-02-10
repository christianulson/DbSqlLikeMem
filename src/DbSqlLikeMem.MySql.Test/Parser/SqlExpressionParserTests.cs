namespace DbSqlLikeMem.MySql.Test.Parser;

/// <summary>
/// Auto-generated summary.
/// </summary>
public sealed class SqlExpressionParserTests(
    ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{

    // ----------- Smoke tests: todas as expressões reais encontradas no zip (suportadas) -----------

    /// <summary>
    /// EN: Tests ParseWhere_ShouldNotThrow_ForSupportedRealWorldExpressions behavior.
    /// PT: Testa o comportamento de ParseWhere_ShouldNotThrow_ForSupportedRealWorldExpressions.
    /// </summary>
    [Theory]
    [MemberDataByMySqlVersion(nameof(WhereExpressions_Supported))]
    public void ParseWhere_ShouldNotThrow_ForSupportedRealWorldExpressions(string whereExpr, int version)
    {
        Console.WriteLine("Where: @\"" + whereExpr + "\"");

        var ex = Record.Exception(() => SqlExpressionParser.ParseWhere(whereExpr, new MySqlDialect(version)));
        Assert.Null(ex);
    }


    /// <summary>
    /// Auto-generated summary.
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
        yield return new object[] { "FIND_IN_SET('b', tags)" };
        yield return new object[] { "(a,b) in (@rows)" };
        yield return new object[] { "(a) in ((SELECT 1 WHERE 0))" };
        yield return new object[] { "a in ((SELECT 1 WHERE 0))" };
        yield return new object[] { "EXISTS (SELECT 1 FROM orders o WHERE o.UserId = u.Id AND o.Amount >= 100)" };
    }

    // ----------- Negative tests: coisas que aparecem nos testes atuais mas NÃO fazem parte do subset -----------

    /// <summary>
    /// EN: Tests ParseWhere_ShouldThrow_ForUnsupportedExpressions behavior.
    /// PT: Testa o comportamento de ParseWhere_ShouldThrow_ForUnsupportedExpressions.
    /// </summary>
    [Theory]
    [MemberDataByMySqlVersion(nameof(WhereExpressions_Unsupported))]
    public void ParseWhere_ShouldThrow_ForUnsupportedExpressions(string whereExpr, int version)
    {
        Console.WriteLine("Where: @\"" + whereExpr + "\"");

        Assert.ThrowsAny<InvalidOperationException>(() => SqlExpressionParser.ParseWhere(whereExpr, new MySqlDialect(version)));
    }

    /// <summary>
    /// Auto-generated summary.
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
    /// EN: Tests Precedence_OR_ShouldBindLooserThan_AND behavior.
    /// PT: Testa o comportamento de Precedence_OR_ShouldBindLooserThan_AND.
    /// </summary>
    [Theory]
    [MemberDataMySqlVersion]
    public void Precedence_OR_ShouldBindLooserThan_AND(int version)
    {
        // id = 1 OR id = 2 AND name = 'Bob'
        // esperado: OR( id=1 , AND(id=2, name='Bob') )
        var ast = SqlExpressionParser.ParseWhere("id = 1 OR id = 2 AND name = 'Bob'", new MySqlDialect(version));

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
    /// EN: Tests Parentheses_ShouldOverridePrecedence behavior.
    /// PT: Testa o comportamento de Parentheses_ShouldOverridePrecedence.
    /// </summary>
    [Theory]
    [MemberDataMySqlVersion]
    public void Parentheses_ShouldOverridePrecedence(int version)
    {
        // (id = 1 OR id = 2) AND email IS NULL
        var ast = SqlExpressionParser.ParseWhere("(id = 1 OR id = 2) AND email IS NULL", new MySqlDialect(version));

        var and = Assert.IsType<BinaryExpr>(ast);
        Assert.Equal(SqlBinaryOp.And, and.Op);

        var or = Assert.IsType<BinaryExpr>(and.Left);
        Assert.Equal(SqlBinaryOp.Or, or.Op);

        var isNull = Assert.IsType<IsNullExpr>(and.Right);
        Assert.False(isNull.Negated);
    }

    /// <summary>
    /// EN: Tests Not_ShouldWork behavior.
    /// PT: Testa o comportamento de Not_ShouldWork.
    /// </summary>
    [Theory]
    [MemberDataMySqlVersion]
    public void Not_ShouldWork(int version)
    {
        var ast = SqlExpressionParser.ParseWhere("NOT (id = 1 OR id = 2)", new MySqlDialect(version));

        var not = Assert.IsType<UnaryExpr>(ast);
        Assert.Equal(SqlUnaryOp.Not, not.Op);

        var or = Assert.IsType<BinaryExpr>(not.Expr);
        Assert.Equal(SqlBinaryOp.Or, or.Op);
    }

    /// <summary>
    /// EN: Tests IsNotNull_ShouldProduce_IsNullExpr_Negated behavior.
    /// PT: Testa o comportamento de IsNotNull_ShouldProduce_IsNullExpr_Negated.
    /// </summary>
    [Theory]
    [MemberDataMySqlVersion]
    public void IsNotNull_ShouldProduce_IsNullExpr_Negated(int version)
    {
        var ast = SqlExpressionParser.ParseWhere("email IS NOT NULL", new MySqlDialect(version));
        var n = Assert.IsType<IsNullExpr>(ast);
        Assert.True(n.Negated);
    }

    /// <summary>
    /// EN: Tests In_ShouldParse_List behavior.
    /// PT: Testa o comportamento de In_ShouldParse_List.
    /// </summary>
    [Theory]
    [MemberDataMySqlVersion]
    public void In_ShouldParse_List(int version)
    {
        var ast = SqlExpressionParser.ParseWhere("u.id IN (1,2,3)", new MySqlDialect(version));
        var ins = Assert.IsType<InExpr>(ast);
        Assert.Equal(3, ins.Items.Count);
    }

    /// <summary>
    /// EN: Tests Like_ShouldParse behavior.
    /// PT: Testa o comportamento de Like_ShouldParse.
    /// </summary>
    [Theory]
    [MemberDataMySqlVersion]
    public void Like_ShouldParse(int version)
    {
        var ast = SqlExpressionParser.ParseWhere("name LIKE '%oh%'", new MySqlDialect(version));
        var like = Assert.IsType<LikeExpr>(ast);
        Assert.NotNull(like.Pattern);
    }

    /// <summary>
    /// EN: Tests Identifier_WithAliasDotColumn_ShouldParse behavior.
    /// PT: Testa o comportamento de Identifier_WithAliasDotColumn_ShouldParse.
    /// </summary>
    [Theory]
    [MemberDataMySqlVersion]
    public void Identifier_WithAliasDotColumn_ShouldParse(int version)
    {
        var ast = SqlExpressionParser.ParseWhere("u.id = o.userId", new MySqlDialect(version));
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
    /// EN: Tests Parameter_Tokens_ShouldParse behavior.
    /// PT: Testa o comportamento de Parameter_Tokens_ShouldParse.
    /// </summary>
    [Theory]
    [MemberDataMySqlVersion]
    public void Parameter_Tokens_ShouldParse(int version)
    {
        var d = new MySqlDialect(version);
        Assert.NotNull(SqlExpressionParser.ParseWhere("a = @p", d));
        Assert.NotNull(SqlExpressionParser.ParseWhere("a = :p", d));
        Assert.NotNull(SqlExpressionParser.ParseWhere("a = ?", d));
    }

    /// <summary>
    /// EN: Tests Backtick_Identifier_ShouldParse behavior.
    /// PT: Testa o comportamento de Backtick_Identifier_ShouldParse.
    /// </summary>
    [Theory]
    [MemberDataMySqlVersion]
    public void Backtick_Identifier_ShouldParse(int version)
    {
        var ast = SqlExpressionParser.ParseWhere("`DeletedDtt` IS NULL", new MySqlDialect(version));
        var n = Assert.IsType<IsNullExpr>(ast);
        var id = Assert.IsType<IdentifierExpr>(n.Expr);
        Assert.Equal("DeletedDtt", id.Name);
    }

    /// <summary>
    /// EN: Tests DoubleQuoted_String_ShouldParse behavior.
    /// PT: Testa o comportamento de DoubleQuoted_String_ShouldParse.
    /// </summary>
    [Theory]
    [MemberDataMySqlVersion]
    public void DoubleQuoted_String_ShouldParse(int version)
    {
        var ast = SqlExpressionParser.ParseWhere("name = \"John\"", new MySqlDialect(version));
        var eq = Assert.IsType<BinaryExpr>(ast);
        var lit = Assert.IsType<LiteralExpr>(eq.Right);
        Assert.Equal("John", lit.Value);
    }

    /// <summary>
    /// EN: Tests Printer_ShouldBeStable_ForSimpleExpression behavior.
    /// PT: Testa o comportamento de Printer_ShouldBeStable_ForSimpleExpression.
    /// </summary>
    [Theory]
    [MemberDataMySqlVersion]
    public void Printer_ShouldBeStable_ForSimpleExpression(int version)
    {
        var ast = SqlExpressionParser.ParseWhere("a = 1 AND b = 2", new MySqlDialect(version));
        var s = SqlExprPrinter.Print(ast);

        // só uma checagem básica de que não está vazio e contém operadores esperados
        Assert.Contains("AND", s, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("=", s, StringComparison.OrdinalIgnoreCase);
    }
}
