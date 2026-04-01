using FluentAssertions;

namespace DbSqlLikeMem.MySql.Test.Parser;

/// <summary>
/// EN: Covers WHERE expression parsing for the MySql dialect.
/// PT: Cobre o parsing de expressoes WHERE para o dialeto MySql.
/// </summary>
public sealed class SqlExpressionParserTests(
    ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{

    // ----------- Smoke tests: todas as expressões reais encontradas no zip (suportadas) -----------

    /// <summary>
    /// EN: Verifies the parser accepts supported real-world WHERE expressions.
    /// PT: Verifica se o parser aceita expressoes WHERE reais suportadas.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataByMySqlVersion(nameof(WhereExpressions_Supported))]
    public void ParseWhere_ShouldNotThrow_ForSupportedRealWorldExpressions(string whereExpr, int version)
    {
        var d = Get(version, v => new MySqlDialect(v));
        var db = Get(version, v => new MySqlDbMock(v));
        Console.WriteLine("Where: @\"" + whereExpr + "\"");

        var ex = Record.Exception(() => SqlExpressionParser.ParseWhere(whereExpr, db, d));
        ex.Should().BeNull();
    }


    /// <summary>
    /// EN: Provides test data for WhereExpressions_Supported.
    /// PT: Fornece dados de teste para WhereExpressions_Supported.
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
    /// EN: Verifies unsupported WHERE expressions raise an error.
    /// PT: Verifica se expressoes WHERE nao suportadas geram erro.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataByMySqlVersion(nameof(WhereExpressions_Unsupported))]
    public void ParseWhere_ShouldThrow_ForUnsupportedExpressions(string whereExpr, int version)
    {
        var d = Get(version, v => new MySqlDialect(v));
        var db = Get(version, v => new MySqlDbMock(v));
        Console.WriteLine("Where: @\"" + whereExpr + "\"");

        if (whereExpr.Contains(SqlConst.JSON_TABLE, StringComparison.OrdinalIgnoreCase))
        {
            var ex = FluentActions.Invoking(() => SqlExpressionParser.ParseWhere(whereExpr, db, d)).Should().Throw<NotSupportedException>().Which;
            ex.Message.Should().Contain(SqlConst.JSON_TABLE);
            return;
        }

        FluentActions.Invoking(() => SqlExpressionParser.ParseWhere(whereExpr, db, d)).Should().Throw<InvalidOperationException>();
    }

    /// <summary>
    /// EN: Provides test data for WhereExpressions_Unsupported.
    /// PT: Fornece dados de teste para WhereExpressions_Unsupported.
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
        yield return new object[] { "JSON_TABLE(col, '$[*]' COLUMNS(x INT PATH '$'))" };
    }

    // ----------- Regras (cenários extra) -----------

    /// <summary>
    /// EN: Verifies OR binds looser than AND.
    /// PT: Verifica se OR tem precedencia menor que AND.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void Precedence_OR_ShouldBindLooserThan_AND(int version)
    {
        var d = Get(version, v => new MySqlDialect(v));
        var db = Get(version, v => new MySqlDbMock(v));
        // id = 1 OR id = 2 AND name = 'Bob'
        // esperado: OR( id=1 , AND(id=2, name='Bob') )
        var ast = SqlExpressionParser.ParseWhere("id = 1 OR id = 2 AND name = 'Bob'", db,d);

        var or = ast.Should().BeOfType<BinaryExpr>().Subject;
        or.Op.Should().Be(SqlBinaryOp.Or);

        var leftEq = or.Left.Should().BeOfType<BinaryExpr>().Subject;
        leftEq.Op.Should().Be(SqlBinaryOp.Eq);

        var and = or.Right.Should().BeOfType<BinaryExpr>().Subject;
        and.Op.Should().Be(SqlBinaryOp.And);

        var andLeft = and.Left.Should().BeOfType<BinaryExpr>().Subject;
        andLeft.Op.Should().Be(SqlBinaryOp.Eq);

        var andRight = and.Right.Should().BeOfType<BinaryExpr>().Subject;
        andRight.Op.Should().Be(SqlBinaryOp.Eq);
    }

    /// <summary>
    /// EN: Verifies parentheses override operator precedence.
    /// PT: Verifica se parenteses sobrescrevem a precedencia dos operadores.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void Parentheses_ShouldOverridePrecedence(int version)
    {
        var d = Get(version, v => new MySqlDialect(v));
        var db = Get(version, v => new MySqlDbMock(v));
        // (id = 1 OR id = 2) AND email IS NULL
        var ast = SqlExpressionParser.ParseWhere("(id = 1 OR id = 2) AND email IS NULL", db, d);

        var and = ast.Should().BeOfType<BinaryExpr>().Subject;
        and.Op.Should().Be(SqlBinaryOp.And);

        var or = and.Left.Should().BeOfType<BinaryExpr>().Subject;
        or.Op.Should().Be(SqlBinaryOp.Or);

        var isNull = and.Right.Should().BeOfType<IsNullExpr>().Subject;
        isNull.Negated.Should().BeFalse();
    }

    /// <summary>
    /// EN: Verifies NOT expressions are parsed correctly.
    /// PT: Verifica se expressoes NOT sao parseadas corretamente.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void Not_ShouldWork(int version)
    {
        var d = Get(version, v => new MySqlDialect(v));
        var db = Get(version, v => new MySqlDbMock(v));
        var ast = SqlExpressionParser.ParseWhere("NOT (id = 1 OR id = 2)", db, d);

        var not = ast.Should().BeOfType<UnaryExpr>().Subject;
        not.Op.Should().Be(SqlUnaryOp.Not);

        var or = not.Expr.Should().BeOfType<BinaryExpr>().Subject;
        or.Op.Should().Be(SqlBinaryOp.Or);
    }

    /// <summary>
    /// EN: Verifies IS NOT NULL is parsed as a negated IS NULL expression.
    /// PT: Verifica se IS NOT NULL e parseado como uma expressao IS NULL negada.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void IsNotNull_ShouldProduce_IsNullExpr_Negated(int version)
    {
        var d = Get(version, v => new MySqlDialect(v));
        var db = Get(version, v => new MySqlDbMock(v));
        var ast = SqlExpressionParser.ParseWhere("email IS NOT NULL", db, d);
        var n = ast.Should().BeOfType<IsNullExpr>().Subject;
        n.Negated.Should().BeTrue();
    }

    /// <summary>
    /// EN: Verifies IN lists are parsed correctly.
    /// PT: Verifica se listas IN sao parseadas corretamente.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void In_ShouldParse_List(int version)
    {
        var d = Get(version, v => new MySqlDialect(v));
        var db = Get(version, v => new MySqlDbMock(v));
        var ast = SqlExpressionParser.ParseWhere("u.id IN (1,2,3)", db, d);
        var ins = ast.Should().BeOfType<InExpr>().Subject;
        ins.Items.Should().HaveCount(3);
    }

    /// <summary>
    /// EN: Verifies LIKE expressions are parsed correctly.
    /// PT: Verifica se expressoes LIKE sao parseadas corretamente.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void Like_ShouldParse(int version)
    {
        var d = Get(version, v => new MySqlDialect(v));
        var db = Get(version, v => new MySqlDbMock(v));
        var ast = SqlExpressionParser.ParseWhere("name LIKE '%oh%'", db, d);
        var like = ast.Should().BeOfType<LikeExpr>().Subject;
        like.Pattern.Should().NotBeNull();
    }

    /// <summary>
    /// EN: Verifies aliased column identifiers are parsed correctly.
    /// PT: Verifica se identificadores de coluna com alias sao parseados corretamente.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void Identifier_WithAliasDotColumn_ShouldParse(int version)
    {
        var d = Get(version, v => new MySqlDialect(v));
        var db = Get(version, v => new MySqlDbMock(v));
        var ast = SqlExpressionParser.ParseWhere("u.id = o.userId", db, d);
        var eq = ast.Should().BeOfType<BinaryExpr>().Subject;
        eq.Op.Should().Be(SqlBinaryOp.Eq);

        var l = eq.Left.Should().BeOfType<ColumnExpr>().Subject;
        var r = eq.Right.Should().BeOfType<ColumnExpr>().Subject;

        l.Qualifier.Should().Be("u");
        l.Name.Should().Be("id");

        r.Qualifier.Should().Be("o");
        r.Name.Should().Be("userId");
    }

    /// <summary>
    /// EN: Verifies parameter tokens are parsed correctly.
    /// PT: Verifica se tokens de parametro sao parseados corretamente.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void Parameter_Tokens_ShouldParse(int version)
    {
        var d = Get(version, v => new MySqlDialect(v));
        var db = Get(version, v => new MySqlDbMock(v));
        SqlExpressionParser.ParseWhere("a = @p", db, d).Should().NotBeNull();
        SqlExpressionParser.ParseWhere("a = :p", db, d).Should().NotBeNull();
        SqlExpressionParser.ParseWhere("a = ?", db, d).Should().NotBeNull();
    }

    /// <summary>
    /// EN: Verifies backticked identifiers are parsed correctly.
    /// PT: Verifica se identificadores entre backticks sao parseados corretamente.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void Backtick_Identifier_ShouldParse(int version)
    {
        var d = Get(version, v => new MySqlDialect(v));
        var db = Get(version, v => new MySqlDbMock(v));
        var ast = SqlExpressionParser.ParseWhere("`DeletedDtt` IS NULL", db, d);
        var n = ast.Should().BeOfType<IsNullExpr>().Subject;
        var id = n.Expr.Should().BeOfType<IdentifierExpr>().Subject;
        id.Name.Should().Be("DeletedDtt");
    }

    /// <summary>
    /// EN: Verifies double-quoted string literals are parsed correctly.
    /// PT: Verifica se literais de string entre aspas duplas sao parseados corretamente.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void DoubleQuoted_String_ShouldParse(int version)
    {
        var d = Get(version, v => new MySqlDialect(v));
        var db = Get(version, v => new MySqlDbMock(v));
        var ast = SqlExpressionParser.ParseWhere("name = \"John\"", db,d);
        var eq = ast.Should().BeOfType<BinaryExpr>().Subject;
        var lit = eq.Right.Should().BeOfType<LiteralExpr>().Subject;
        lit.Value.Should().Be("John");
    }

    /// <summary>
    /// EN: Verifies the printer is stable for a simple expression.
    /// PT: Verifica se o printer e estavel para uma expressao simples.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void Printer_ShouldBeStable_ForSimpleExpression(int version)
    {
        var d = Get(version, v => new MySqlDialect(v));
        var db = Get(version, v => new MySqlDbMock(v));
        var ast = SqlExpressionParser.ParseWhere("a = 1 AND b = 2", db, d);
        var s = SqlExprPrinter.Print(ast);

        // só uma checagem básica de que não está vazio e contém operadores esperados
        s.Should().Contain(SqlConst.AND);
        s.Should().Contain("=");
    }
}
