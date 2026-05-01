namespace DbSqlLikeMem.MySql.Test.Parser;

/// <summary>
/// EN: Covers round-trip SQL expression printing in the MySql parser.
/// PT-br: Cobre o round-trip de impressao de expressoes SQL no parser MySql.
/// </summary>
public sealed class SqlExprPrinterTest(
        ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Verifies parsed expressions can be printed and parsed again without changing the normalized output.
    /// PT-br: Verifica se expressoes parseadas podem ser impressas e parseadas novamente sem alterar a saida normalizada.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataByMySqlVersion(nameof(Expressions))]
    public void ExprPrinter_ShouldAllow_Roundtrip_Parse_Print_Parse(string expr, int minVersion, int version)
    {
        var d = Get(version, v => new MySqlDialect(v));
        var db = Get(version, v => new MySqlDbMock(v));
        if (version < minVersion)
            return;

        var ast1 = SqlExpressionParser.ParseWhere(expr, db, d);
        var printed = SqlExprPrinter.Print(ast1);

        var ast2 = SqlExpressionParser.ParseWhere(printed, db, d);

        // não compara árvore (chato), compara “print normalizado”
        SqlExprPrinter.Print(ast1).Should().Be(SqlExprPrinter.Print(ast2));
    }

    /// <summary>
    /// EN: Provides test data for Expressions.
    /// PT-br: Fornece dados de teste para Expressions.
    /// </summary>
    public static IEnumerable<object[]> Expressions()
    {
        yield return new object[] { "a = 1 AND b = 2 OR c = 3", 0 };
        yield return new object[] { "NOT (a = 1)", 0 };
        yield return new object[] { "a IN (1,2,3)", 0 };
        yield return new object[] { "a IN ((SELECT 1 WHERE 0))", 0 };
        yield return new object[] { "EXISTS(SELECT 1 WHERE 0)", 0 };
        yield return new object[] { "data->'$.name' = 'x'", MySqlDialect.JsonArrowOperatorsMinVersion };
        yield return new object[] { "data->>'$.name' = 'x'", MySqlDialect.JsonArrowOperatorsMinVersion };
    }
}
