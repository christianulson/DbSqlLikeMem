namespace DbSqlLikeMem.Sqlite.Test.Parser;

/// <summary>
/// EN: Covers round-trip SQL expression printing in the Sqlite parser.
/// PT-br: Cobre o round-trip de impressao de expressoes SQL no parser Sqlite.
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
    [MemberDataBySqliteVersion(nameof(Expressions))]
    public void ExprPrinter_ShouldAllow_Roundtrip_Parse_Print_Parse(string expr, int version)
    {
        var d = Get(version, v => new SqliteDialect(v));
        var db = Get(version, v => new SqliteDbMock(v));
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
        yield return new object[] { "a = 1 AND b = 2 OR c = 3" };
        yield return new object[] { "NOT (a = 1)" };
        yield return new object[] { "a IN (1,2,3)" };
        yield return new object[] { "a IN ((SELECT 1 WHERE 0))" };
        yield return new object[] { "EXISTS(SELECT 1 WHERE 0)" };
    }

    /// <summary>
    /// EN: Provides SQLite JSON arrow expressions that require a newer dialect version.
    /// PT-br: Fornece expressoes JSON arrow do SQLite que exigem uma versao mais nova do dialeto.
    /// </summary>
    public static IEnumerable<object[]> Expressions_JsonArrowOperators()
    {
        yield return new object[] { "data->'$.name' = 'x'" };
        yield return new object[] { "data->>'$.name' = 'x'" };
    }

    /// <summary>
    /// EN: Verifies SQLite JSON arrow expressions round-trip only on supported versions.
    /// PT-br: Verifica se expressoes JSON arrow do SQLite fazem round-trip apenas em versoes suportadas.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataBySqliteVersion(nameof(Expressions_JsonArrowOperators), VersionGraterOrEqual = 338)]
    public void ExprPrinter_ShouldAllow_Roundtrip_Parse_Print_Parse_ForJsonArrowOperators(string expr, int version)
    {
        var d = Get(version, v => new SqliteDialect(v));
        var db = Get(version, v => new SqliteDbMock(v));
        var ast1 = SqlExpressionParser.ParseWhere(expr, db, d);
        var printed = SqlExprPrinter.Print(ast1);

        var ast2 = SqlExpressionParser.ParseWhere(printed, db, d);
        SqlExprPrinter.Print(ast1).Should().Be(SqlExprPrinter.Print(ast2));
    }
}
