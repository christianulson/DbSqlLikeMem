namespace DbSqlLikeMem.Npgsql.Test.Parser;
/// <summary>
/// Auto-generated summary.
/// </summary>
public sealed class SqlExprPrinterTest(
        ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Tests ExprPrinter_ShouldAllow_Roundtrip_Parse_Print_Parse behavior.
    /// PT: Testa o comportamento de ExprPrinter_ShouldAllow_Roundtrip_Parse_Print_Parse.
    /// </summary>
    [Theory]
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    [MemberDataByNpgsqlVersion(nameof(Expressions))]
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public void ExprPrinter_ShouldAllow_Roundtrip_Parse_Print_Parse(string expr, int version)
    {
        var d = new NpgsqlDialect(version);
        var ast1 = SqlExpressionParser.ParseWhere(expr, d);
        var printed = SqlExprPrinter.Print(ast1);

        var ast2 = SqlExpressionParser.ParseWhere(printed, d);

        // não compara árvore (chato), compara “print normalizado”
        Assert.Equal(SqlExprPrinter.Print(ast1), SqlExprPrinter.Print(ast2));
    }

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public static IEnumerable<object[]> Expressions()
    {
        yield return new object[] { "a = 1 AND b = 2 OR c = 3" };
        yield return new object[] { "NOT (a = 1)" };
        yield return new object[] { "x IS NULL OR y IS NOT NULL" };
        yield return new object[] { "a BETWEEN 1 AND 2" };
        yield return new object[] { "a IN (1,2,3)" };
        yield return new object[] { "a IN ((SELECT 1 WHERE 0))" };
        yield return new object[] { "EXISTS(SELECT 1 WHERE 0)" };
        yield return new object[] { "data->'$.name' = 'x'" };
        yield return new object[] { "data->>'$.name' = 'x'" };
    }
}
