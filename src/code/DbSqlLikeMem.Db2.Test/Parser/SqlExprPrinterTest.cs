namespace DbSqlLikeMem.Db2.Test.Parser;

/// <summary>
/// EN: Covers round-trip SQL expression printing in the Db2 parser.
/// PT-br: Cobre o round-trip de impressao de expressoes SQL no parser Db2.
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
    [MemberDataByDb2Version(nameof(Expressions))]
    public void ExprPrinter_ShouldAllow_Roundtrip_Parse_Print_Parse(string expr, int version)
    {
        var d = Get(version, v => new Db2Dialect(v));
        var db = new Db2DbMock();
        var ast1 = SqlExpressionParser.ParseWhere(expr, db, d);
        var printed = SqlExprPrinter.Print(ast1);

        var ast2 = SqlExpressionParser.ParseWhere(printed, db, d);

        // não compara árvore (chato), compara “print normalizado”
        SqlExprPrinter.Print(ast1).Should().Be(SqlExprPrinter.Print(ast2));
    }

    /// <summary>
    /// EN: Provides sample expressions for the round-trip parser test.
    /// PT-br: Fornece expressoes de exemplo para o teste de round-trip do parser.
    /// </summary>
    public static IEnumerable<object[]> Expressions()
    {
        yield return new object[] { "a = 1 AND b = 2 OR c = 3" };
        yield return new object[] { "NOT (a = 1)" };
        yield return new object[] { "a IN (1,2,3)" };
        yield return new object[] { "a IN ((SELECT 1 WHERE 0))" };
        yield return new object[] { "EXISTS(SELECT 1 WHERE 0)" };
        yield return new object[] { "name LIKE 'Jo#_%' ESCAPE '#'" };
    }
}
