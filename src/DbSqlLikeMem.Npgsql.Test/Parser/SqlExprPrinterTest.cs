namespace DbSqlLikeMem.Npgsql.Test.Parser;
/// <summary>
/// EN: Covers round-trip SQL expression printing in the Npgsql parser.
/// PT: Cobre o round-trip de impressao de expressoes SQL no parser Npgsql.
/// </summary>
public sealed class SqlExprPrinterTest(
        ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Verifies parsed expressions can be printed and parsed again without changing the normalized output.
    /// PT: Verifica se expressoes parseadas podem ser impressas e parseadas novamente sem alterar a saida normalizada.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataByNpgsqlVersion(nameof(Expressions))]
    public void ExprPrinter_ShouldAllow_Roundtrip_Parse_Print_Parse(string expr, int version)
    {
        var d = Get(version, v => new NpgsqlDialect(v));
        var db = Get(version, v => new NpgsqlDbMock(v));
        if (version < NpgsqlDialect.JsonbMinVersion && (expr.Contains("->", StringComparison.Ordinal) || expr.Contains("#>", StringComparison.Ordinal)))
        {
            FluentActions.Invoking(() => SqlExpressionParser.ParseWhere(expr, db, d))
                .Should().Throw<NotSupportedException>();
            return;
        }

        var ast1 = SqlExpressionParser.ParseWhere(expr, db, d);
        var printed = SqlExprPrinter.Print(ast1);

        var ast2 = SqlExpressionParser.ParseWhere(printed, db, d);

        // não compara árvore (chato), compara “print normalizado”
        SqlExprPrinter.Print(ast1).Should().Be(SqlExprPrinter.Print(ast2));
    }

    /// <summary>
    /// EN: Provides sample expressions for the round-trip parser test.
    /// PT: Fornece expressoes de exemplo para o teste de round-trip do parser.
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
