namespace DbSqlLikeMem.Npgsql.Test.Parser;

/// <summary>
/// EN: Covers PostgreSQL parser acceptance for JSON table-valued sources.
/// PT-br: Cobre a aceitacao do parser PostgreSQL para fontes JSON tabulares.
/// </summary>
public sealed class NpgsqlJsonTableParserTests(
    ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Verifies that json_each can be parsed as a table source in PostgreSQL.
    /// PT-br: Verifica se json_each pode ser interpretada como fonte de tabela no PostgreSQL.
    /// </summary>
    /// <param name="version">EN: Npgsql dialect version under test. PT-br: Versao do dialeto Npgsql em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseJsonEachTableSource_ShouldCaptureFunction(int version)
    {
        var db = Get(version, v => new NpgsqlDbMock(v));
        var dialect = Get(version, v => new NpgsqlDialect(v));

        var parsed = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(
            "SELECT * FROM json_each('[1,2,3]')",
            db,
            dialect));

        Assert.NotNull(parsed.Table);
        Assert.NotNull(parsed.Table!.TableFunction);
        Assert.Equal("json_each", parsed.Table.TableFunction!.Name, StringComparer.OrdinalIgnoreCase);
    }
}
