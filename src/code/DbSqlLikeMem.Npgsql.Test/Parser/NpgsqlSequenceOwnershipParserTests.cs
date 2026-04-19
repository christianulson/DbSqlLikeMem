namespace DbSqlLikeMem.Npgsql.Test.Parser;

/// <summary>
/// EN: Covers PostgreSQL sequence ownership parser cases.
/// PT: Cobre casos do parser de ownership de sequences no PostgreSQL.
/// </summary>
public sealed class NpgsqlSequenceOwnershipParserTests(
    ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Ensures PostgreSQL parses CREATE SEQUENCE OWNED BY with table ownership metadata.
    /// PT: Garante que o PostgreSQL interprete CREATE SEQUENCE OWNED BY com metadados da tabela proprietaria.
    /// </summary>
    /// <param name="version">EN: Npgsql dialect version under test. PT: Versao do dialeto Npgsql em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseCreateSequence_OwnedByTable_ShouldCaptureOwnership(int version)
    {
        var d = Get(version, v => new NpgsqlDialect(v));
        var db = Get(version, v => new NpgsqlDbMock(v));

        var parsed = Assert.IsType<SqlCreateSequenceQuery>(SqlQueryParser.Parse(
            "CREATE SEQUENCE public.seq_users START WITH 1 INCREMENT BY 1 OWNED BY public.users.id",
            db, d));

        Assert.False(parsed.IsOwnedByNone);
        Assert.NotNull(parsed.OwnedByTable);
        Assert.Equal("public", parsed.OwnedByTable!.DbName, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("users", parsed.OwnedByTable.Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("id", parsed.OwnedByColumn, StringComparer.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL parses ALTER SEQUENCE OWNED BY NONE and clears ownership metadata.
    /// PT: Garante que o PostgreSQL interprete ALTER SEQUENCE OWNED BY NONE e limpe os metadados de ownership.
    /// </summary>
    /// <param name="version">EN: Npgsql dialect version under test. PT: Versao do dialeto Npgsql em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseAlterSequence_OwnedByNone_ShouldCaptureOwnershipReset(int version)
    {
        var d = Get(version, v => new NpgsqlDialect(v));
        var db = Get(version, v => new NpgsqlDbMock(v));

        var parsed = Assert.IsType<SqlAlterSequenceQuery>(SqlQueryParser.Parse(
            "ALTER SEQUENCE public.seq_users OWNED BY NONE",
            db, d));

        Assert.True(parsed.IsOwnedByNone);
        Assert.Null(parsed.OwnedByTable);
        Assert.Null(parsed.OwnedByColumn);
    }
}
