namespace DbSqlLikeMem;

internal static class SqlSequenceOwnershipParserHelper
{
    internal static (SqlTableSource Table, string Column) ParseSequenceOwnershipTarget(
        this SqlQueryParserContext ctx)
    {
        var first = ctx.ExpectIdentifier();

        if (!ctx.IsSymbol("."))
            throw new InvalidOperationException("OWNED BY requires table.column or schema.table.column.");

        ctx.Consume(); // .
        var second = ctx.ExpectIdentifier();

        if (!ctx.IsSymbol("."))
        {
            var table = new SqlTableSource(
                DbName: null,
                Name: first,
                Alias: null,
                Derived: null,
                DerivedUnion: null,
                DerivedSql: null,
                Pivot: null,
                MySqlIndexHints: null);

            return (table, second);
        }

        ctx.Consume(); // .
        var column = ctx.ExpectIdentifier();

        var qualifiedTable = new SqlTableSource(
            DbName: first,
            Name: second,
            Alias: null,
            Derived: null,
            DerivedUnion: null,
            DerivedSql: null,
            Pivot: null,
            MySqlIndexHints: null);

        return (qualifiedTable, column);
    }
}
