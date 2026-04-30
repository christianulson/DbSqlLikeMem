namespace DbSqlLikeMem;

internal static class SqlCreateSchemaHelper
{
    internal static SqlCreateSchemaQuery ParseCreateSchema(
        this SqlQueryParserContext ctx,
        bool orReplace)
    {
        if (orReplace)
            throw new InvalidOperationException("CREATE OR REPLACE is only supported for VIEW, FUNCTION, PROCEDURE and TABLE statements.");

        ctx.ExpectWord(SqlConst.SCHEMA);

        var ifNotExists = false;
        if (ctx.IsWord(SqlConst.IF))
        {
            ctx.Consume();
            ctx.ExpectWord(SqlConst.NOT);
            ctx.ExpectWord(SqlConst.EXISTS);
            ifNotExists = true;
        }

        var schemaName = ctx.ExpectIdentifier();
        ctx.EnsureStatementEnd("CREATE SCHEMA");

        return new SqlCreateSchemaQuery
        {
            IfNotExists = ifNotExists,
            Table = new SqlTableSource(
                DbName: null,
                Name: schemaName,
                Alias: null,
                Derived: null,
                DerivedUnion: null,
                DerivedSql: null,
                Pivot: null,
                MySqlIndexHints: null)
        };
    }
}
