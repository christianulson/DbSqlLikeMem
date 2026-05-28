using System.Linq.Expressions;
using System.Text;

namespace DbSqlLikeMem.Npgsql;

#pragma warning disable CA1305 // Specify IFormatProvider
/// <summary>
/// EN: Translates LINQ expressions into PostgreSQL-compatible SQL statements.
/// PT-br: Traduz expressões LINQ para instruções SQL compatíveis com PostgreSQL.
/// </summary>
public class NpgsqlTranslator : TranslatorBase<NpgsqlTranslator>
{
    /// <inheritdoc />
    protected override Type QueryableTypeDefinition => typeof(NpgsqlQueryable<>);

    /// <summary>
    /// EN: Translates SqlFunctions.TsQuery into to_tsvector() @@ to_tsquery().
    /// PT-br: Traduz SqlFunctions.TsQuery para to_tsvector() @@ to_tsquery().
    /// </summary>
    protected override bool TryTranslateSqlFunction(StringBuilder sb, string method, MethodCallExpression node)
    {
        if (method != nameof(SqlFunctions.TsQuery))
            return false;

        sb.Append("to_tsvector(");
        Visit(node.Arguments[0]);
        sb.Append(") @@ to_tsquery(");
        Visit(node.Arguments[1]);
        sb.Append(')');
        return true;
    }
}
#pragma warning restore CA1305 // Specify IFormatProvider
