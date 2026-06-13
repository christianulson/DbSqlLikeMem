using System.Linq.Expressions;
using System.Text;

namespace DbSqlLikeMem.MySql;

#pragma warning disable CA1305 // Specify IFormatProvider
/// <summary>
/// EN: Translates LINQ expressions into MySQL-compatible SQL statements.
/// PT-br: Traduz expressões LINQ para instruções SQL compatíveis com MySQL.
/// </summary>
public class MySqlTranslator : TranslatorBase<MySqlTranslator>
{
    /// <inheritdoc />
    protected override Type QueryableTypeDefinition => typeof(MySqlQueryable<>);

    /// <summary>
    /// EN: Translates SqlFunctions.MatchAgainst into MATCH(...) AGAINST(...).
    /// PT-br: Traduz SqlFunctions.MatchAgainst para MATCH(...) AGAINST(...).
    /// </summary>
    protected override bool TryTranslateSqlFunction(StringBuilder sb, string method, MethodCallExpression node)
    {
        if (method != nameof(SqlFunctions.MatchAgainst))
            return false;

        sb.Append("MATCH(");
        Visit(node.Arguments[0]);
        sb.Append(") AGAINST(");
        Visit(node.Arguments[1]);
        sb.Append(')');
        return true;
    }
}
#pragma warning restore CA1305 // Specify IFormatProvider
