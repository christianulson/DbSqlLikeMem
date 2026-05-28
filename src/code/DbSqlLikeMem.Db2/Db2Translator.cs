using System.Linq.Expressions;
using System.Text;

namespace DbSqlLikeMem.Db2;

#pragma warning disable CA1305 // Specify IFormatProvider
/// <summary>
/// EN: Translates LINQ expressions into Db2-compatible SQL statements.
/// PT-br: Traduz expressões LINQ para instruções SQL compatíveis com Db2.
/// </summary>
public class Db2Translator : TranslatorBase<Db2Translator>
{
    /// <inheritdoc />
    protected override Type QueryableTypeDefinition => typeof(Db2Queryable<>);

    /// <summary>
    /// EN: Translates SqlFunctions.Contains into CONTAINS().
    /// PT-br: Traduz SqlFunctions.Contains para CONTAINS().
    /// </summary>
    protected override bool TryTranslateSqlFunction(StringBuilder sb, string method, MethodCallExpression node)
    {
        if (method != nameof(SqlFunctions.Contains))
            return false;

        sb.Append("CONTAINS(");
        Visit(node.Arguments[0]);
        sb.Append(", ");
        Visit(node.Arguments[1]);
        sb.Append(')');
        return true;
    }
}
#pragma warning restore CA1305 // Specify IFormatProvider
