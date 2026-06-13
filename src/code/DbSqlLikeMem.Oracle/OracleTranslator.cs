using System.Linq.Expressions;
using System.Text;

namespace DbSqlLikeMem.Oracle;

#pragma warning disable CA1305 // Specify IFormatProvider
/// <summary>
/// EN: Translates LINQ expressions into Oracle-compatible SQL statements.
/// PT-br: Traduz expressões LINQ para instruções SQL compatíveis com Oracle.
/// </summary>
public class OracleTranslator : TranslatorBase<OracleTranslator>
{
    /// <inheritdoc />
    protected override Type QueryableTypeDefinition => typeof(OracleQueryable<>);

    /// <summary>
    /// EN: Appends Oracle-specific pagination using OFFSET ... ROWS FETCH NEXT/FIRST ... ROWS ONLY.
    /// PT-br: Adiciona paginacao especifica do Oracle usando OFFSET ... ROWS FETCH NEXT/FIRST ... ROWS ONLY.
    /// </summary>
    protected override void AppendPagination(StringBuilder sb, int? offset, int? limit)
    {
        if (offset.HasValue)
            sb.Append(" OFFSET ").Append(offset.Value).Append(" ROWS");
        if (limit.HasValue)
        {
            if (offset.HasValue)
                sb.Append(" FETCH NEXT ").Append(limit.Value).Append(" ROWS ONLY");
            else
                sb.Append(" FETCH FIRST ").Append(limit.Value).Append(" ROWS ONLY");
        }
    }

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
