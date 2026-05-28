using System.Linq.Expressions;
using System.Text;

namespace DbSqlLikeMem.SqlServer;

#pragma warning disable CA1305 // Specify IFormatProvider
/// <summary>
/// EN: Translates LINQ expressions into SQL Server-compatible SQL statements.
/// PT-br: Traduz expressões LINQ para instruções SQL compatíveis com SQL Server.
/// </summary>
public class SqlServerTranslator : TranslatorBase<SqlServerTranslator>
{
    /// <inheritdoc />
    protected override Type QueryableTypeDefinition => typeof(SqlServerQueryable<>);

    /// <summary>
    /// EN: Appends SQL Server-specific pagination using OFFSET/FETCH NEXT.
    /// PT-br: Adiciona paginacao especifica do SQL Server usando OFFSET/FETCH NEXT.
    /// </summary>
    protected override void AppendPagination(StringBuilder sb, int? offset, int? limit)
    {
        if (offset.HasValue)
            sb.Append(" OFFSET ").Append(offset.Value);
        if (limit.HasValue)
            sb.Append(" FETCH NEXT ").Append(limit.Value).Append(" ROWS ONLY");
    }

    /// <summary>
    /// EN: Translates SqlFunctions.Contains and SqlFunctions.FreeText into CONTAINS() and FREETEXT().
    /// PT-br: Traduz SqlFunctions.Contains e SqlFunctions.FreeText para CONTAINS() e FREETEXT().
    /// </summary>
    protected override bool TryTranslateSqlFunction(StringBuilder sb, string method, MethodCallExpression node)
    {
        switch (method)
        {
            case nameof(SqlFunctions.Contains):
                sb.Append("CONTAINS(");
                break;
            case nameof(SqlFunctions.FreeText):
                sb.Append("FREETEXT(");
                break;
            default:
                return false;
        }

        Visit(node.Arguments[0]);
        sb.Append(", ");
        Visit(node.Arguments[1]);
        sb.Append(')');
        return true;
    }
}
#pragma warning restore CA1305 // Specify IFormatProvider
