using System.Linq.Expressions;
using System.Text;

namespace DbSqlLikeMem.Sqlite;

#pragma warning disable CA1305 // Specify IFormatProvider
/// <summary>
/// EN: Translates LINQ expressions into SQLite-compatible SQL statements.
/// PT-br: Traduz expressões LINQ para instruções SQL compatíveis com SQLite.
/// </summary>
public class SqliteTranslator : TranslatorBase<SqliteTranslator>
{
    /// <inheritdoc />
    protected override Type QueryableTypeDefinition => typeof(SqliteQueryable<>);

    /// <summary>
    /// EN: Translates SqlFunctions.Match into the MATCH operator.
    /// PT-br: Traduz SqlFunctions.Match para o operador MATCH.
    /// </summary>
    protected override bool TryTranslateSqlFunction(StringBuilder sb, string method, MethodCallExpression node)
    {
        if (method != nameof(SqlFunctions.Match))
            return false;

        Visit(node.Arguments[0]);
        sb.Append(" MATCH ");
        Visit(node.Arguments[1]);
        return true;
    }
}
#pragma warning restore CA1305 // Specify IFormatProvider
