using System.Text;

namespace DbSqlLikeMem;
internal static class SqlExprPrinter
{
    public static string Print(SqlExpr e)
    {
        var sb = new StringBuilder();
        Write(e, sb);
        return sb.ToString();
    }

    private static void Write(SqlExpr e, StringBuilder sb)
    {
        switch (e)
        {
            case IdentifierExpr x:
                sb.Append(x.Name);
                break;
            case ColumnExpr c:
                sb.Append(CultureInfo.InvariantCulture, $"{c.Qualifier}.{c.Name}");
                break;
            case RawSqlExpr r:
                sb.Append(r.Sql);
                break;

            case ParameterExpr p:
                sb.Append(p.Name);
                break;

            case LiteralExpr l:
                sb.Append(l.Value is null
                    ? "NULL"
                    : l.Value is bool b
                        ? b
                            ? "TRUE"
                            : "FALSE"
                        : l.Value is string s
                            ? $"'{s}'"
                            : l.Value);
                break;

            case UnaryExpr u:
                sb.Append("NOT ");
                Wrap(u.Expr, sb);
                break;

            case BinaryExpr bi:
                Wrap(bi.Left, sb);
                sb.Append(' ').Append(OpText(bi.Op)).Append(' ');
                Wrap(bi.Right, sb);
                break;

            case InExpr i:
                Wrap(i.Left, sb);
                sb.Append(" IN (");
                sb.Append(string.Join(", ", i.Items.Select(Print)));
                sb.Append(')');
                break;

            case LikeExpr l:
                Wrap(l.Left, sb);
                sb.Append(" LIKE ");
                Wrap(l.Pattern, sb);
                break;

            case IsNullExpr n:
                Wrap(n.Expr, sb);
                sb.Append(n.Negated ? " IS NOT NULL" : " IS NULL");
                break;
            case ExistsExpr ee:
                sb.Append(CultureInfo.InvariantCulture, $"EXISTS ({ee.Subquery.Sql})");
                break;
            case JsonAccessExpr j:
                Write(j.Target, sb);
                sb.Append(j.Unquote ? " ->> " : " -> ");
                Write(j.Path, sb);
                break;
            case FunctionCallExpr f:
                sb.Append(CultureInfo.InvariantCulture, $"{f.Name}({string.Join(", ", f.Args.Select(Print))})");
                break;
            case CallExpr c:
                sb.Append(c.Name);
                sb.Append('(');
                if (c.Distinct) sb.Append("DISTINCT ");
                sb.Append(string.Join(", ", c.Args.Select(Print)));
                sb.Append(')');
                break;
            case CaseExpr c:
                sb.Append("CASE");
                if (c.BaseExpr is not null)
                {
                    sb.Append(' ');
                    Write(c.BaseExpr, sb);
                }

                foreach (var wt in c.Whens)
                {
                    sb.Append(" WHEN ");
                    Write(wt.When, sb);
                    sb.Append(" THEN ");
                    Write(wt.Then, sb);
                }

                if (c.ElseExpr is not null)
                {
                    sb.Append(" ELSE ");
                    Write(c.ElseExpr, sb);
                }

                sb.Append(" END");
                break;
            case BetweenExpr bt:
                Wrap(bt.Expr, sb);
                if (bt.Negated) sb.Append(" NOT");
                sb.Append(" BETWEEN ");
                Wrap(bt.Low, sb);
                sb.Append(" AND ");
                Wrap(bt.High, sb);
                break;

            case StarExpr:
                sb.Append('*');
                break;

            default:
                sb.Append(e.GetType().Name);
                break;
        }
    }

    private static void Wrap(SqlExpr e, StringBuilder sb)
    {
        var need = e is BinaryExpr or UnaryExpr;
        if (need) sb.Append('(');
        Write(e, sb);
        if (need) sb.Append(')');
    }

    private static string OpText(SqlBinaryOp op) => op switch
    {
        SqlBinaryOp.And => "AND",
        SqlBinaryOp.Or => "OR",
        SqlBinaryOp.Eq => "=",
        SqlBinaryOp.Neq => "!=",
        SqlBinaryOp.Greater => ">",
        SqlBinaryOp.GreaterOrEqual => ">=",
        SqlBinaryOp.Less => "<",
        SqlBinaryOp.LessOrEqual => "<=",
        SqlBinaryOp.NullSafeEq => "<=>",
        SqlBinaryOp.Regexp => "REGEXP",
        _ => op.ToString()
    };
}
