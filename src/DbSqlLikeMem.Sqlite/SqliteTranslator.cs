using System.Linq.Expressions;
using System.Reflection;
using System.Text;

namespace DbSqlLikeMem.Sqlite;

#pragma warning disable CA1305 // Specify IFormatProvider
/// <summary>
/// EN: Translates LINQ expressions into SQLite-compatible SQL statements.
/// PT: Traduz expressões LINQ para instruções SQL compatíveis com SQLite.
/// </summary>
public class SqliteTranslator : ExpressionVisitor
{
    private StringBuilder _sb = new();
    private readonly List<object> _values = [];
    private string? _table;
    private string? _projection;
    private string? _whereClause;
    private string? _orderByClause;
    private int? _offset;
    private int? _limit;

    /// <summary>
    /// EN: Translates a LINQ expression into SQL and parameters.
    /// PT: Traduz uma expressão LINQ em SQL e parâmetros.
    /// </summary>
    public TranslationResult Translate(Expression expression)
    {
        _sb.Clear();
        _values.Clear();
        _table = null;
        _whereClause = null;
        _orderByClause = null;
        _offset = null;
        _limit = null;

        Visit(expression);

        _sb.Append("SELECT ");
        _sb.Append(string.IsNullOrWhiteSpace(_projection) ? "*" : _projection);
        _sb.Append(" FROM ").Append(_table);
        if (!string.IsNullOrWhiteSpace(_whereClause))
            _sb.Append(" WHERE ").Append(_whereClause);
        if (!string.IsNullOrWhiteSpace(_orderByClause))
            _sb.Append(" ORDER BY ").Append(_orderByClause);
        if (_offset.HasValue)
            _sb.Append(" OFFSET ").Append(_offset.Value);
        if (_limit.HasValue)
            _sb.Append(" LIMIT ").Append(_limit.Value);

        return new TranslationResult(_sb.ToString(), BuildParameters(_values));
    }

#pragma warning disable CA1859 // Use concrete types when possible for improved performance
    private static object BuildParameters(List<object> vals)
#pragma warning restore CA1859 // Use concrete types when possible for improved performance
    {
        var dict = new Dictionary<string, object>();
        for (int i = 0; i < vals.Count; i++)
            dict[$"p{i}"] = vals[i];
        return dict;
    }

#pragma warning disable CS8605 // Unboxing a possibly null value.
    /// <summary>
    /// EN: Represents Visit Method Call.
    /// PT: Representa Visit Method Call.
    /// </summary>
    protected override Expression VisitMethodCall(MethodCallExpression node)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(node, nameof(node));
        var method = node.Method.Name;
        if (method == "Where")
        {
            Visit(node.Arguments[0]);
            var lambda = (LambdaExpression)StripQuotes(node.Arguments[1]);

            var whereBuilder = new StringBuilder();
            var oldSb = _sb;
            _sb = whereBuilder;
            Visit(lambda.Body);
            _whereClause = whereBuilder.ToString();
            _sb = oldSb;
            return node;
        }
        if (method.StartsWith("OrderBy", StringComparison.Ordinal)
            || method.StartsWith("ThenBy", StringComparison.Ordinal))
        {
            Visit(node.Arguments[0]);
            var lambda = (LambdaExpression)StripQuotes(node.Arguments[1]);
            var member = (MemberExpression)lambda.Body;

            if (_orderByClause == null)
                _orderByClause = member.Member.Name;
            else
                _orderByClause += ", " + member.Member.Name;

            if (method.EndsWith("Descending", StringComparison.OrdinalIgnoreCase))
                _orderByClause += " DESC";

            return node;
        }
        if (method == "Skip")
        {
            Visit(node.Arguments[0]);
            _offset = (int)((ConstantExpression)node.Arguments[1]).Value;
            return node;
        }
        if (method == "Take")
        {
            Visit(node.Arguments[0]);
            _limit = (int)((ConstantExpression)node.Arguments[1]).Value;
            return node;
        }
        if (method == "Count")
        {
            Visit(node.Arguments[0]); // garante _table
            _projection = "COUNT(*)";
            return node;
        }
        if (method == "Select")
        {
            Visit(node.Arguments[0]); // Visita a fonte

            var lambda = (LambdaExpression)StripQuotes(node.Arguments[1]);
            _projection = BuildProjection(lambda);
            return node;
        }

        // raiz: Query<TEntity>
        return base.VisitMethodCall(node);
    }
#pragma warning restore CS8605 // Unboxing a possibly null value.

    /// <summary>
    /// EN: Represents Visit Constant.
    /// PT: Representa Visit Constant.
    /// </summary>
    protected override Expression VisitConstant(ConstantExpression node)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(node, nameof(node));

        // 1) Prioridade: se for SqliteQueryable<>, usa TableName (é o que você passou em AsQueryable<T>("users"))
        if (node.Value is not null)
        {
            var t = node.Value.GetType();
            if (t.IsGenericType && t.GetGenericTypeDefinition() == typeof(SqliteQueryable<>))
            {
                var prop = t.GetProperty("TableName", BindingFlags.Public | BindingFlags.Instance);
                if (prop?.GetValue(node.Value) is string tn && !string.IsNullOrWhiteSpace(tn))
                {
                    _table = tn;
                    return node;
                }
            }
        }

        // 2) Fallback: qualquer IQueryable que não seja o seu root custom (usa nome do tipo)
        if (node.Value is IQueryable q)
        {
            _table = q.ElementType.Name;
            return node;
        }

        // 3) Constante normal vira parâmetro
        if (node.Value != null)
        {
            var idx = _values.Count;
            _values.Add(node.Value);
            _sb.Append($"@p{idx}");
        }

        return node;
    }

    /// <summary>
    /// EN: Represents Visit Binary.
    /// PT: Representa Visit Binary.
    /// </summary>
    protected override Expression VisitBinary(BinaryExpression node)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(node, nameof(node));
        _sb.Append('(');
        Visit(node.Left);
        switch (node.NodeType)
        {
            case ExpressionType.Equal: _sb.Append(" = "); break;
            case ExpressionType.GreaterThan: _sb.Append(" > "); break;
            case ExpressionType.LessThan: _sb.Append(" < "); break;
            case ExpressionType.AndAlso: _sb.Append(" AND "); break;
            case ExpressionType.OrElse: _sb.Append(" OR "); break;
            default: _sb.Append(' '); break;
        }
        Visit(node.Right);
        _sb.Append(')');
        return node;
    }

    /// <summary>
    /// EN: Represents Visit Member.
    /// PT: Representa Visit Member.
    /// </summary>
    protected override Expression VisitMember(MemberExpression node)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(node, nameof(node));
        if (node.Expression != null
            && node.Expression.NodeType == ExpressionType.Parameter)
        {
            _sb.Append(node.Member.Name);
            return node;
        }
        // acesso a constante (captured variable)
        var value = Expression.Lambda(node).Compile().DynamicInvoke();
        var idx = _values.Count;
        if (value != null)
        {
            _values.Add(value);
            _sb.Append($"@p{idx}");
        }
        return node;
    }

    private static Expression StripQuotes(Expression e)
    {
        while (e.NodeType == ExpressionType.Quote)
            e = ((UnaryExpression)e).Operand;
        return e;
    }

    private static string BuildProjection(LambdaExpression lambda)
    {
        if (lambda.Body is NewExpression nex)
        {
            // exemplo: u => new { u.X, u.Y }
            return string.Join(", ",
                nex.Arguments.Zip(nex.Members ?? new List<MemberInfo>().AsReadOnly(), (arg, member) =>
                {
                    if (arg is MemberExpression me)
                    {
                        var name = me.Member.Name;
                        return name == member.Name ? name : $"{name} AS {member.Name}";
                    }
                    return member.Name;
                }));
        }

        if (lambda.Body is MemberExpression mex)
        {
            // exemplo: u => u.X
            return mex.Member.Name;
        }

        return "*"; // fallback
    }
}
#pragma warning restore CA1305 // Specify IFormatProvider
