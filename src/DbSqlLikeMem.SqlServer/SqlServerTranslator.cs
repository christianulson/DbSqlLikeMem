using System.Linq.Expressions;
using System.Reflection;
using System.Text;

namespace DbSqlLikeMem.SqlServer;

/// <summary>
/// Visitor que converte árvore de Expression em SQL básico.
/// Suporta .Where, .Select (projeção simples), .OrderBy/.ThenBy, .Skip, .Take e .Count.
/// </summary>
#pragma warning disable CA1305 // Specify IFormatProvider
public class SqlServerTranslator : ExpressionVisitor
{
    private StringBuilder _sb = new();
    private readonly List<object> _values = [];
    private string? _table;
    private string? _projection;
    private string? _whereClause;
    private string? _orderByClause;
    private int? _offset;
    private int? _limit;

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
            _sb.Append(" FETCH NEXT ").Append(_limit.Value).Append(" ROWS ONLY");

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
    /// EN: Translates method calls into SQL Server expressions.
    /// PT: Traduz chamadas de método em expressões do SQL Server.
    /// </summary>
    /// <param name="node">EN: Method call expression. PT: Expressão de chamada de método.</param>
    /// <returns>EN: Translated expression. PT: Expressão traduzida.</returns>
    protected override Expression VisitMethodCall(MethodCallExpression node)
    {
        ArgumentNullException.ThrowIfNull(node);
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
    /// EN: Translates constants into SQL Server literals.
    /// PT: Traduz constantes em literais do SQL Server.
    /// </summary>
    /// <param name="node">EN: Constant expression. PT: Expressão constante.</param>
    /// <returns>EN: Translated expression. PT: Expressão traduzida.</returns>
    protected override Expression VisitConstant(ConstantExpression node)
    {
        ArgumentNullException.ThrowIfNull(node);

        // 1) Prioridade: se for SqlServerQueryable<>, usa TableName (é o que você passou em AsQueryable<T>("users"))
        if (node.Value is not null)
        {
            var t = node.Value.GetType();
            if (t.IsGenericType && t.GetGenericTypeDefinition() == typeof(SqlServerQueryable<>))
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
    /// EN: Translates binary expressions into SQL Server syntax.
    /// PT: Traduz expressões binárias para a sintaxe do SQL Server.
    /// </summary>
    /// <param name="node">EN: Binary expression. PT: Expressão binária.</param>
    /// <returns>EN: Translated expression. PT: Expressão traduzida.</returns>
    protected override Expression VisitBinary(BinaryExpression node)
    {
        ArgumentNullException.ThrowIfNull(node);
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
    /// EN: Translates member access into SQL Server expressions.
    /// PT: Traduz acesso a membros em expressões do SQL Server.
    /// </summary>
    /// <param name="node">EN: Member expression. PT: Expressão de membro.</param>
    /// <returns>EN: Translated expression. PT: Expressão traduzida.</returns>
    protected override Expression VisitMember(MemberExpression node)
    {
        ArgumentNullException.ThrowIfNull(node);
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
