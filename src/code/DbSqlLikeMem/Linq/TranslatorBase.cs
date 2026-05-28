using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;

namespace DbSqlLikeMem;

#pragma warning disable CA1305 // Specify IFormatProvider
/// <summary>
/// EN: Base class for LINQ-to-SQL expression translators shared across all supported providers.
/// PT-br: Classe base para tradutores de expressao LINQ para SQL compartilhada por todos os provedores suportados.
/// </summary>
public abstract class TranslatorBase<TProvider> : ExpressionVisitor
    where TProvider : TranslatorBase<TProvider>
{
    private const int TranslationCacheSoftLimit = 256;
    private static readonly ConcurrentDictionary<string, string> TranslationCache = new(StringComparer.Ordinal);
    private StringBuilder _sb = new();
    private readonly List<object> _values = [];
    private string? _table;
    private string? _projection;
    private string? _whereClause;
    private string? _orderByClause;
    private int? _offset;
    private int? _limit;

    /// <summary>
    /// EN: Gets the provider-specific Queryable type definition (e.g., typeof(SqlServerQueryable&lt;&gt;)).
    /// PT-br: Obtem a definicao do tipo Queryable especifico do provedor.
    /// </summary>
    protected abstract Type QueryableTypeDefinition { get; }

    /// <summary>
    /// EN: Translates a LINQ expression into SQL and parameters.
    /// PT-br: Traduz uma expressao LINQ em SQL e parametros.
    /// </summary>
    public TranslationResult Translate(Expression expression)
    {
        var cacheKey = BuildTranslationCacheKey(expression);
        if (TranslationCache.TryGetValue(cacheKey, out var cachedSql))
            return new TranslationResult(cachedSql, BuildParameters(CollectParameters(expression)));

        _sb.Clear();
        _values.Clear();
        _table = null;
        _projection = null;
        _whereClause = null;
        _orderByClause = null;
        _offset = null;
        _limit = null;

        Visit(expression);

        BuildSelectSql();

        var sql = _sb.ToString();
        CacheTranslation(cacheKey, sql);
        return new TranslationResult(sql, BuildParameters(_values));
    }

    private void BuildSelectSql()
    {
        _sb.Append("SELECT ");
        _sb.Append(string.IsNullOrWhiteSpace(_projection) ? "*" : _projection);
        _sb.Append(" FROM ").Append(_table);
        if (!string.IsNullOrWhiteSpace(_whereClause))
            _sb.Append(" WHERE ").Append(_whereClause);
        if (!string.IsNullOrWhiteSpace(_orderByClause))
            _sb.Append(" ORDER BY ").Append(_orderByClause);
        AppendPagination(_sb, _offset, _limit);
    }

    /// <summary>
    /// EN: Appends the provider-specific pagination clause (OFFSET/LIMIT, FETCH NEXT, etc.).
    /// PT-br: Adiciona a clausula de paginacao especifica do provedor.
    /// </summary>
    protected virtual void AppendPagination(StringBuilder sb, int? offset, int? limit)
    {
        if (offset.HasValue)
            sb.Append(" OFFSET ").Append(offset.Value);
        if (limit.HasValue)
            sb.Append(" LIMIT ").Append(limit.Value);
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

    private string BuildTranslationCacheKey(Expression expression)
    {
        var tableName = TryExtractTableName(expression) ?? string.Empty;
        var expressionText = expression.ToString();
        return string.Concat(tableName, "|", expressionText);
    }

    private static void CacheTranslation(string cacheKey, string sql)
    {
        if (TranslationCache.Count >= TranslationCacheSoftLimit)
            TranslationCache.Clear();

        TranslationCache[cacheKey] = sql;
    }

    private List<object> CollectParameters(Expression expression)
    {
        var collector = new TranslationParameterCollector(QueryableTypeDefinition);
        collector.Visit(expression);
        return collector.Values;
    }

#pragma warning disable CS8605 // Unboxing a possibly null value.
    /// <summary>
    /// EN: Dispatches LINQ method calls into SQL clauses (Where, OrderBy, Skip, Take, Count, Select) and SqlFunctions.
    /// PT-br: Dispacha chamadas de metodo LINQ para clausulas SQL (Where, OrderBy, Skip, Take, Count, Select) e SqlFunctions.
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
            Visit(node.Arguments[0]);
            _projection = "COUNT(*)";
            return node;
        }
        if (method == "Select")
        {
            Visit(node.Arguments[0]);

            var lambda = (LambdaExpression)StripQuotes(node.Arguments[1]);
            _projection = BuildProjection(lambda);
            return node;
        }

        // SqlFunctions dispatch
        if (node.Method.DeclaringType == typeof(SqlFunctions))
        {
            if (TryTranslateSqlFunction(_sb, method, node))
                return node;
            return base.VisitMethodCall(node);
        }

        return base.VisitMethodCall(node);
    }
#pragma warning restore CS8605 // Unboxing a possibly null value.

    /// <summary>
    /// EN: Tries to translate a SqlFunctions method call into provider-specific SQL.
    /// PT-br: Tenta traduzir uma chamada de metodo SqlFunctions para SQL especifico do provedor.
    /// </summary>
    /// <param name="sb">EN: StringBuilder to append SQL to. PT-br: StringBuilder para anexar o SQL.</param>
    /// <param name="method">EN: Name of the SqlFunctions method. PT-br: Nome do metodo SqlFunctions.</param>
    /// <param name="node">EN: Method call expression. PT-br: Expressao de chamada de metodo.</param>
    /// <returns>EN: True if the method was handled; otherwise false. PT-br: True se o metodo foi tratado; caso contrario false.</returns>
    protected virtual bool TryTranslateSqlFunction(StringBuilder sb, string method, MethodCallExpression node) => false;

    /// <summary>
    /// EN: Visits a constant expression to extract table name or parameter values.
    /// PT-br: Visita uma expressao constante para extrair nome da tabela ou valores de parametro.
    /// </summary>
    protected override Expression VisitConstant(ConstantExpression node)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(node, nameof(node));

        if (node.Value is not null)
        {
            var t = node.Value.GetType();
            if (t.IsGenericType && t.GetGenericTypeDefinition() == QueryableTypeDefinition)
            {
                var prop = t.GetProperty("TableName", BindingFlags.Public | BindingFlags.Instance);
                if (prop?.GetValue(node.Value) is string tn && !string.IsNullOrWhiteSpace(tn))
                {
                    _table = tn;
                    return node;
                }
            }
        }

        if (node.Value is IQueryable q)
        {
            _table = q.ElementType.Name;
            return node;
        }

        if (node.Value != null)
        {
            var idx = _values.Count;
            _values.Add(node.Value);
            _sb.Append($"@p{idx}");
        }

        return node;
    }

    /// <summary>
    /// EN: Visits a binary expression and emits the corresponding SQL operator.
    /// PT-br: Visita uma expressao binaria e emite o operador SQL correspondente.
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
            case ExpressionType.AndAlso: _sb.Append(SqlConst.AND_SPACED); break;
            case ExpressionType.OrElse: _sb.Append(" OR "); break;
            default: _sb.Append(' '); break;
        }
        Visit(node.Right);
        _sb.Append(')');
        return node;
    }

    /// <summary>
    /// EN: Visits a member expression and emits the column name or a parameter reference.
    /// PT-br: Visita uma expressao de membro e emite o nome da coluna ou uma referencia de parametro.
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
            return mex.Member.Name;
        }

        return "*";
    }

    private string? TryExtractTableName(Expression expression)
    {
        switch (expression)
        {
            case ConstantExpression constant:
                {
                    if (constant.Value is null)
                        return null;

                    var valueType = constant.Value.GetType();
                    if (valueType.IsGenericType && valueType.GetGenericTypeDefinition() == QueryableTypeDefinition)
                    {
                        var property = valueType.GetProperty("TableName", BindingFlags.Instance | BindingFlags.Public);
                        if (property?.GetValue(constant.Value) is string tableName && !string.IsNullOrWhiteSpace(tableName))
                            return tableName;
                    }

                    if (constant.Value is IQueryable queryable)
                        return queryable.ElementType.Name;

                    return null;
                }

            case MethodCallExpression methodCall when methodCall.Arguments.Count > 0:
                return TryExtractTableName(methodCall.Arguments[0]);

            default:
                return null;
        }
    }

    private sealed class TranslationParameterCollector : ExpressionVisitor
    {
        private readonly Type _queryableTypeDefinition;

        internal List<object> Values { get; } = [];

        internal TranslationParameterCollector(Type queryableTypeDefinition)
        {
            _queryableTypeDefinition = queryableTypeDefinition;
        }

        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            ArgumentNullExceptionCompatible.ThrowIfNull(node, nameof(node));

            if (node.Method.Name is "Where" or "OrderBy" or "OrderByDescending" or "ThenBy" or "ThenByDescending" or "Select")
            {
                Visit(node.Arguments[0]);
                Visit(StripQuotes(node.Arguments[1]));
                return node;
            }

            if (node.Method.Name is "Skip" or "Take")
            {
                Visit(node.Arguments[0]);
                Visit(node.Arguments[1]);
                return node;
            }

            if (node.Method.Name == "Count")
            {
                Visit(node.Arguments[0]);
                return node;
            }

            if (node.Method.DeclaringType == typeof(SqlFunctions))
            {
                foreach (var arg in node.Arguments)
                    Visit(arg);
                return node;
            }

            return base.VisitMethodCall(node);
        }

        protected override Expression VisitConstant(ConstantExpression node)
        {
            ArgumentNullExceptionCompatible.ThrowIfNull(node, nameof(node));

            if (node.Value is null)
                return node;

            var valueType = node.Value.GetType();
            if (valueType.IsGenericType && valueType.GetGenericTypeDefinition() == _queryableTypeDefinition)
                return node;

            if (node.Value is IQueryable)
                return node;

            Values.Add(node.Value);
            return node;
        }

        protected override Expression VisitMember(MemberExpression node)
        {
            ArgumentNullExceptionCompatible.ThrowIfNull(node, nameof(node));

            if (node.Expression is not null && node.Expression.NodeType == ExpressionType.Parameter)
                return node;

            var value = Expression.Lambda(node).Compile().DynamicInvoke();
            if (value is not null)
                Values.Add(value);

            return node;
        }
    }
}
#pragma warning restore CA1305 // Specify IFormatProvider
