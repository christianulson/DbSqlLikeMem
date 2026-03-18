using System.Collections.Concurrent;
using System.Linq.Expressions;

namespace DbSqlLikeMem;

/// <summary>
/// Executa consultas LINQ traduzidas para SQL sem depender de Dapper.
/// </summary>
public static class LinqQueryExecutor
{
    private static readonly ConcurrentDictionary<Type, IReadOnlyList<(string Name, Func<object, object?> Getter)>> ParameterAccessorCache = new();
    private static readonly ConcurrentDictionary<Type, IReadOnlyList<LinqRecordSetter>> RecordSetterCache = new();
    private static readonly ConcurrentDictionary<string, IReadOnlyList<LinqRecordBinding>> RecordPlanCache = new(StringComparer.Ordinal);

    /// <summary>
    /// Executa SQL e materializa o resultado no tipo esperado.
    /// </summary>
    public static TResult Execute<TResult>(IDbConnection connection, string sql, object? parameters)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(connection, nameof(connection));
        ArgumentNullExceptionCompatible.ThrowIfNull(sql, nameof(sql));

        var resultType = typeof(TResult);
        if (typeof(IEnumerable).IsAssignableFrom(resultType) && resultType != typeof(string))
        {
            var elementType = resultType.IsGenericType
                ? resultType.GetGenericArguments().First()
                : typeof(object);

            var list = ExecuteMany(connection, sql, parameters, elementType);
            return (TResult)list;
        }

        var single = ExecuteSingle(connection, sql, parameters, resultType);
        return single is null ? default! : (TResult)single;
    }

    private static object ExecuteMany(IDbConnection connection, string sql, object? parameters, Type elementType)
    {
        using var command = CreateCommand(connection, sql, parameters);
        using var reader = command.ExecuteReader();
        var metrics = (connection as DbConnectionMockBase)?.Metrics;

        var listType = typeof(List<>).MakeGenericType(elementType);
        var list = (IList)Activator.CreateInstance(listType)!;

        if (!reader.Read())
            return list;

        var planStartedAt = metrics is null ? 0 : Stopwatch.GetTimestamp();
        var plan = GetRecordPlan(reader, elementType);
        if (planStartedAt != 0)
        {
            metrics!.IncrementPerformancePhaseHit(DbPerformanceMetricKeys.MaterializationLinqPlan);
            metrics.IncrementPerformancePhaseElapsedTicks(
                DbPerformanceMetricKeys.MaterializationLinqPlan,
                StopwatchCompatible.GetElapsedTicks(planStartedAt));
        }

        do
        {
            var rowStartedAt = metrics is null ? 0 : Stopwatch.GetTimestamp();
            list.Add(MapRecord(reader, elementType, plan));
            if (rowStartedAt != 0)
            {
                metrics!.IncrementPerformancePhaseHit(DbPerformanceMetricKeys.MaterializationLinqRow);
                metrics.IncrementPerformancePhaseElapsedTicks(
                    DbPerformanceMetricKeys.MaterializationLinqRow,
                    StopwatchCompatible.GetElapsedTicks(rowStartedAt));
            }
        }
        while (reader.Read());

        return list;
    }

    private static object? ExecuteSingle(IDbConnection connection, string sql, object? parameters, Type resultType)
    {
        using var command = CreateCommand(connection, sql, parameters);
        using var reader = command.ExecuteReader();
        var metrics = (connection as DbConnectionMockBase)?.Metrics;

        if (!reader.Read())
            return resultType.IsValueType ? Activator.CreateInstance(resultType) : null;

        var planStartedAt = metrics is null ? 0 : Stopwatch.GetTimestamp();
        var plan = GetRecordPlan(reader, resultType);
        if (planStartedAt != 0)
        {
            metrics!.IncrementPerformancePhaseHit(DbPerformanceMetricKeys.MaterializationLinqPlan);
            metrics.IncrementPerformancePhaseElapsedTicks(
                DbPerformanceMetricKeys.MaterializationLinqPlan,
                StopwatchCompatible.GetElapsedTicks(planStartedAt));
        }

        var rowStartedAt = metrics is null ? 0 : Stopwatch.GetTimestamp();
        var mapped = MapRecord(reader, resultType, plan);
        if (rowStartedAt != 0)
        {
            metrics!.IncrementPerformancePhaseHit(DbPerformanceMetricKeys.MaterializationLinqRow);
            metrics.IncrementPerformancePhaseElapsedTicks(
                DbPerformanceMetricKeys.MaterializationLinqRow,
                StopwatchCompatible.GetElapsedTicks(rowStartedAt));
        }

        return mapped;
    }

    private static IDbCommand CreateCommand(IDbConnection connection, string sql, object? parameters)
    {
        var command = connection.CreateCommand();
        command.CommandType = CommandType.Text;
        command.CommandText = sql;
        AddParameters(command, parameters);
        return command;
    }

    private static void AddParameters(IDbCommand command, object? parameters)
    {
        if (parameters is null)
            return;

        if (parameters is IEnumerable<KeyValuePair<string, object?>> kvps)
        {
            foreach (var kvp in kvps)
                AddParameter(command, kvp.Key, kvp.Value);

            return;
        }

        foreach (var accessor in GetParameterAccessors(parameters.GetType()))
        {
            AddParameter(command, accessor.Name, accessor.Getter(parameters));
        }
    }

    private static void AddParameter(IDbCommand command, string name, object? value)
    {
        var parameter = command.CreateParameter();
        parameter.ParameterName = name;
        parameter.Value = value ?? DBNull.Value;
        command.Parameters.Add(parameter);
    }

    private static object? MapRecord(IDataRecord record, Type targetType, IReadOnlyList<LinqRecordBinding>? plan = null)
    {
        if (targetType == typeof(object))
            return ReadValue(record, 0, typeof(object));

        if (IsSimpleType(targetType))
            return ReadValue(record, 0, targetType);

        var instance = Activator.CreateInstance(targetType)
            ?? throw new InvalidOperationException($"Não foi possível instanciar o tipo {targetType}.");

        foreach (var binding in plan ?? GetRecordPlan(record, targetType))
            binding.Setter.Setter(instance, ReadValue(record, binding.Ordinal, binding.Setter.PropertyType));

        return instance;
    }

    private static IReadOnlyList<(string Name, Func<object, object?> Getter)> GetParameterAccessors(Type parameterType)
        => ParameterAccessorCache.GetOrAdd(parameterType, static type =>
            [.. type.GetProperties(BindingFlags.Instance | BindingFlags.Public)
                .Where(static property => property.CanRead)
                .Select(property => (property.Name, BuildParameterGetter(type, property)))]);

    private static Func<object, object?> BuildParameterGetter(Type sourceType, PropertyInfo property)
    {
        var source = Expression.Parameter(typeof(object), "source");
        var castSource = Expression.Convert(source, sourceType);
        var propertyAccess = Expression.Property(castSource, property);
        var boxedValue = Expression.Convert(propertyAccess, typeof(object));
        return Expression.Lambda<Func<object, object?>>(boxedValue, source).Compile();
    }

    private static IReadOnlyList<LinqRecordSetter> GetRecordSetters(Type targetType)
        => RecordSetterCache.GetOrAdd(targetType, static type =>
            [.. type.GetProperties(BindingFlags.Instance | BindingFlags.Public)
                .Where(static property => property.CanWrite)
                .Select(property => new LinqRecordSetter(
                    property.Name,
                    property.PropertyType,
                    BuildRecordSetter(type, property)))]);

    private static Action<object, object?> BuildRecordSetter(Type targetType, PropertyInfo property)
    {
        var instance = Expression.Parameter(typeof(object), "instance");
        var value = Expression.Parameter(typeof(object), "value");
        var castInstance = Expression.Convert(instance, targetType);
        var castValue = Expression.Convert(value, property.PropertyType);
        var propertyAccess = Expression.Property(castInstance, property);
        var assign = Expression.Assign(propertyAccess, castValue);
        return Expression.Lambda<Action<object, object?>>(assign, instance, value).Compile();
    }

    private static IReadOnlyList<LinqRecordBinding> GetRecordPlan(IDataRecord record, Type targetType)
    {
        if (targetType == typeof(object) || IsSimpleType(targetType))
            return [];

        var cacheKey = BuildRecordPlanCacheKey(record, targetType);
        return RecordPlanCache.GetOrAdd(cacheKey, _ => BuildRecordPlan(record, targetType));
    }

    private static string BuildRecordPlanCacheKey(IDataRecord record, Type targetType)
    {
        var sb = new StringBuilder(targetType.FullName?.Length ?? targetType.Name.Length + 32);
        sb.Append(targetType.AssemblyQualifiedName ?? targetType.FullName ?? targetType.Name);
        sb.Append('|').Append(record.FieldCount);
        for (var i = 0; i < record.FieldCount; i++)
            sb.Append('|').Append(record.GetName(i));

        return sb.ToString();
    }

    private static IReadOnlyList<LinqRecordBinding> BuildRecordPlan(IDataRecord record, Type targetType)
    {
        var columns = new Dictionary<string, int>(record.FieldCount, StringComparer.OrdinalIgnoreCase);
        for (var i = 0; i < record.FieldCount; i++)
            columns[record.GetName(i)] = i;

        var bindings = new List<LinqRecordBinding>();
        foreach (var setter in GetRecordSetters(targetType))
        {
            if (!columns.TryGetValue(setter.Name, out var ordinal))
                continue;

            bindings.Add(new LinqRecordBinding(ordinal, setter));
        }

        return bindings;
    }

    private static object? ReadValue(IDataRecord record, int ordinal, Type destinationType)
    {
        if (record.IsDBNull(ordinal))
            return null;

        var rawValue = record.GetValue(ordinal);

        var underlying = Nullable.GetUnderlyingType(destinationType) ?? destinationType;
        if (underlying.IsInstanceOfType(rawValue))
            return rawValue;

        if (underlying.IsEnum)
            return rawValue is string s
                ? Enum.Parse(underlying, s, ignoreCase: true)
                : Enum.ToObject(underlying, rawValue);

        if (underlying == typeof(Guid))
            return rawValue is Guid g ? g : Guid.Parse(rawValue.ToString()!);

        return Convert.ChangeType(rawValue, underlying, CultureInfo.InvariantCulture);
    }

    private static bool IsSimpleType(Type type)
    {
        var underlying = Nullable.GetUnderlyingType(type) ?? type;
        return underlying.IsPrimitive
            || underlying.IsEnum
            || underlying == typeof(string)
            || underlying == typeof(decimal)
            || underlying == typeof(DateTime)
            || underlying == typeof(DateTimeOffset)
            || underlying == typeof(TimeSpan)
            || underlying == typeof(Guid);
    }

    private sealed record LinqRecordSetter(string Name, Type PropertyType, Action<object, object?> Setter);
    private sealed record LinqRecordBinding(int Ordinal, LinqRecordSetter Setter);
}
