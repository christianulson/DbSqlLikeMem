namespace DbSqlLikeMem;

/// <summary>
/// Executa consultas LINQ traduzidas para SQL sem depender de Dapper.
/// </summary>
public static class LinqQueryExecutor
{
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

        var listType = typeof(List<>).MakeGenericType(elementType);
        var list = (IList)Activator.CreateInstance(listType)!;

        while (reader.Read())
            list.Add(MapRecord(reader, elementType));

        return list;
    }

    private static object? ExecuteSingle(IDbConnection connection, string sql, object? parameters, Type resultType)
    {
        using var command = CreateCommand(connection, sql, parameters);
        using var reader = command.ExecuteReader();

        if (!reader.Read())
            return resultType.IsValueType ? Activator.CreateInstance(resultType) : null;

        return MapRecord(reader, resultType);
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

        var props = parameters.GetType().GetProperties(BindingFlags.Instance | BindingFlags.Public);
        foreach (var prop in props)
        {
            if (!prop.CanRead)
                continue;

            AddParameter(command, prop.Name, prop.GetValue(parameters));
        }
    }

    private static void AddParameter(IDbCommand command, string name, object? value)
    {
        var parameter = command.CreateParameter();
        parameter.ParameterName = name;
        parameter.Value = value ?? DBNull.Value;
        command.Parameters.Add(parameter);
    }

    private static object? MapRecord(IDataRecord record, Type targetType)
    {
        if (targetType == typeof(object))
            return ReadValue(record, 0, typeof(object));

        if (IsSimpleType(targetType))
            return ReadValue(record, 0, targetType);

        var instance = Activator.CreateInstance(targetType)
            ?? throw new InvalidOperationException($"Não foi possível instanciar o tipo {targetType}.");

        var columns = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        for (var i = 0; i < record.FieldCount; i++)
            columns[record.GetName(i)] = i;

        var properties = targetType.GetProperties(BindingFlags.Instance | BindingFlags.Public)
            .Where(p => p.CanWrite);

        foreach (var property in properties)
        {
            if (!columns.TryGetValue(property.Name, out var ordinal))
                continue;

            var value = ReadValue(record, ordinal, property.PropertyType);
            property.SetValue(instance, value);
        }

        return instance;
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
}
