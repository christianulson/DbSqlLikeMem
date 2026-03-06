using System.Collections.Concurrent;
using System.Linq.Expressions;

namespace DbSqlLikeMem;

internal static class BatchCommandMaterializer
{
#if NET6_0_OR_GREATER
    public static void Apply(DbCommand command, DbBatchCommand batchCommand, int timeout)
    {
        command.CommandText = batchCommand.CommandText;
        command.CommandType = batchCommand.CommandType;
        command.CommandTimeout = timeout;

        foreach (DbParameter parameter in batchCommand.Parameters)
            command.Parameters.Add(parameter);
    }
#endif

    public static void Apply<TBatchCommand>(DbCommand command, TBatchCommand batchCommand, int timeout)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(batchCommand, nameof(batchCommand));

#if NET6_0_OR_GREATER
        if (batchCommand is DbBatchCommand typed)
        {
            Apply(command, typed, timeout);
            return;
        }
#endif

        var accessor = AccessorCache.GetOrAdd(batchCommand!.GetType(), static t => BatchCommandAccessor.Create(t));
        command.CommandText = accessor.GetCommandText(batchCommand!);
        command.CommandType = accessor.GetCommandType(batchCommand!);
        command.CommandTimeout = timeout;

        foreach (DbParameter parameter in accessor.GetParameters(batchCommand!))
            command.Parameters.Add(parameter);
    }

    private static readonly ConcurrentDictionary<Type, BatchCommandAccessor> AccessorCache = new();

    private sealed class BatchCommandAccessor(
        Func<object, string> getCommandText,
        Func<object, CommandType> getCommandType,
        Func<object, DbParameterCollection> getParameters)
    {
        public Func<object, string> GetCommandText { get; } = getCommandText;
        public Func<object, CommandType> GetCommandType { get; } = getCommandType;
        public Func<object, DbParameterCollection> GetParameters { get; } = getParameters;

        public static BatchCommandAccessor Create(Type batchCommandType)
        {
            var textProperty = batchCommandType.GetProperty("CommandText");
            var typeProperty = batchCommandType.GetProperty("CommandType");
            var parametersProperty = batchCommandType.GetProperty("Parameters")
                ?? batchCommandType.GetProperty("DbParameterCollection", BindingFlags.Instance | BindingFlags.NonPublic);

            if (textProperty is null || typeProperty is null || parametersProperty is null)
                throw new InvalidOperationException($"Cannot materialize batch command type '{batchCommandType.FullName}'.");

            if (textProperty.PropertyType != typeof(string)
                || typeProperty.PropertyType != typeof(CommandType)
                || !typeof(DbParameterCollection).IsAssignableFrom(parametersProperty.PropertyType))
                throw new InvalidOperationException($"Batch command type '{batchCommandType.FullName}' has incompatible members.");

            return new BatchCommandAccessor(
                BuildPropertyGetter<string>(batchCommandType, textProperty),
                BuildPropertyGetter<CommandType>(batchCommandType, typeProperty),
                BuildPropertyGetter<DbParameterCollection>(batchCommandType, parametersProperty));
        }

        private static Func<object, TProperty> BuildPropertyGetter<TProperty>(Type sourceType, PropertyInfo property)
        {
            var source = Expression.Parameter(typeof(object), "source");
            var castSource = Expression.Convert(source, sourceType);
            var propertyAccess = Expression.Property(castSource, property);
            var castResult = Expression.Convert(propertyAccess, typeof(TProperty));
            return Expression.Lambda<Func<object, TProperty>>(castResult, source).Compile();
        }
    }
}
