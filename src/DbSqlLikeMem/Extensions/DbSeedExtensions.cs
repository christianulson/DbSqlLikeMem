using System.Collections.Concurrent;
using System.Linq.Expressions;

namespace DbSqlLikeMem;

/// <summary>
/// EN: Defines the class DbSeedExtensions.
/// PT: Define a classe DbSeedExtensions.
/// </summary>
public static class DbSeedExtensions
{
    private static readonly ConcurrentDictionary<Type, IReadOnlyList<(string Name, Func<object, object?> Getter)>> _seedFieldAccessorCache = new();

    /// <summary>
    /// EN: Implements Define.
    /// PT: Implementa Define.
    /// </summary>
    public static DbConnectionMockBase Define(
        this DbConnectionMockBase cnn,
        string tableName,
        IEnumerable<Col>? columns = null,
        IEnumerable<Dictionary<int, object?>>? rows = null,
        string? schemaName = null)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(cnn, nameof(cnn));
        if (!cnn.Db.ContainsTable(tableName, schemaName))
            cnn.Db.AddTable(tableName, columns, rows);

        return cnn;
    }

    /// <summary>
    /// EN: Implements DefineTable.
    /// PT: Implementa DefineTable.
    /// </summary>
    public static ITableMock DefineTable(
        this DbConnectionMockBase cnn,
        string tableName,
        IEnumerable<Col>? columns = null,
        IEnumerable<Dictionary<int, object?>>? rows = null,
        string? schemaName = null)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(cnn, nameof(cnn));
        if (!cnn.Db.TryGetTable(tableName, out var tb, schemaName))
            tb = cnn.Db.AddTable(tableName, columns, rows);
        ArgumentNullExceptionCompatible.ThrowIfNull(tb, nameof(tb));
        return tb!;
    }

    /// <summary>
    /// EN: Configures the next identity value and optional explicit identity insertion for a table.
    /// PT: Configura o próximo valor de identidade e a inserção explícita opcional de identity para uma tabela.
    /// </summary>
    /// <param name="cnn">EN: Target connection. PT: Conexão alvo.</param>
    /// <param name="tableName">EN: Table name. PT: Nome da tabela.</param>
    /// <param name="nextIdentity">EN: Next identity value. PT: Próximo valor de identidade.</param>
    /// <param name="allowInsertOverride">EN: Enables explicit identity values when true. PT: Habilita valores explícitos de identity quando true.</param>
    /// <param name="schemaName">EN: Target schema. PT: Schema alvo.</param>
    /// <returns>EN: Same connection instance. PT: Mesma instância da conexão.</returns>
    public static DbConnectionMockBase IdentityOf(
        this DbConnectionMockBase cnn,
        string tableName,
        int nextIdentity,
        bool allowInsertOverride = false,
        string? schemaName = null)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(cnn, nameof(cnn));
        cnn.GetTable(tableName, schemaName).IdentityOf(nextIdentity, allowInsertOverride);
        return cnn;
    }

    /// <summary>
    /// EN: Configures the next identity value and optional explicit identity insertion for a table.
    /// PT: Configura o próximo valor de identidade e a inserção explícita opcional de identity para uma tabela.
    /// </summary>
    /// <param name="tb">EN: Target table. PT: Tabela alvo.</param>
    /// <param name="nextIdentity">EN: Next identity value. PT: Próximo valor de identidade.</param>
    /// <param name="allowInsertOverride">EN: Enables explicit identity values when true. PT: Habilita valores explícitos de identity quando true.</param>
    /// <returns>EN: Same table instance. PT: Mesma instância da tabela.</returns>
    public static ITableMock IdentityOf(
        this ITableMock tb,
        int nextIdentity,
        bool allowInsertOverride = false)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(tb, nameof(tb));
        tb.NextIdentity = nextIdentity;
        tb.AllowIdentityInsert = allowInsertOverride;
        return tb;
    }

    /// <summary>
    /// EN: Implements this member.
    /// PT: Implementa este membro.
    /// </summary>
    public static DbConnectionMockBase Column<T>(
        this DbConnectionMockBase cnn,
        string tableName,
        string column,
        bool pk = false,
        bool identity = false,
        bool nullable = false,
        object? defaultValue = null,
        int? size = null,
        int? decimalPlaces = null,
        string? schemaName = null)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(cnn, nameof(cnn));

        if (!cnn.Db.TryGetTable(tableName, out var tb, schemaName))
            throw new InvalidOperationException(SqlExceptionMessages.TableNotYetDefined(tableName));
        ArgumentNullExceptionCompatible.ThrowIfNull(tb, nameof(tb));
        ArgumentNullExceptionCompatible.ThrowIfNull(column, nameof(column));

        if (tb!.Columns.ContainsKey(column))
            throw new InvalidOperationException(SqlExceptionMessages.ColumnAlreadyExistsInTable(column, tableName));

        var dbType = MapTypeToDbType(typeof(T));

        tb.AddColumn(
            name: column,
            dbType: dbType,
            nullable: nullable,
            decimalPlaces: decimalPlaces,
            size: size,
            identity: identity,
            defaultValue: defaultValue
        );

        return cnn;
    }

    /// <summary>
    /// EN: Implements this member.
    /// PT: Implementa este membro.
    /// </summary>
    public static ITableMock Column<T>(
        this ITableMock tb,
        string column,
        bool pk = false,
        bool identity = false,
        bool nullable = false,
        object? defaultValue = null,
        string? references = null,
        int? size = null,
        int? decimalPlaces = null,
        params string[] enumOrSetValues)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(tb, nameof(tb));
        ArgumentNullExceptionCompatible.ThrowIfNull(column, nameof(column));

        if (tb.Columns.ContainsKey(column))
            throw new InvalidOperationException(SqlExceptionMessages.ColumnAlreadyExistsInTable(column, tb.TableName));

        var dbType = MapTypeToDbType(typeof(T));

        tb.AddColumn(
            name: column,
            dbType: dbType,
            nullable: nullable,
            decimalPlaces: decimalPlaces,
            size: size,
            identity: identity,
            defaultValue: defaultValue,
            enumValues: enumOrSetValues
        );

        return tb;
    }

    /// <summary>
    /// EN: Implements this member.
    /// PT: Implementa este membro.
    /// </summary>
    public static DbConnectionMockBase Seed<T>(
        this DbConnectionMockBase cnn,
        string tableName,
        string? schemaName = null,
        params T[] rows)
        where T : struct
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(cnn, nameof(cnn));
        ArgumentNullExceptionCompatible.ThrowIfNull(rows, nameof(rows));
        if (!cnn.Db.TryGetTable(tableName, out var tb, schemaName) || tb == null)
            throw new InvalidOperationException(SqlExceptionMessages.TableNotYetDefined(tableName));
        var fields = GetSeedFieldAccessors(typeof(T));
        var fieldIndexes = fields
            .Select(field => (Index: tb.GetColumn(field.Name).Index, field.Getter))
            .ToArray();
        var materializedRows = new List<Dictionary<int, object?>>(rows.Length);
        foreach (var r in rows)
        {
            var dic = new Dictionary<int, object?>();
            foreach (var field in fieldIndexes)
                dic[field.Index] = field.Getter(r);
            materializedRows.Add(dic);
        }
        tb.AddRange(materializedRows);
        return cnn;
    }

    /// <summary>
    /// EN: Implements Seed.
    /// PT: Implementa Seed.
    /// </summary>
    public static DbConnectionMockBase Seed(
        this DbConnectionMockBase cnn,
        string tableName,
        string? schemaName = null,
        params object?[][] rows)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(cnn, nameof(cnn));
        ArgumentNullExceptionCompatible.ThrowIfNull(rows, nameof(rows));
        if (!cnn.Db.TryGetTable(tableName, out var tb, schemaName) || tb == null)
            throw new InvalidOperationException(SqlExceptionMessages.TableNotYetDefined(tableName));
        var orderedCols = tb.Columns.Values.OrderBy(c => c.Index).ToList();
        var materializedRows = new List<Dictionary<int, object?>>(rows.Length);
        foreach (var arr in rows)
        {
            var dic = new Dictionary<int, object?>();
            if (arr.Length > orderedCols.Count)
                throw new InvalidOperationException(
                    SqlExceptionMessages.SeedRowHasMoreValuesThanColumns(arr.Length, tableName, orderedCols.Count));
            for (int i = 0; i < arr.Length; i++)
                dic[orderedCols[i].Index] = arr[i];
            materializedRows.Add(dic);
        }
        tb.AddRange(materializedRows);
        return cnn;
    }

    private static DbType MapTypeToDbType(Type t) => t switch
    {
        var _ when t == typeof(int) => DbType.Int32,
        var _ when t == typeof(long) => DbType.Int64,
        var _ when t == typeof(string) => DbType.String,
        var _ when t == typeof(bool) => DbType.Boolean,
        var _ when t == typeof(Guid) => DbType.Guid,
        var _ when t == typeof(DateTime) => DbType.DateTime,
        var _ when t == typeof(decimal) => DbType.Decimal,
        var _ when t == typeof(double) => DbType.Double,
        _ => DbType.Object
    };

    private static IReadOnlyList<(string Name, Func<object, object?> Getter)> GetSeedFieldAccessors(Type rowType)
        => _seedFieldAccessorCache.GetOrAdd(rowType, BuildSeedFieldAccessors);

    private static IReadOnlyList<(string Name, Func<object, object?> Getter)> BuildSeedFieldAccessors(Type rowType)
        => [.. rowType
            .GetFields(BindingFlags.Instance | BindingFlags.Public)
            .Select(field => (field.Name, BuildSeedFieldAccessor(rowType, field)))];

    private static Func<object, object?> BuildSeedFieldAccessor(Type rowType, FieldInfo field)
    {
        var instance = Expression.Parameter(typeof(object), "instance");
        var typedInstance = Expression.Convert(instance, rowType);
        var fieldAccess = Expression.Field(typedInstance, field);
        var boxedValue = Expression.Convert(fieldAccess, typeof(object));
        return Expression.Lambda<Func<object, object?>>(boxedValue, instance).Compile();
    }

    // --------------------------- ÍNDICE -------------------------------
    /// <summary>
    /// EN: Implements Index.
    /// PT: Implementa Index.
    /// </summary>
    public static ITableMock Index(this ITableMock tb,
        string name,
        string[] keyCols,
        string[]? include = null)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(tb, nameof(tb));
        tb.CreateIndex(name, keyCols, include ?? []);
        return tb;
    }
}
