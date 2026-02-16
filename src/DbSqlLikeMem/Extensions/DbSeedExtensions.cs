namespace DbSqlLikeMem;

/// <summary>
/// Auto-generated summary.
/// </summary>
public static class DbSeedExtensions
{
    /// <summary>
    /// Auto-generated summary.
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
    /// Auto-generated summary.
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
    /// Auto-generated summary.
    /// </summary>
    public static DbConnectionMockBase Column<T>(
        this DbConnectionMockBase cnn,
        string tableName,
        string column,
        bool pk = false,
        bool identity = false,
        bool nullable = false,
        object? defaultValue = null,
        string? references = null,
        int? size = null,
        int? decimalPlaces = null,
        string? schemaName = null)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(cnn, nameof(cnn));

        if (!cnn.Db.TryGetTable(tableName, out var tb, schemaName))
            throw new InvalidOperationException($"Tabela '{tableName}' ainda não definida.");
        ArgumentNullExceptionCompatible.ThrowIfNull(tb, nameof(tb));
        ArgumentNullExceptionCompatible.ThrowIfNull(column, nameof(column));

        if (tb!.Columns.ContainsKey(column))
            throw new InvalidOperationException($"Coluna '{column}' já existe em '{tableName}'.");

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

        if (references is not null)
        {
            var parts = references.Split('.');
            tb.CreateForeignKey(column, parts[0], parts[1]);
        }
        return cnn;
    }

    /// <summary>
    /// Auto-generated summary.
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
            throw new InvalidOperationException($"Coluna '{column}' já existe em '{tb}'.");

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

        if (references is not null)
        {
            var parts = references.Split('.');
            tb.CreateForeignKey(column, parts[0], parts[1]);
        }
        return tb;
    }

    /// <summary>
    /// Auto-generated summary.
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
            throw new InvalidOperationException($"Tabela '{tableName}' ainda não definida.");
        var props = typeof(T).GetFields();
        foreach (var r in rows)
        {
            var dic = new Dictionary<int, object?>();
            foreach (var p in props)
            {
                var idx = tb.GetColumn(p.Name).Index;
                dic[idx] = p.GetValue(r);
            }
            tb.Add(dic);
        }
        return cnn;
    }

    /// <summary>
    /// Auto-generated summary.
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
            throw new InvalidOperationException($"Tabela '{tableName}' ainda não definida.");
        foreach (var arr in rows)
        {
            var dic = new Dictionary<int, object?>();
            var orderedCols = tb.Columns.Values.OrderBy(c => c.Index).ToList();
            if (arr.Length > orderedCols.Count)
                throw new InvalidOperationException($"Seed row has {arr.Length} values, but table '{tableName}' has only {orderedCols.Count} columns.");
            for (int i = 0; i < arr.Length; i++)
                dic[orderedCols[i].Index] = arr[i];
            tb.Add(dic);
        }
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

    // --------------------------- ÍNDICE -------------------------------
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public static ITableMock Index(this ITableMock tb,
        string name,
        string[] keyCols,
        string[]? include = null)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(tb, nameof(tb));
        tb.CreateIndex(new IndexDef(name, keyCols, include ?? []));
        return tb;
    }
}
