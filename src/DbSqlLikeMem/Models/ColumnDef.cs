namespace DbSqlLikeMem;

public sealed class ColumnDef
{
    public ColumnDef()
    { }

    public ColumnDef(
        int index,
        DbType dbType,
        bool nullable,
        bool identity,
        object? defaultValue
    ) : this(index, dbType, nullable, identity)
    {
        DefaultValue = defaultValue;
    }

    public ColumnDef(
        int index,
        DbType dbType,
        bool nullable,
        bool identity
    ) : this(index, dbType, nullable)
    {
        Identity = identity;
    }

    public ColumnDef(
        int index,
        DbType dbType
    ) : this(index, dbType, false) 
    { }

    public ColumnDef(
        int index,
        DbType dbType,
        bool nullable)
    {
        Index = index;
        DbType = dbType;
        Nullable = nullable;
    }

    public int Index { get; init; }
    public DbType DbType { get; init; }
    public bool Nullable { get; init; }
    public bool Identity { get; set; }
    public object? DefaultValue { get; set; }

    public HashSet<string> EnumValues { get; internal set; } = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
    public void SetEnumValues(params string[] values) => EnumValues = new(values, StringComparer.OrdinalIgnoreCase);

    public Func<Dictionary<int, object?>, ITableMock, object?>? GetGenValue { get; set; }
}