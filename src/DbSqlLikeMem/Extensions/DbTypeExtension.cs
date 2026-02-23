namespace DbSqlLikeMem;
/// <summary>
/// EN: Defines the class DbTypeExtension.
/// PT: Define a classe DbTypeExtension.
/// </summary>
public static class DbTypeExtension
{
    /// <summary>
    /// EN: Implements ConvertDbTypeToType.
    /// PT: Implementa ConvertDbTypeToType.
    /// </summary>
    public static Type ConvertDbTypeToType(
        this DbType dbType)
        => dbType switch
        {
            DbType.AnsiString
            or DbType.String
            or DbType.StringFixedLength
            or DbType.AnsiStringFixedLength => typeof(string),
            DbType.Binary => typeof(byte[]),
            DbType.Boolean => typeof(bool),
            DbType.Byte => typeof(byte),
            DbType.Currency or DbType.Decimal => typeof(decimal),
            DbType.Date or DbType.DateTime
            or DbType.DateTime2
            or DbType.DateTimeOffset => typeof(DateTime),
            DbType.Double => typeof(double),
            DbType.Guid => typeof(Guid),
            DbType.Int16 => typeof(short),
            DbType.Int32 => typeof(int),
            DbType.Int64 => typeof(long),
            DbType.Single => typeof(float),
            DbType.Time => typeof(TimeSpan),
            DbType.VarNumeric or DbType.Object => typeof(object),
            _ => throw new ArgumentException("Unsupported DbType", nameof(dbType)),
        };

    /// <summary>
    /// EN: Implements ConvertTypeToDbType.
    /// PT: Implementa ConvertTypeToDbType.
    /// </summary>
    public static DbType ConvertTypeToDbType(
        this Type type)
        => type switch
        {
            _ when type == typeof(string) => DbType.String,
            _ when type == typeof(byte[]) => DbType.Binary,
            _ when type == typeof(bool) => DbType.Boolean,
            _ when type == typeof(byte) => DbType.Byte,
            _ when type == typeof(decimal) => DbType.Decimal,
            _ when type == typeof(DateTime) => DbType.DateTime,
            _ when type == typeof(double) => DbType.Double,
            _ when type == typeof(Guid) => DbType.Guid,
            _ when type == typeof(short) => DbType.Int16,
            _ when type == typeof(int) => DbType.Int32,
            _ when type == typeof(long) => DbType.Int64,
            _ when type == typeof(float) => DbType.Single,
            _ when type == typeof(TimeSpan) => DbType.Time,
            _ when type == typeof(object) => DbType.Object,
            _ => throw new ArgumentException("Unsupported Type", nameof(type)),
        };
}
