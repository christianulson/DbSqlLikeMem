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
            DbType.SByte => typeof(sbyte),
            DbType.Currency or DbType.Decimal => typeof(decimal),
            DbType.Date or DbType.DateTime
            or DbType.DateTime2
            => typeof(DateTime),
            DbType.DateTimeOffset => typeof(DateTimeOffset),
            DbType.Double => typeof(double),
            DbType.Guid => typeof(Guid),
            DbType.Int16 => typeof(short),
            DbType.Int32 => typeof(int),
            DbType.Int64 => typeof(long),
            DbType.UInt16 => typeof(ushort),
            DbType.UInt32 => typeof(uint),
            DbType.UInt64 => typeof(ulong),
            DbType.Single => typeof(float),
            DbType.Time => typeof(TimeSpan),
            DbType.VarNumeric => typeof(decimal),
            DbType.Object => typeof(object),
            _ => throw new ArgumentException("Unsupported DbType", nameof(dbType)),
        };

    /// <summary>
    /// EN: Implements ConvertTypeToDbType.
    /// PT: Implementa ConvertTypeToDbType.
    /// </summary>
    public static DbType ConvertTypeToDbType(
        this Type type)
    {
        var targetType = Nullable.GetUnderlyingType(type) ?? type;
        if (targetType.IsEnum)
            targetType = Enum.GetUnderlyingType(targetType);

        return targetType switch
        {
            _ when targetType == typeof(string) => DbType.String,
            _ when targetType == typeof(byte[]) => DbType.Binary,
            _ when targetType == typeof(bool) => DbType.Boolean,
            _ when targetType == typeof(byte) => DbType.Byte,
            _ when targetType == typeof(sbyte) => DbType.SByte,
            _ when targetType == typeof(decimal) => DbType.Decimal,
            _ when targetType == typeof(DateTime) => DbType.DateTime,
            _ when targetType == typeof(DateTimeOffset) => DbType.DateTimeOffset,
            _ when targetType == typeof(double) => DbType.Double,
            _ when targetType == typeof(Guid) => DbType.Guid,
            _ when targetType == typeof(short) => DbType.Int16,
            _ when targetType == typeof(ushort) => DbType.UInt16,
            _ when targetType == typeof(int) => DbType.Int32,
            _ when targetType == typeof(uint) => DbType.UInt32,
            _ when targetType == typeof(long) => DbType.Int64,
            _ when targetType == typeof(ulong) => DbType.UInt64,
            _ when targetType == typeof(float) => DbType.Single,
            _ when targetType == typeof(TimeSpan) => DbType.Time,
            _ when targetType == typeof(object) => DbType.Object,
            _ => throw new ArgumentException("Unsupported Type", nameof(type)),
        };
    }
}
