namespace DbSqlLikeMem.Oracle;

#pragma warning disable CA1010 // Generic interface should also be implemented
/// <summary>
/// EN: Represents Oracle Data Reader Mock.
/// PT-br: Representa Oracle Data leitor simulado.
/// </summary>
public sealed class OracleDataReaderMock(
#pragma warning restore CA1010 // Generic interface should also be implemented
    IList<TableResultMock> tables
    ) : DbDataReaderMockBase(tables)
{
    /// <inheritdoc />
    public override object this[int i] => NormalizeBinaryValue(base[i]);

    /// <inheritdoc />
    public override object this[string name] => NormalizeBinaryValue(base[name]);

    /// <inheritdoc />
    public override string GetName(int ordinal) => base.GetName(ordinal);

    /// <inheritdoc />
    public override object GetValue(int ordinal) => NormalizeBinaryValue(base.GetValue(ordinal));

    /// <inheritdoc />
    public override Guid GetGuid(int ordinal) => GetGuidValue(base.GetValue(ordinal));

    /// <inheritdoc />
    public override T GetFieldValue<T>(int ordinal)
    {
        if (typeof(T) == typeof(Guid))
            return (T)(object)GetGuidValue(base.GetValue(ordinal));

        return base.GetFieldValue<T>(ordinal);
    }

    /// <inheritdoc />
    public override bool IsDBNull(int ordinal) => GetValue(ordinal) is null or DBNull;

    private static object NormalizeBinaryValue(object value)
        => value is byte[] bytes && bytes.Length == 0
            ? DBNull.Value
            : value;

    private static Guid GetGuidValue(object value)
        => value switch
        {
            Guid guid => guid,
            byte[] bytes when bytes.Length == 16 => new Guid(bytes),
            byte[] bytes when bytes.Length == 0 => throw new InvalidCastException("Column value cannot be converted to Guid."),
            DBNull => throw new InvalidCastException("Column value cannot be converted to Guid."),
            null => throw new InvalidCastException("Column value cannot be converted to Guid."),
            _ => (Guid)value
        };
}
