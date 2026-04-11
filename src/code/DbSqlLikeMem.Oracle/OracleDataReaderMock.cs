namespace DbSqlLikeMem.Oracle;

#pragma warning disable CA1010 // Generic interface should also be implemented
/// <summary>
/// EN: Represents Oracle Data Reader Mock.
/// PT: Representa Oracle Data leitor simulado.
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
    public override object GetValue(int ordinal) => NormalizeBinaryValue(base.GetValue(ordinal));

    /// <inheritdoc />
    public override bool IsDBNull(int ordinal) => GetValue(ordinal) is null or DBNull;

    private static object NormalizeBinaryValue(object value)
        => value is byte[] bytes && bytes.Length == 0
            ? DBNull.Value
            : value;
}
