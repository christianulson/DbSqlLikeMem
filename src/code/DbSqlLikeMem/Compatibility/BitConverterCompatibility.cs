namespace DbSqlLikeMem;

internal static class BitConverterCompatibility
{
    internal static int SingleToInt32Bits(float value)
        => BitConverter.ToInt32(BitConverter.GetBytes(value), 0);

    internal static long DoubleToInt64Bits(double value)
        => BitConverter.ToInt64(BitConverter.GetBytes(value), 0);
}
