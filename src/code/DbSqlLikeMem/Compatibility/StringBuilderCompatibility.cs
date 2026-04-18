namespace DbSqlLikeMem;

internal static class StringBuilderCompatibility
{
    internal static StringBuilder AppendSpan(
        this StringBuilder builder,
        ReadOnlySpan<char> value)
    {
#if NET8_0_OR_GREATER
        return builder.Append(value);
#else
        return builder.Append(value.ToString());
#endif
    }
}
