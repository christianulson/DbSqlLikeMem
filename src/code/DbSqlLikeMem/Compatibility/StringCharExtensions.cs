namespace System;

#if NET462 || NETSTANDARD2_0
/// <summary>
/// EN: Provides polyfill extension methods for string operations using char overloads
/// that are only available in .NET Core 2.0+ / .NET Standard 2.1+.
/// PT-br: Fornece metodos de extensao polyfill para operacoes de string usando overloads
/// com char que so estao disponiveis em .NET Core 2.0+ / .NET Standard 2.1+.
/// </summary>
internal static class StringCharExtensions
{
    internal static bool EndsWith(this string value, char ch)
        => value.Length > 0 && value[^1] == ch;

    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
    internal static bool EndsWith(this string value, char ch, StringComparison comparisonType)
        => comparisonType == StringComparison.Ordinal
            ? value.EndsWith(ch)
            : value.EndsWith(ch.ToString(), comparisonType);

    internal static bool StartsWith(this string value, char ch)
        => value.Length > 0 && value[0] == ch;

    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
    internal static bool StartsWith(this string value, char ch, StringComparison comparisonType)
        => comparisonType == StringComparison.Ordinal
            ? value.StartsWith(ch)
            : value.StartsWith(ch.ToString(), comparisonType);

    internal static bool Contains(this string value, char ch)
        => value.IndexOf(ch) >= 0;

    internal static string[] Split(this string value, char separator, StringSplitOptions options)
        => value.Split(new[] { separator }, options);
}
#endif

/// <summary>
/// EN: Provides polyfill extension methods for ReadOnlySpan&lt;char&gt; operations using char overloads
/// that are not available in any .NET version.
/// PT-br: Fornece metodos de extensao polyfill para operacoes de ReadOnlySpan&lt;char&gt; usando overloads
/// com char que nao estao disponiveis em nenhuma versao do .NET.
/// </summary>
internal static class ReadOnlySpanCharExtensions
{
    internal static bool StartsWith(this ReadOnlySpan<char> span, char ch)
        => span.Length > 0 && span[0] == ch;

    internal static bool EndsWith(this ReadOnlySpan<char> span, char ch)
        => span.Length > 0 && span[^1] == ch;
}
