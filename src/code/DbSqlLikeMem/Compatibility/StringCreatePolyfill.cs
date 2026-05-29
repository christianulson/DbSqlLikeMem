namespace DbSqlLikeMem;

/// <summary>
/// EN: Provides a polyfill for string.Create on target frameworks that lack the API.
/// PT-br: Fornece um polyfill para string.Create em frameworks alvo que nao possuem a API.
/// </summary>
internal static class StringCreatePolyfill
{
    /// <summary>
    /// EN: Creates a string by repeating the pad text to fill the specified length.
    /// PT-br: Cria uma string repetindo o texto de preenchimento ate o comprimento especificado.
    /// </summary>
    internal static string CreateRepeated(int length, string padText)
    {
#if NET6_0_OR_GREATER
        return string.Create(length, padText, (span, pt) =>
        {
            while (span.Length > 0)
            {
                var copyLen = Math.Min(pt.Length, span.Length);
                pt.AsSpan(0, copyLen).CopyTo(span);
                span = span[copyLen..];
            }
        });
#else
        var buffer = new char[length];
        var span = new Span<char>(buffer);
        while (span.Length > 0)
        {
            var copyLen = Math.Min(padText.Length, span.Length);
            padText.AsSpan(0, copyLen).CopyTo(span);
            span = span[copyLen..];
        }
        return new string(buffer);
#endif
    }
}
