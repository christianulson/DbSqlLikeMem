namespace DbSqlLikeMem;

/// <summary>
/// EN: String normalization helpers used internally by the SQL engine.
/// PT-br: Helpers de normalizacao de string usados internamente pelo motor SQL.
/// </summary>
// Note: the file name retains the legacy "Extencions" spelling to avoid disrupting build artefacts.
// The class is intentionally renamed to the correct "Extensions" spelling.
internal static class SqlStringExtensions
{
    /// <summary>
    /// EN: Collapses whitespace outside quoted strings while preserving escaped single quotes.
    /// PT-br: Reduz espacos em branco fora de strings entre aspas e preserva aspas simples escapadas.
    /// </summary>
    public static string NormalizeString(this string str)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(str, nameof(str));

        if (str.Length == 0)
            return string.Empty;

        var sb = new StringBuilder(str.Length);
        var pendingSpace = false;
        var inSingleQuotedString = false;

        for (var i = 0; i < str.Length; i++)
        {
            var ch = str[i];

            if (inSingleQuotedString)
            {
                sb.Append(ch);
                if (ch == '\'')
                {
                    if (i + 1 < str.Length && str[i + 1] == '\'')
                    {
                        sb.Append(str[i + 1]);
                        i++;
                    }
                    else
                    {
                        inSingleQuotedString = false;
                    }
                }

                continue;
            }

            if (ch == '\'')
            {
                if (pendingSpace)
                {
                    sb.Append(' ');
                    pendingSpace = false;
                }

                sb.Append(ch);
                inSingleQuotedString = true;
                continue;
            }

            var isWhitespace = ch is ' ' or '\n' or '\r' or '\t';

            if (isWhitespace)
            {
                if (sb.Length > 0)
                    pendingSpace = true;

                continue;
            }

            if (pendingSpace)
            {
                sb.Append(' ');
                pendingSpace = false;
            }

            sb.Append(ch);
        }

        return sb.ToString();
    }

    /// <summary>
    /// EN: Trims and normalizes a dotted identifier by removing wrapper characters from each segment.
    /// PT-br: Remove espacos e normaliza um identificador pontuado removendo os caracteres de envoltorio de cada segmento.
    /// </summary>
    public static string NormalizeName(this string name)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(name, nameof(name));

        var trimmed = name.Trim();
        if (trimmed.Length == 0)
            return string.Empty;

        if (!trimmed.Contains('.'))
            return StripIdentifierWrappers(trimmed);

        var parts = trimmed.Split('.').Select(_ => _.Trim()).Where(_ => !string.IsNullOrEmpty(_)).ToArray();
        for (var i = 0; i < parts.Length; i++)
            parts[i] = StripIdentifierWrappers(parts[i]);

        return string.Join(".", parts);
    }

    private static string StripIdentifierWrappers(string identifier)
    {
        var normalized = identifier.Trim();

        while (normalized.Length >= 2)
        {
            var first = normalized[0];
            var last = normalized[^1];
            var hasWrapperPair =
                (first == '`' && last == '`') ||
                (first == '"' && last == '"') ||
                (first == '[' && last == ']');

            if (!hasWrapperPair)
                break;

            normalized = normalized[1..^1].Trim();
        }

        return normalized;
    }
}
