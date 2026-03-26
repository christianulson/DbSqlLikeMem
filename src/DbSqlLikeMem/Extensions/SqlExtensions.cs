namespace DbSqlLikeMem;

internal static class SqlExtensions
{
    private static readonly SqlExtensionsDefaultDialect _defaultDialect = new();

    internal static decimal ToDec(this object? v)
    {
        if (v is null || v is DBNull) return 0m;
        return v switch
        {
            decimal d => d,
            bool b => b ? 1m : 0m,
            int i => i,
            long l => l,
            short s => s,
            sbyte sb => sb,
            byte b => b,
            ushort us => us,
            uint ui => ui,
            ulong ul => ul,
            double db => (decimal)db,
            float f => (decimal)f,
            DateTime dt => dt.Ticks,
            DateTimeOffset dto => dto.Ticks,
            TimeSpan ts => ts.Ticks,
            _ when decimal.TryParse(v.ToString(), NumberStyles.Any, CultureInfo.InvariantCulture, out var dd) => dd,
            _ => 0m
        };
    }

    internal static bool ToBool(this object? v)
    {
        if (v is null || v is DBNull) return false;
        if (v is bool b) return b;

        if (v is byte or ushort or uint or ulong)
            return Convert.ToUInt64(v, CultureInfo.InvariantCulture) != 0UL;

        if (v is sbyte or short or int or long)
            return Convert.ToInt64(v, CultureInfo.InvariantCulture) != 0;

        if (v is float or double or decimal)
            return Convert.ToDecimal(v, CultureInfo.InvariantCulture) != 0m;

        var s = v.ToString();
        if (string.IsNullOrWhiteSpace(s)) return false;
        s = s.Trim();

        if (s.Equals("yes", StringComparison.OrdinalIgnoreCase)
            || s.Equals("y", StringComparison.OrdinalIgnoreCase)
            || s.Equals("on", StringComparison.OrdinalIgnoreCase))
            return true;

        if (s.Equals("no", StringComparison.OrdinalIgnoreCase)
            || s.Equals("n", StringComparison.OrdinalIgnoreCase)
            || s.Equals("off", StringComparison.OrdinalIgnoreCase))
            return false;

        if (bool.TryParse(s, out var bb)) return bb;
        if (decimal.TryParse(s, NumberStyles.Any, CultureInfo.InvariantCulture, out var d)) return d != 0m;

        return true;
    }

    internal static bool Like(
        this string input,
        string pattern,
        QueryExecutionContext context,
        string? escape = null,
        bool? forceCaseInsensitive = null)
        => Like(input, pattern, context.Dialect, escape, forceCaseInsensitive);

    internal static bool Like(
        this string input,
        string pattern,
        ISqlDialect dialect,
        string? escape = null,
        bool? forceCaseInsensitive = null)
    {
        input ??= "";
        pattern ??= "";

        var escapeChar = ResolveLikeEscapeCharacter(dialect, escape);
        var sb = new StringBuilder();
        sb.Append('^');

        for (int i = 0; i < pattern.Length; i++)
        {
            char ch = pattern[i];

            if (escapeChar.HasValue && ch == escapeChar.Value)
            {
                if (i + 1 < pattern.Length)
                {
                    AppendRegexLiteral(sb, pattern[++i]);
                }
                else
                {
                    AppendRegexLiteral(sb, ch);
                }
                continue;
            }

            if (ch == '%') { sb.Append(".*"); continue; }
            if (ch == '_') { sb.Append('.'); continue; }

            AppendRegexLiteral(sb, ch);
        }

        sb.Append('$');

        var options = RegexOptions.CultureInvariant;
        if (forceCaseInsensitive ?? dialect.LikeIsCaseInsensitive)
            options |= RegexOptions.IgnoreCase;

        return Regex.IsMatch(input, sb.ToString(), options);
    }

    internal static int PatIndex(
        this string input,
        string pattern,
        QueryExecutionContext context,
        string? escape = null)
        => PatIndex(input, pattern, context.Dialect, escape);

    internal static int PatIndex(
        this string input,
        string pattern,
        ISqlDialect dialect,
        string? escape = null)
    {
        input ??= "";
        pattern ??= "";

        if (input.Length == 0 || pattern.Length == 0)
            return 0;

        var searchPattern = TrimLeadingLikePercents(pattern, ResolveLikeEscapeCharacter(dialect, escape));
        if (searchPattern.Length == 0)
            return 1;

        for (var i = 0; i < input.Length; i++)
        {
            if (input[i..].Like(searchPattern, dialect, escape))
                return i + 1;
        }

        return 0;
    }

    private static char? ResolveLikeEscapeCharacter(
        ISqlDialect dialect,
        string? explicitEscape)
    {
        if (explicitEscape is not null)
        {
            if (dialect.LikeEscapeExpressionMustBeSingleCharacter
                && explicitEscape.Length != 1)
                throw new InvalidOperationException("LIKE ESCAPE expression must evaluate to a single character.");

            return explicitEscape.Length == 0 ? null : explicitEscape[0];
        }

        return dialect.LikeDefaultEscapeCharacter;
    }

    private static void AppendRegexLiteral(StringBuilder sb, char ch)
    {
        if ("\\.^$|?*+()[]{}".Contains($"{ch}", StringComparison.OrdinalIgnoreCase))
            sb.Append('\\');

        sb.Append(ch);
    }

    private static string TrimLeadingLikePercents(string pattern, char? escapeChar)
    {
        var index = 0;
        while (index < pattern.Length)
        {
            var ch = pattern[index];
            if (escapeChar.HasValue && ch == escapeChar.Value)
                break;

            if (ch != '%')
                break;

            index++;
        }

        return pattern[index..];
    }

    internal static int Compare(this object a, object b, QueryExecutionContext context)
        => Compare(a, b, context.Dialect);

    internal static int Compare(this object a, object b)
        => Compare(a, b, new SqlExtensionsDefaultDialect());

    internal static int Compare(this object a, object b, ISqlDialect dialect)
    {
        if (a is byte[] ba && b is byte[] bb)
            return CompareBinary(ba, bb);

        if (a is string sa && b is string sb)
            return string.Compare(sa, sb, dialect.TextComparison);

        if (a.GetType() == b.GetType() && a is IComparable comparable)
            return comparable.CompareTo(b);

        // numeric compare if possible
        if (dialect.SupportsImplicitNumericStringComparison
            && TryConvertToDecimal(a, out var da) && TryConvertToDecimal(b, out var db))
            return da.CompareTo(db);

        if (TryConvertToDateTimeLike(a, out var dateTimeA)
            && TryConvertToDateTimeLike(b, out var dateTimeB))
            return dateTimeA.CompareTo(dateTimeB);

        return string.Compare(a.ToString(), b.ToString(), dialect.TextComparison);
    }

    internal static bool EqualsSql(this object? a, object? b,QueryExecutionContext context)
        => EqualsSql(a, b, context.Dialect);

    internal static bool EqualsSql(this object? a, object? b)
        => EqualsSql(a, b, _defaultDialect);

    internal static bool EqualsSql(this object? a, object? b, ISqlDialect dialect)
    {
        if (a is null || a is DBNull) return b is null || b is DBNull;
        if (b is null || b is DBNull) return false;

        if (a is byte[] ba && b is byte[] bb)
            return ba.AsSpan().SequenceEqual(bb);

        if (a is string sa && b is string sb)
            return string.Equals(sa, sb, dialect.TextComparison);

        if (a.GetType() == b.GetType())
            return a.Equals(b);

        if (dialect.SupportsImplicitNumericStringComparison
            && TryConvertToDecimal(a, out var da) && TryConvertToDecimal(b, out var db))
            return da == db;

        if (!dialect.SupportsImplicitNumericStringComparison)
            return false;

        if (TryConvertToDateTimeLike(a, out var dateTimeA)
            && TryConvertToDateTimeLike(b, out var dateTimeB))
            return dateTimeA == dateTimeB;

        return string.Equals(a.ToString(), b.ToString(), dialect.TextComparison);
    }

    private sealed class SqlExtensionsDefaultDialect : SqlDialectBase
    {
        public SqlExtensionsDefaultDialect()
            : base(
                name: "default",
                version: 1,
                keywords: [],
                binOps: [],
                operators: [])
        {
        }
    }

    /// <summary>
    /// EN: Attempts to coerce heterogeneous values into decimal using invariant rules for SQL-like implicit comparison.
    /// PT: Tenta converter valores heterogêneos para decimal usando regras invariáveis para comparação implícita estilo SQL.
    /// </summary>
    private static bool TryConvertToDecimal(object value, out decimal numericValue)
    {
        if (value is DateTime dt)
        {
            numericValue = dt.Ticks;
            return true;
        }

        if (value is DateTimeOffset dto)
        {
            numericValue = dto.Ticks;
            return true;
        }

        if (value is TimeSpan ts)
        {
            numericValue = ts.Ticks;
            return true;
        }

        if (value is bool boolValue)
        {
            numericValue = boolValue ? 1m : 0m;
            return true;
        }

        if (value is IConvertible)
        {
#pragma warning disable CA1031 // Do not catch general exception types
            try
            {
                numericValue = Convert.ToDecimal(value, CultureInfo.InvariantCulture);
                return true;
            }
            catch
            {
                // fallback to text parsing below
            }
#pragma warning restore CA1031 // Do not catch general exception types
        }

        return decimal.TryParse(value.ToString(), NumberStyles.Any, CultureInfo.InvariantCulture, out numericValue);
    }

    private static bool TryConvertToDateTimeLike(object value, out DateTime dateTime)
    {
        if (value is DateTime dt)
        {
            dateTime = dt;
            return true;
        }

        if (value is DateTimeOffset dto)
        {
            dateTime = dto.DateTime;
            return true;
        }

        var text = value.ToString();
        if (!string.IsNullOrWhiteSpace(text)
            && DateTime.TryParse(
                text,
                CultureInfo.InvariantCulture,
                DateTimeStyles.AllowWhiteSpaces | DateTimeStyles.AssumeLocal,
                out dateTime))
        {
            return true;
        }

        dateTime = default;
        return false;
    }

    /// <summary>
    /// EN: Compares two binary payloads lexicographically to provide deterministic ordering semantics.
    /// PT: Compara dois payloads binários de forma lexicográfica para fornecer semântica determinística de ordenação.
    /// </summary>
    private static int CompareBinary(byte[] left, byte[] right)
    {
        var min = Math.Min(left.Length, right.Length);
        for (var i = 0; i < min; i++)
        {
            var diff = left[i].CompareTo(right[i]);
            if (diff != 0)
                return diff;
        }

        return left.Length.CompareTo(right.Length);
    }
}
