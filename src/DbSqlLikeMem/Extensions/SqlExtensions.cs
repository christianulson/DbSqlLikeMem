namespace DbSqlLikeMem;

internal static class SqlExtensions
{
    internal static decimal ToDec(this object? v)
    {
        if (v is null || v is DBNull) return 0m;
        return v switch
        {
            decimal d => d,
            int i => i,
            long l => l,
            short s => s,
            byte b => b,
            double db => (decimal)db,
            float f => (decimal)f,
            DateTime dt => dt.Ticks,
            _ when decimal.TryParse(v.ToString(), NumberStyles.Any, CultureInfo.InvariantCulture, out var dd) => dd,
            _ => 0m
        };
    }

    internal static bool ToBool(this object? v)
    {
        if (v is null || v is DBNull) return false;
        if (v is bool b) return b;

        if (v is byte or short or int or long)
            return Convert.ToInt64(v, CultureInfo.InvariantCulture) != 0;

        if (v is float or double or decimal)
            return Convert.ToDecimal(v, CultureInfo.InvariantCulture) != 0m;

        var s = v.ToString();
        if (bool.TryParse(s, out var bb)) return bb;
        if (decimal.TryParse(s, out var d)) return d != 0m;

        return !string.IsNullOrWhiteSpace(s);
    }

    internal static bool Like(this string input, string pattern, ISqlDialect? dialect = null)
    {
        input ??= "";
        pattern ??= "";

        // Converte LIKE -> Regex
        // %  => .*     (qualquer sequência)
        // _  => .      (um caractere)
        // \% e \_ (escape) -> literal
        var sb = new System.Text.StringBuilder();
        sb.Append('^');

        bool escaped = false;
        for (int i = 0; i < pattern.Length; i++)
        {
            char ch = pattern[i];

            if (!escaped && ch == '\\')
            {
                escaped = true;
                continue;
            }

            if (!escaped)
            {
                if (ch == '%') { sb.Append(".*"); continue; }
                if (ch == '_') { sb.Append('.'); continue; }
            }

            // literal (escapa regex specials)
            if ("\\.^$|?*+()[]{}".Contains($"{ch}", StringComparison.OrdinalIgnoreCase))
                sb.Append('\\');

            sb.Append(ch);
            escaped = false;
        }

        sb.Append('$');

        var options = RegexOptions.CultureInvariant;
        if (dialect?.LikeIsCaseInsensitive ?? true)
            options |= RegexOptions.IgnoreCase;

        return Regex.IsMatch(input, sb.ToString(), options);
    }

    internal static int Compare(this object a, object b, ISqlDialect? dialect = null)
    {
        if (a.GetType() == b.GetType() && a is IComparable comparable)
            return comparable.CompareTo(b);

        // numeric compare if possible
        if ((dialect?.SupportsImplicitNumericStringComparison ?? true)
            && TryDecimal(a, out var da) && TryDecimal(b, out var db))
            return da.CompareTo(db);

        return string.Compare(a.ToString(), b.ToString(), dialect?.TextComparison ?? StringComparison.OrdinalIgnoreCase);

        static bool TryDecimal(object o, out decimal d)
        {
            switch (o)
            {
                case decimal dd: d = dd; return true;
                case int i: d = i; return true;
                case long l: d = l; return true;
                case double db: d = (decimal)db; return true;
                default:
                    return decimal.TryParse(o.ToString(), NumberStyles.Any, CultureInfo.InvariantCulture, out d);
            }
        }
    }

    internal static bool EqualsSql(this object? a, object? b, ISqlDialect? dialect = null)
    {
        if (a is null || a is DBNull) return b is null || b is DBNull;
        if (b is null || b is DBNull) return false;

        if (a.GetType() == b.GetType())
            return a.Equals(b);

        if ((dialect?.SupportsImplicitNumericStringComparison ?? true)
            && TryDecimal(a, out var da) && TryDecimal(b, out var db))
            return da == db;

        return string.Equals(a.ToString(), b.ToString(), dialect?.TextComparison ?? StringComparison.OrdinalIgnoreCase);

        static bool TryDecimal(object o, out decimal d)
        {
            switch (o)
            {
                case decimal dd: d = dd; return true;
                case int i: d = i; return true;
                case long l: d = l; return true;
                case double db: d = (decimal)db; return true;
                default:
                    return decimal.TryParse(o.ToString(), NumberStyles.Any, CultureInfo.InvariantCulture, out d);
            }
        }
    }
}
