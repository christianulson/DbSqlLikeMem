namespace DbSqlLikeMem;

internal static class AstQueryBinarySupportHelper
{
    internal static bool EvalSoundLike(object left, object right)
    {
        var leftSoundex = AstQuerySqlServerResolutionHelper.ComputeSoundex(left.ToString() ?? string.Empty);
        var rightSoundex = AstQuerySqlServerResolutionHelper.ComputeSoundex(right.ToString() ?? string.Empty);
        return leftSoundex == rightSoundex;
    }

    internal static bool EvalRegexp(object left, object right, ISqlDialect dialect)
    {
        try
        {
            var options = RegexOptions.CultureInvariant;
            if (dialect.RegexIsCaseInsensitive)
                options |= RegexOptions.IgnoreCase;

            return Regex.IsMatch(left.ToString() ?? string.Empty, right.ToString() ?? string.Empty, options);
        }
        catch (ArgumentException)
        {
            if (dialect.RegexInvalidPatternEvaluatesToFalse)
                return false;
            throw;
        }
    }

    internal static bool IsSqlNullLike(object? value)
        => value is null or DBNull;

    internal static bool HasNullElement(object?[] values)
    {
        for (var i = 0; i < values.Length; i++)
        {
            if (values[i] is null or DBNull)
                return true;
        }

        return false;
    }
}
