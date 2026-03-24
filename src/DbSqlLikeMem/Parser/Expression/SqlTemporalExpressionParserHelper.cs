namespace DbSqlLikeMem;

internal static class SqlTemporalExpressionParserHelper
{
    internal static void EnsureTemporalIdentifierDoesNotAllowParentheses(
        this SqlExpressionParserContext ctx,
        string identifier,
        string message,
        SqlToken contextToken)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(ctx, nameof(ctx));
        EnsureTemporalIdentifierDoesNotAllowParentheses(
            ctx.Dialect,
            identifier,
            message,
            ctx.Error,
            contextToken);
    }

    internal static void EnsureTemporalIdentifierDoesNotAllowParentheses(
        ISqlDialect dialect,
        string identifier,
        string message,
        Func<string, SqlToken, InvalidOperationException> error,
        SqlToken contextToken)
    {
        if (!dialect.AllowsTemporalIdentifier(identifier) || dialect.AllowsTemporalCall(identifier))
            return;

        throw error(message, contextToken);
    }

    internal static void EnsureTemporalCallIdentifierRequiresParentheses(
        this SqlExpressionParserContext ctx,
        string identifier,
        string message,
        SqlToken contextToken)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(ctx, nameof(ctx));
        EnsureTemporalCallIdentifierRequiresParentheses(
            ctx.Dialect,
            identifier,
            message,
            ctx.Error,
            contextToken);
    }

    internal static void EnsureTemporalCallIdentifierRequiresParentheses(
        ISqlDialect dialect,
        string identifier,
        string message,
        Func<string, SqlToken, InvalidOperationException> error,
        SqlToken contextToken)
    {
        if (dialect.AllowsTemporalIdentifier(identifier))
            return;

        if (!dialect.AllowsTemporalCall(identifier))
            return;

        throw error(message, contextToken);
    }
}
