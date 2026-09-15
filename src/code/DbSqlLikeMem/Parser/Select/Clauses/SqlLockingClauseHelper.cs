namespace DbSqlLikeMem;

/// <summary>Locking clause used in SELECT ... FOR UPDATE / FOR SHARE / LOCK IN SHARE MODE.</summary>
public enum SqlLockingClause
{
    /// <summary>
    /// None
    /// </summary>
    None,

    /// <summary>
    /// For Update
    /// </summary>
    ForUpdate,

    /// <summary>
    /// For Share
    /// </summary>
    ForShare,

    /// <summary>
    /// Lock
    /// </summary>
    LockInShareMode
}

internal static class SqlLockingClauseHelper
{
    /// <summary>
    /// Consumes a trailing locking clause (FOR UPDATE, FOR SHARE, LOCK IN SHARE MODE)
    /// with optional NOWAIT / SKIP LOCKED. Returns null when no locking clause is present.
    /// </summary>
    internal static SqlLockingClause? TryParseLockingClause(this SqlQueryParserContext ctx)
    {
        if (ctx.IsWord(SqlConst.FOR) && ctx.IsWord(1, SqlConst.UPDATE))
        {
            ctx.Consume(); // FOR
            ctx.Consume(); // UPDATE
            TryConsumeLockWaitOption(ctx);
            return SqlLockingClause.ForUpdate;
        }

        if (ctx.IsWord(SqlConst.FOR) && ctx.IsWord(1, SqlConst.SHARE))
        {
            ctx.Consume(); // FOR
            ctx.Consume(); // SHARE
            TryConsumeLockWaitOption(ctx);
            return SqlLockingClause.ForShare;
        }

        if (ctx.IsWord(SqlConst.LOCK)
            && ctx.IsWord(1, SqlConst.IN)
            && ctx.IsWord(2, SqlConst.SHARE)
            && ctx.IsWord(3, SqlConst.MODE))
        {
            ctx.Consume(); // LOCK
            ctx.Consume(); // IN
            ctx.Consume(); // SHARE
            ctx.Consume(); // MODE
            return SqlLockingClause.LockInShareMode;
        }

        return null;
    }

    private static void TryConsumeLockWaitOption(SqlQueryParserContext ctx)
    {
        if (ctx.IsWord(SqlConst.NOWAIT))
        {
            ctx.Consume();
            return;
        }

        if (ctx.IsWord(SqlConst.SKIP) && ctx.IsWord(1, SqlConst.LOCKED))
        {
            ctx.Consume(); // SKIP
            ctx.Consume(); // LOCKED
        }
    }
}