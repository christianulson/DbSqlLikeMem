namespace DbSqlLikeMem;

internal static class AstQueryFirebirdIdentifierEvaluator
{
    internal static bool TryResolveIdentifierCore(
        QueryExecutionContext context,
        IdentifierExpr identifier,
        DbConnectionMockBase connection,
        out object? result)
        => AstQueryFirebirdIdentifierEvaluator.TryResolveIdentifier(context, identifier, connection, out result);

    internal static bool TryResolveIdentifier(
        this QueryExecutionContext context,
        IdentifierExpr identifier,
        DbConnectionMockBase connection,
        out object? result)
    {
        _ = context;

        if (!string.Equals(connection.ProviderExecutionDialect.Name, "firebird", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (identifier.Name.Equals("CURRENT_USER", StringComparison.OrdinalIgnoreCase)
            || identifier.Name.Equals("USER", StringComparison.OrdinalIgnoreCase))
        {
            result = connection.FirebirdCurrentUser;
            return true;
        }

        if (identifier.Name.Equals("CURRENT_ROLE", StringComparison.OrdinalIgnoreCase))
        {
            result = connection.FirebirdCurrentRole;
            return true;
        }

        if (identifier.Name.Equals("CURRENT_DATABASE", StringComparison.OrdinalIgnoreCase))
        {
            result = connection.Database;
            return true;
        }

        if (identifier.Name.Equals("CURRENT_CATALOG", StringComparison.OrdinalIgnoreCase))
        {
            result = connection.Database;
            return true;
        }

        if (identifier.Name.Equals("CURRENT_CONNECTION", StringComparison.OrdinalIgnoreCase))
        {
            result = unchecked((int)(uint)System.Runtime.CompilerServices.RuntimeHelpers.GetHashCode(connection));
            return true;
        }

        if (identifier.Name.Equals("SESSION_ID", StringComparison.OrdinalIgnoreCase))
        {
            result = unchecked((int)(uint)System.Runtime.CompilerServices.RuntimeHelpers.GetHashCode(connection));
            return true;
        }

        if (identifier.Name.Equals("CURRENT_TRANSACTION", StringComparison.OrdinalIgnoreCase))
        {
            result = connection.GetCurrentTransactionId();
            return true;
        }

        if (identifier.Name.Equals("TRANSACTION_ID", StringComparison.OrdinalIgnoreCase))
        {
            result = connection.GetCurrentTransactionId();
            return true;
        }

        if (identifier.Name.Equals("SQLSTATE", StringComparison.OrdinalIgnoreCase))
        {
            result = "00000";
            return true;
        }

        if (identifier.Name.Equals("SQLCODE", StringComparison.OrdinalIgnoreCase))
        {
            result = 0;
            return true;
        }

        if (identifier.Name.Equals("GDSCODE", StringComparison.OrdinalIgnoreCase))
        {
            result = 0;
            return true;
        }

        if (identifier.Name.Equals("INSERTING", StringComparison.OrdinalIgnoreCase))
        {
            result = DbConnectionMockBase.CurrentTriggerEvent == TableTriggerEvent.BeforeInsert
                || DbConnectionMockBase.CurrentTriggerEvent == TableTriggerEvent.AfterInsert;
            return true;
        }

        if (identifier.Name.Equals("UPDATING", StringComparison.OrdinalIgnoreCase))
        {
            result = DbConnectionMockBase.CurrentTriggerEvent == TableTriggerEvent.BeforeUpdate
                || DbConnectionMockBase.CurrentTriggerEvent == TableTriggerEvent.AfterUpdate;
            return true;
        }

        if (identifier.Name.Equals("DELETING", StringComparison.OrdinalIgnoreCase))
        {
            result = DbConnectionMockBase.CurrentTriggerEvent == TableTriggerEvent.BeforeDelete
                || DbConnectionMockBase.CurrentTriggerEvent == TableTriggerEvent.AfterDelete;
            return true;
        }

        if (identifier.Name.Equals("RESETTING", StringComparison.OrdinalIgnoreCase))
        {
            result = false;
            return true;
        }

        if (identifier.Name.Equals("NOW", StringComparison.OrdinalIgnoreCase))
        {
            result = context.EvaluationLocalNow;
            return true;
        }

        if (identifier.Name.Equals("TODAY", StringComparison.OrdinalIgnoreCase))
        {
            result = context.EvaluationLocalNow.Date;
            return true;
        }

        if (identifier.Name.Equals("TOMORROW", StringComparison.OrdinalIgnoreCase))
        {
            result = context.EvaluationLocalNow.Date.AddDays(1);
            return true;
        }

        if (identifier.Name.Equals("YESTERDAY", StringComparison.OrdinalIgnoreCase))
        {
            result = context.EvaluationLocalNow.Date.AddDays(-1);
            return true;
        }

        result = null;
        return false;
    }
}
