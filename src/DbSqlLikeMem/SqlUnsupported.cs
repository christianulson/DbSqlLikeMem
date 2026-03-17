namespace DbSqlLikeMem;

internal static class SqlUnsupported
{
    private static string FormatDialectLabel(ISqlDialect dialect)
        => string.Equals(dialect.Name, "postgresql", StringComparison.OrdinalIgnoreCase)
            ? $"{dialect.Name}/npgsql"
            : dialect.Name;

    public static NotSupportedException ForDialect(ISqlDialect dialect, string feature)
        => new($"SQL não suportado para dialeto '{FormatDialectLabel(dialect)}' (v{dialect.Version}): {feature}.");

    public static NotSupportedException ForParser(string feature)
        => new($"SQL não suportado no parser: {feature}.");

    public static NotSupportedException ForWithRecursive(ISqlDialect dialect)
        => new($"SQL não suportado para dialeto '{FormatDialectLabel(dialect)}' (v{dialect.Version}): WITH RECURSIVE. Use WITH sem RECURSIVE quando possível, ou selecione uma versão/dialeto que suporte recursão.");


    public static NotSupportedException ForMerge(ISqlDialect dialect)
    {
        var hint = dialect.Name.ToLowerInvariant() switch
        {
            "mysql" => "Use INSERT ... ON DUPLICATE KEY UPDATE.",
            "sqlite" => "Use INSERT ... ON CONFLICT.",
            "postgresql" => "Use INSERT ... ON CONFLICT (ou MERGE em versões suportadas).",
            _ => "Selecione uma versão/dialeto com suporte a MERGE."
        };

        return new NotSupportedException($"SQL não suportado para dialeto '{FormatDialectLabel(dialect)}' (v{dialect.Version}): MERGE statement. {hint}");
    }


    public static NotSupportedException ForPagination(ISqlDialect dialect, string feature)
    {
        var hint = feature.ToUpperInvariant() switch
        {
            "LIMIT" when string.Equals(dialect.Name, "sqlserver", StringComparison.OrdinalIgnoreCase)
                => "Use OFFSET ... FETCH com ORDER BY.",
            "FETCH FIRST/NEXT" when string.Equals(dialect.Name, "mysql", StringComparison.OrdinalIgnoreCase)
                => "Use LIMIT [offset,] count.",
            "OFFSET/FETCH" when string.Equals(dialect.Name, "sqlite", StringComparison.OrdinalIgnoreCase)
                => "Use LIMIT ... OFFSET.",
            _ => "Use a sintaxe de paginação suportada pelo dialeto/versão atual."
        };

        return new NotSupportedException($"SQL não suportado para dialeto '{FormatDialectLabel(dialect)}' (v{dialect.Version}): {feature}. {hint}");
    }




    public static Exception ForOnConflictClause(ISqlDialect dialect)
    {
        var hint = dialect.Name.ToLowerInvariant() switch
        {
            "mysql" => "Use ON DUPLICATE KEY UPDATE.",
            "sqlserver" or "oracle" or "db2" => "Use MERGE (quando suportado pela versão do dialeto).",
            _ => "Use uma sintaxe de UPSERT suportada pelo dialeto atual."
        };

        var message = $"Dialeto '{FormatDialectLabel(dialect)}' não suporta ON CONFLICT. {hint}";
        return string.Equals(dialect.Name, "sqlserver", StringComparison.OrdinalIgnoreCase)
            ? new NotSupportedException(message)
            : new InvalidOperationException(message);
    }

    public static Exception ForOnDuplicateKeyUpdateClause(ISqlDialect dialect)
    {
        var hint = dialect.Name.ToLowerInvariant() switch
        {
            "postgresql" or "sqlite" => "Use ON CONFLICT.",
            "sqlserver" or "oracle" or "db2" => "Use MERGE (quando suportado pela versão do dialeto).",
            _ => "Use uma sintaxe de UPSERT suportada pelo dialeto atual."
        };

        var message = $"Dialeto '{FormatDialectLabel(dialect)}' não suporta ON DUPLICATE KEY UPDATE. {hint}";
        return string.Equals(dialect.Name, "sqlserver", StringComparison.OrdinalIgnoreCase)
            ? new NotSupportedException(message)
            : new InvalidOperationException(message);
    }

    public static NotSupportedException ForOptionQueryHints(ISqlDialect dialect)
    {
        var hint = string.Equals(dialect.Name, "sqlserver", StringComparison.OrdinalIgnoreCase)
            ? "OPTION(query hints) é suportado neste dialeto."
            : "Use hints compatíveis com o dialeto (ex.: USE/IGNORE/FORCE INDEX no MySQL quando aplicável).";

        return new NotSupportedException($"SQL não suportado para dialeto '{FormatDialectLabel(dialect)}' (v{dialect.Version}): OPTION(query hints). {hint}");
    }

    public static InvalidOperationException ForDeleteWithoutFrom(ISqlDialect dialect)
        => new($"DELETE sem FROM não suportado no dialeto '{FormatDialectLabel(dialect)}'. Use DELETE FROM <tabela> ...");

    public static InvalidOperationException ForDeleteTargetAliasFrom(ISqlDialect dialect)
        => new($"DELETE <alvo> FROM ... não suportado no dialeto '{FormatDialectLabel(dialect)}'. Use DELETE FROM <tabela> ...");

    public static InvalidOperationException ForOffsetFetchRequiresOrderBy(ISqlDialect dialect)
        => new($"OFFSET/FETCH requer ORDER BY no dialeto '{FormatDialectLabel(dialect)}'. Adicione ORDER BY para usar paginação com OFFSET/FETCH.");


    public static InvalidOperationException ForUnknownTopLevelStatement(ISqlDialect dialect, string token)
        => new($"SQL não suportado ou parser inválido para dialeto '{FormatDialectLabel(dialect)}' (v{dialect.Version}): token inicial '{token}'. Use SELECT/INSERT/UPDATE/DELETE/CREATE/ALTER/DROP/MERGE.");

    public static NotSupportedException ForCommandType(ISqlDialect dialect, string operation, Type queryType)
        => new($"SQL não suportado em {operation} para dialeto '{FormatDialectLabel(dialect)}' (v{dialect.Version}): {queryType.Name}.");

    public static InvalidOperationException ForTableDoesNotExist(string tableName)
        => new($"Table {tableName} does not exist.");

    public static InvalidOperationException ForNormalizedTableDoesNotExist(string tableName)
        => new($"Table '{tableName.NormalizeName()}' does not exist.");

    public static InvalidOperationException ForSavepointNotFound(string savepointName)
        => new($"Savepoint '{savepointName}' was not found.");

    public static InvalidOperationException ForNoActiveTransactionForSavepointOperation()
        => new("No active transaction for savepoint operation.");

    public static InvalidOperationException ForDmlProjectionRequiresValidTargetTable(string projectionClause)
        => new($"{projectionClause} requires a valid target table.");

    public static NotSupportedException ForDmlProjectionExpressionNotSupportedInExecutor(string projectionClause, string expression)
        => new($"{projectionClause} expression not supported in executor: '{expression}'.");

    public static InvalidOperationException ForProjectionClauseEmpty(string projectionClause)
        => new($"{projectionClause} clause is empty.");

    public static InvalidOperationException ForProjectionItemEmpty(string projectionClause)
        => new($"{projectionClause} item is empty.");

    public static NotSupportedException ForReturningIntoOnlySupportedInExecuteNonQuery()
        => new("RETURNING INTO is only supported for INSERT/UPDATE/DELETE in ExecuteNonQuery.");

    public static InvalidOperationException ForReturningIntoColumnParameterCountMismatch()
        => new("RETURNING INTO must map the same number of columns and parameters.");
}
