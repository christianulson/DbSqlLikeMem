namespace DbSqlLikeMem;

/// <summary>
/// EN: Builds unsupported-operation exceptions with dialect-specific messages and hints.
/// PT-br: Monta excecoes de operacao sem suporte com mensagens e dicas especificas do dialeto.
/// </summary>
internal static class SqlUnsupported
{
    /// <summary>
    /// EN: Formats the dialect label used in unsupported-operation messages.
    /// PT-br: Formata o rótulo do dialeto usado nas mensagens de operacao sem suporte.
    /// </summary>
    private static string FormatDialectLabel(this ISqlDialect dialect)
        => string.Equals(dialect.Name, "postgresql", StringComparison.OrdinalIgnoreCase)
            ? $"{dialect.Name}/npgsql"
            : dialect.Name;

    /// <summary>
    /// EN: Creates an unsupported-operation exception for a dialect feature.
    /// PT-br: Cria uma excecao de operacao sem suporte para um recurso do dialeto.
    /// </summary>
    public static NotSupportedException NotSupported(this ISqlDialect dialect, string feature)
        => new($"SQL não suportado para dialeto '{FormatDialectLabel(dialect)}' (v{dialect.Version}): {feature}.");

    /// <summary>
    /// EN: Creates an unsupported-operation exception for parser-level features.
    /// PT-br: Cria uma excecao de operacao sem suporte para recursos do parser.
    /// </summary>
    public static NotSupportedException NotSupported(this SqlQueryParserContext ctx, string feature)
        => ctx.Dialect.NotSupported(feature);

    /// <summary>
    /// EN: Creates an unsupported-operation exception for parser-only features.
    /// PT-br: Cria uma excecao de operacao sem suporte para recursos apenas do parser.
    /// </summary>
    public static NotSupportedException NotSupportedParser(string feature)
        => new($"SQL não suportado no parser: {feature}.");

    /// <summary>
    /// EN: Creates an unsupported-operation exception for recursive WITH clauses.
    /// PT-br: Cria uma excecao de operacao sem suporte para clausulas WITH recursivas.
    /// </summary>
    public static NotSupportedException NotSupportedWithRecursive(this ISqlDialect dialect)
        => new($"SQL não suportado para dialeto '{FormatDialectLabel(dialect)}' (v{dialect.Version}): WITH RECURSIVE. Use WITH sem RECURSIVE quando possível, ou selecione uma versão/dialeto que suporte recursão.");


    /// <summary>
    /// EN: Creates an unsupported-operation exception for MERGE and suggests a dialect-specific alternative.
    /// PT-br: Cria uma excecao de operacao sem suporte para MERGE e sugere uma alternativa especifica do dialeto.
    /// </summary>
    public static NotSupportedException NotSupportedMerge(this ISqlDialect dialect)
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


    /// <summary>
    /// EN: Creates an unsupported-operation exception for pagination hints that are not available.
    /// PT-br: Cria uma excecao de operacao sem suporte para dicas de paginacao indisponiveis.
    /// </summary>
    public static NotSupportedException NotSupportedPagination(this ISqlDialect dialect, string feature)
    {
        var hint = feature.ToUpperInvariant() switch
        {
            SqlConst.LIMIT when string.Equals(dialect.Name, "sqlserver", StringComparison.OrdinalIgnoreCase)
                => "Use OFFSET ... FETCH com ORDER BY.",
            SqlConst.FETCH_FIRST_NEXT when string.Equals(dialect.Name, "mysql", StringComparison.OrdinalIgnoreCase)
                => "Use LIMIT [offset,] count.",
            SqlConst.OFFSET_FETCH when string.Equals(dialect.Name, "sqlite", StringComparison.OrdinalIgnoreCase)
                => "Use LIMIT ... OFFSET.",
            _ => "Use a sintaxe de paginação suportada pelo dialeto/versão atual."
        };

        return new NotSupportedException($"SQL não suportado para dialeto '{FormatDialectLabel(dialect)}' (v{dialect.Version}): {feature}. {hint}");
    }




    /// <summary>
    /// EN: Creates an unsupported-operation exception for ON CONFLICT clauses.
    /// PT-br: Cria uma excecao de operacao sem suporte para clausulas ON CONFLICT.
    /// </summary>
    public static Exception NotSupportedOnConflictClause(this ISqlDialect dialect)
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

    /// <summary>
    /// EN: Creates an unsupported-operation exception for ON DUPLICATE KEY UPDATE clauses.
    /// PT-br: Cria uma excecao de operacao sem suporte para clausulas ON DUPLICATE KEY UPDATE.
    /// </summary>
    public static Exception NotSupportedOnDuplicateKeyUpdateClause(this ISqlDialect dialect)
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

    /// <summary>
    /// EN: Creates an unsupported-operation exception for query hints.
    /// PT-br: Cria uma excecao de operacao sem suporte para query hints.
    /// </summary>
    public static NotSupportedException NotSupportedOptionQueryHints(this ISqlDialect dialect)
    {
        var hint = string.Equals(dialect.Name, "sqlserver", StringComparison.OrdinalIgnoreCase)
            ? "OPTION(query hints) é suportado neste dialeto."
            : "Use hints compatíveis com o dialeto (ex.: USE/IGNORE/FORCE INDEX no MySQL quando aplicável).";

        return new NotSupportedException($"SQL não suportado para dialeto '{FormatDialectLabel(dialect)}' (v{dialect.Version}): OPTION(query hints). {hint}");
    }

    /// <summary>
    /// EN: Creates an invalid-operation exception for DELETE statements without FROM.
    /// PT-br: Cria uma excecao de operacao invalida para instrucoes DELETE sem FROM.
    /// </summary>
    public static InvalidOperationException NotSupportedDeleteWithoutFrom(this ISqlDialect dialect)
        => new($"DELETE sem FROM não suportado no dialeto '{FormatDialectLabel(dialect)}'. Use DELETE FROM <tabela> ...");

    /// <summary>
    /// EN: Creates an invalid-operation exception for DELETE statements that target an alias directly.
    /// PT-br: Cria uma excecao de operacao invalida para instrucoes DELETE que apontam diretamente para um alias.
    /// </summary>
    public static InvalidOperationException NotSupportedDeleteTargetAliasFrom(this ISqlDialect dialect)
        => new($"DELETE <alvo> FROM ... não suportado no dialeto '{FormatDialectLabel(dialect)}'. Use DELETE FROM <tabela> ...");

    /// <summary>
    /// EN: Creates an invalid-operation exception for OFFSET/FETCH clauses without ORDER BY.
    /// PT-br: Cria uma excecao de operacao invalida para clausulas OFFSET/FETCH sem ORDER BY.
    /// </summary>
    public static InvalidOperationException NotSupportedOffsetFetchRequiresOrderBy(this ISqlDialect dialect)
        => new($"OFFSET/FETCH requer ORDER BY no dialeto '{FormatDialectLabel(dialect)}'. Adicione ORDER BY para usar paginação com OFFSET/FETCH.");


    /// <summary>
    /// EN: Creates an unsupported-operation exception for unknown top-level statements.
    /// PT-br: Cria uma excecao de operacao sem suporte para instrucoes de nivel superior desconhecidas.
    /// </summary>
    public static InvalidOperationException NotSupportedUnknownTopLevelStatement(this ISqlDialect dialect, string token)
        => new($"SQL não suportado ou parser inválido para dialeto '{FormatDialectLabel(dialect)}' (v{dialect.Version}): token inicial '{token}'. Use SELECT/INSERT/UPDATE/DELETE/CREATE/ALTER/DROP/MERGE.");

    /// <summary>
    /// EN: Creates an unsupported-operation exception for command types that are not supported by a query type.
    /// PT-br: Cria uma excecao de operacao sem suporte para tipos de comando que nao sao suportados por um tipo de consulta.
    /// </summary>
    public static NotSupportedException NotSupportedCommandType(this ISqlDialect dialect, string operation, Type queryType)
        => new($"SQL não suportado em {operation} para dialeto '{FormatDialectLabel(dialect)}' (v{dialect.Version}): {queryType.Name}.");

    /// <summary>
    /// EN: Creates an invalid-operation exception when a table name does not exist.
    /// PT-br: Cria uma excecao de operacao invalida quando um nome de tabela nao existe.
    /// </summary>
    public static InvalidOperationException ForTableDoesNotExist(string tableName)
        => new($"Table {tableName} does not exist.");

    /// <summary>
    /// EN: Creates an invalid-operation exception when a normalized table name does not exist.
    /// PT-br: Cria uma excecao de operacao invalida quando um nome de tabela normalizado nao existe.
    /// </summary>
    public static InvalidOperationException ForNormalizedTableDoesNotExist(string tableName)
        => new($"Table '{tableName.NormalizeName()}' does not exist.");

    /// <summary>
    /// EN: Creates an invalid-operation exception when a savepoint cannot be found.
    /// PT-br: Cria uma excecao de operacao invalida quando um savepoint nao pode ser encontrado.
    /// </summary>
    public static InvalidOperationException ForSavepointNotFound(string savepointName)
        => new($"Savepoint '{savepointName}' was not found.");

    /// <summary>
    /// EN: Creates an invalid-operation exception when savepoint work requires an active transaction.
    /// PT-br: Cria uma excecao de operacao invalida quando a operacao com savepoint exige uma transacao ativa.
    /// </summary>
    public static InvalidOperationException ForNoActiveTransactionForSavepointOperation()
        => new("No active transaction for savepoint operation.");

    /// <summary>
    /// EN: Creates an invalid-operation exception when a DML projection clause needs a target table.
    /// PT-br: Cria uma excecao de operacao invalida quando uma clausula de projeção DML precisa de uma tabela destino.
    /// </summary>
    public static InvalidOperationException ForDmlProjectionRequiresValidTargetTable(string projectionClause)
        => new($"{projectionClause} requires a valid target table.");

    /// <summary>
    /// EN: Creates an unsupported-operation exception when a projection expression cannot be evaluated in the executor.
    /// PT-br: Cria uma excecao de operacao sem suporte quando uma expressao de projeção nao pode ser avaliada no executor.
    /// </summary>
    public static NotSupportedException ForDmlProjectionExpressionNotSupportedInExecutor(string projectionClause, string expression)
        => new($"{projectionClause} expression not supported in executor: '{expression}'.");

    /// <summary>
    /// EN: Creates an invalid-operation exception when a projection clause is empty.
    /// PT-br: Cria uma excecao de operacao invalida quando uma clausula de projeção esta vazia.
    /// </summary>
    public static InvalidOperationException ForProjectionClauseEmpty(string projectionClause)
        => new($"{projectionClause} clause is empty.");

    /// <summary>
    /// EN: Creates an invalid-operation exception when a projection item is empty.
    /// PT-br: Cria uma excecao de operacao invalida quando um item de projeção esta vazio.
    /// </summary>
    public static InvalidOperationException ForProjectionItemEmpty(string projectionClause)
        => new($"{projectionClause} item is empty.");

    /// <summary>
    /// EN: Creates an unsupported-operation exception when RETURNING INTO is used outside ExecuteNonQuery.
    /// PT-br: Cria uma excecao de operacao sem suporte quando RETURNING INTO e usado fora de ExecuteNonQuery.
    /// </summary>
    public static NotSupportedException ForReturningIntoOnlySupportedInExecuteNonQuery()
        => new("RETURNING INTO is only supported for INSERT/UPDATE/DELETE in ExecuteNonQuery.");

    /// <summary>
    /// EN: Creates an invalid-operation exception when RETURNING INTO has a different number of columns and parameters.
    /// PT-br: Cria uma excecao de operacao invalida quando RETURNING INTO tem quantidades diferentes de colunas e parametros.
    /// </summary>
    public static InvalidOperationException ForReturningIntoColumnParameterCountMismatch()
        => new("RETURNING INTO must map the same number of columns and parameters.");
}
