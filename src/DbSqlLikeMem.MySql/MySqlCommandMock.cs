namespace DbSqlLikeMem.MySql;

/// <summary>
/// EN: Mock command for MySQL connections.
/// PT: Comando simulado para conexões MySQL.
/// </summary>
public class MySqlCommandMock(
    MySqlConnectionMock? connection,
    MySqlTransactionMock? transaction = null
    ) : DbCommand, ICloneable
{
    /// <summary>
    /// Contructor
    /// </summary>
    public MySqlCommandMock()
        : this(null, null)
    {
    }

    private bool disposedValue;
    private readonly Dictionary<string, IReadOnlyList<ReturningProjectionTemplate>> _returningProjectionTemplateCache =
        new(StringComparer.Ordinal);

    /// <summary>
    /// Gets or sets the command text.
    /// Obtém ou define o texto do comando.
    /// </summary>
    [AllowNull]
    public override string CommandText { get; set; } = string.Empty;

    /// <summary>
    /// EN: Gets or sets CommandTimeout.
    /// PT: Obtém ou define CommandTimeout.
    /// </summary>
    public override int CommandTimeout { get; set; }

    /// <summary>
    /// EN: Gets or sets CommandType.
    /// PT: Obtém ou define CommandType.
    /// </summary>
    public override CommandType CommandType { get; set; } = CommandType.Text;

    /// <summary>
    /// EN: Gets or sets the associated connection.
    /// PT: Obtém ou define a conexão associada.
    /// </summary>
    public new MySqlConnectionMock? Connection
    {
        get
        {
            return connection;
        }
        set
        {
            if (connection != value)
                Transaction = null;

            connection = value;
        }
    }

    /// <summary>
    /// EN: Gets or sets the associated connection.
    /// PT: Obtém ou define a conexão associada.
    /// </summary>
    protected override DbConnection? DbConnection
    {
        get => connection;
        set => connection = value as MySqlConnectionMock;
    }

    internal List<MySqlCommandMock>? Batch { get; private set; }
    internal string? BatchableCommandText { get; private set; }

    internal void AddToBatch(MySqlCommandMock command)
    {
        if (Batch == null)
        {
            Batch = [];
        }

        Batch.Add(command);
    }

    internal string? GetCommandTextForBatching()
    {
        if (BatchableCommandText == null)
        {
            BatchableCommandText = IsInsertCommandText(CommandText)
                ? BuildInsertBatchableCommandText()
                : CommandText;
        }

        return BatchableCommandText;
    }

    private static bool IsInsertCommandText(string commandText)
        => commandText.Length >= 6
           && string.Compare(commandText[..6], "INSERT", StringComparison.OrdinalIgnoreCase) == 0;

    private string? BuildInsertBatchableCommandText()
    {
        var tokens = new SqlTokenizer(CommandText, connection!.Db.Dialect).Tokenize();
        return TryFindValuesTuple(tokens, out var valuesText, out var nextIndex)
            && !HasUnsupportedBatchSuffix(tokens, nextIndex)
            ? valuesText
            : null;
    }

    private static bool TryFindValuesTuple(
        IReadOnlyList<SqlToken> tokens,
        out string valuesText,
        out int nextIndex)
    {
        valuesText = string.Empty;
        nextIndex = 0;

        for (var i = 0; i < tokens.Count; i++)
        {
            var token = tokens[i];
            if (token.Kind == SqlTokenKind.EndOfFile)
                return false;

            if (!IsValuesKeyword(token))
                continue;

            return TryReadValuesTuple(tokens, i + 1, out valuesText, out nextIndex);
        }

        return false;
    }

    private static bool IsValuesKeyword(SqlToken token)
        => token.Kind != SqlTokenKind.Symbol
           && string.Equals(token.Text, "VALUES", StringComparison.OrdinalIgnoreCase);

    private static bool TryReadValuesTuple(
        IReadOnlyList<SqlToken> tokens,
        int startIndex,
        out string valuesText,
        out int nextIndex)
    {
        valuesText = string.Empty;
        nextIndex = startIndex;
        if (startIndex >= tokens.Count
            || tokens[startIndex].Kind == SqlTokenKind.EndOfFile
            || tokens[startIndex].Text != "(")
        {
            return false;
        }

        var sb = new StringBuilder();
        var depth = 0;
        for (var i = startIndex; i < tokens.Count; i++)
        {
            var token = tokens[i];
            if (token.Kind == SqlTokenKind.EndOfFile)
                return false;

            sb.Append(token.Text);
            if (token.Text == "(")
                depth++;
            else if (token.Text == ")")
            {
                depth--;
                if (depth == 0)
                {
                    valuesText = sb.ToString();
                    nextIndex = i + 1;
                    return true;
                }
            }
        }

        return false;
    }

    private static bool HasUnsupportedBatchSuffix(IReadOnlyList<SqlToken> tokens, int startIndex)
    {
        if (startIndex >= tokens.Count || tokens[startIndex].Kind == SqlTokenKind.EndOfFile)
            return false;

        var text = tokens[startIndex].Text;
        return text == "," || string.Equals(text, "ON", StringComparison.OrdinalIgnoreCase);
    }

    private readonly MySqlDataParameterCollectionMock collectionMock = [];

    /// <summary>
    /// EN: Gets the parameter collection for the command.
    /// PT: Obtém a coleção de parâmetros do comando.
    /// </summary>
    protected override DbParameterCollection DbParameterCollection => collectionMock;

    /// <summary>
    /// EN: Gets or sets the current transaction.
    /// PT: Obtém ou define a transação atual.
    /// </summary>
    protected override DbTransaction? DbTransaction
    {
        get => transaction!;
        set => transaction = value as MySqlTransactionMock;
    }

    /// <summary>
    /// EN: Gets or sets UpdatedRowSource.
    /// PT: Obtém ou define UpdatedRowSource.
    /// </summary>
    public override UpdateRowSource UpdatedRowSource { get; set; }
    
    /// <summary>
    /// EN: Gets or sets DesignTimeVisible.
    /// PT: Obtém ou define DesignTimeVisible.
    /// </summary>
    public override bool DesignTimeVisible { get; set; }

    /// <summary>
    /// EN: Implements Cancel.
    /// PT: Implementa Cancel.
    /// </summary>
    public override void Cancel() => DbTransaction?.Rollback();

    /// <summary>
    /// EN: Creates a new MySQL parameter.
    /// PT: Cria um novo parâmetro MySQL.
    /// </summary>
    /// <returns>EN: Parameter instance. PT: Instância do parâmetro.</returns>
    protected override DbParameter CreateDbParameter()
        => new MySqlParameter();

    /// <summary>
    /// EN: Implements ExecuteNonQuery.
    /// PT: Implementa ExecuteNonQuery.
    /// </summary>
    public override int ExecuteNonQuery()
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(connection, nameof(connection));
        connection!.ClearExecutionPlans();
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(CommandText, nameof(CommandText));

        // 1. Stored Procedure (sem parse SQL)
        if (CommandType == CommandType.StoredProcedure)
        {
            var affected = connection!.ExecuteStoredProcedure(CommandText, Parameters);
            connection.SetLastFoundRows(affected);
            return affected;
        }

        return connection.ExecuteNonQueryWithPipeline(
            CommandText,
            Parameters,
            allowMerge: false,
            unionUsesSelectMessage: false,
            tryExecuteTransactionControl: TryExecuteTransactionControlCommand,
            validateBeforeParse: ValidateBeforeParseNonQuery);
    }

    private void ValidateBeforeParseNonQuery(string sqlRaw)
    {
        if (!connection!.Db.Dialect.SupportsDeleteWithoutFrom && IsDeleteMissingFrom(sqlRaw))
            throw new InvalidOperationException(SqlExceptionMessages.InvalidDeleteExpectedFromKeyword());
    }

    private bool TryExecuteTransactionControlCommand(string sqlRaw, out int affectedRows)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(connection, nameof(connection));
        return connection!.TryExecuteStandardTransactionControl(
            sqlRaw,
            releaseSavepointAsNoOp: false,
            out affectedRows);
    }

    private static bool IsDeleteMissingFrom(string sqlRaw)
    {
        if (!sqlRaw.StartsWith("delete ", StringComparison.OrdinalIgnoreCase))
            return false;

        if (sqlRaw.StartsWith("delete from ", StringComparison.OrdinalIgnoreCase))
            return false;

        return !Regex.IsMatch(
            sqlRaw,
            @"^\s*delete\s+[^\s]+\s+from\s+",
            RegexOptions.IgnoreCase);
    }

    /// <summary>
    /// EN: Executes the command and returns a data reader.
    /// PT: Executa o comando e retorna um data leitor.
    /// </summary>
    /// <param name="behavior">EN: Command behavior. PT: Comportamento do comando.</param>
    /// <returns>EN: Data reader. PT: Data reader.</returns>
    protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(connection, nameof(connection));
        connection!.ClearExecutionPlans();
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(CommandText, nameof(CommandText));

        if (connection.TryHandleExecuteReaderPrelude(
            CommandType,
            CommandText,
            Parameters,
            static () => new MySqlDataReaderMock([[]]),
            normalizeSqlInput: false,
            out var earlyReader,
            out var statements))
        {
            return earlyReader!;
        }
        var executor = AstQueryExecutorFactory.Create(connection!.ExecutionDialect, connection, Parameters);

        // Parse múltiplo (ex: "SELECT 1; SELECT 2;" ou "BEGIN; SELECT FOUND_ROWS();")
        var tables = new List<TableResultMock>();
        var parsedStatementCount = 0;

        foreach (var statementSql in statements)
        {
            var sqlRaw = statementSql.Trim();
            if (string.IsNullOrWhiteSpace(sqlRaw))
                continue;

            if (connection.TryHandleReaderControlCommand(
                sqlRaw,
                Parameters,
                TryExecuteTransactionControlCommand,
                ref parsedStatementCount))
            {
                continue;
            }

            var q = SqlQueryParser.Parse(sqlRaw, connection.ExecutionDialect, Parameters);
            parsedStatementCount++;

            connection.DispatchParsedReaderQuery(
                q,
                Parameters,
                executor,
                tables,
                executeInsert: ExecuteInsertReturning,
                executeUpdate: ExecuteUpdateReturning,
                executeDelete: ExecuteDeleteReturning);
        }

        connection.FinalizeReaderExecution(tables, parsedStatementCount);

        return new MySqlDataReaderMock(tables);
    }

    /// <summary>
    /// EN: Executes INSERT and materializes RETURNING result rows when requested.
    /// PT: Executa INSERT e materializa linhas de resultado de RETURNING quando solicitado.
    /// </summary>
    private TableResultMock? ExecuteInsertReturning(SqlInsertQuery query)
    {
        if (!TryResolveTargetTable(query.Table, out var table) || table == null)
        {
            connection!.ExecuteInsert(query, Parameters, connection!.ExecutionDialect);
            return null;
        }

        var hadReturning = query.Returning.Count > 0;
        var beforeCount = table.Count;
        connection!.ExecuteInsert(query, Parameters, connection!.ExecutionDialect);

        if (!hadReturning)
            return null;

        var projections = BuildReturningProjection(query.Returning, query.Table!, table);
        return BuildReturningResultFromIndexes(
            projections,
            table,
            Enumerable.Range(beforeCount, Math.Max(0, table.Count - beforeCount)));
    }

    /// <summary>
    /// EN: Executes UPDATE and materializes RETURNING result rows when requested.
    /// PT: Executa UPDATE e materializa linhas de resultado de RETURNING quando solicitado.
    /// </summary>
    private TableResultMock? ExecuteUpdateReturning(SqlUpdateQuery query)
    {
        if (!TryResolveTargetTable(query.Table, out var table) || table == null)
        {
            connection!.ExecuteUpdateSmart(query, Parameters, connection!.ExecutionDialect);
            return null;
        }

        var hadReturning = query.Returning.Count > 0;
        List<int>? matchedIndexes = null;
        if (hadReturning)
            matchedIndexes = MatchRowIndexes(table, query.WhereRaw, query.RawSql);

        connection!.ExecuteUpdateSmart(query, Parameters, connection!.ExecutionDialect);

        if (!hadReturning)
            return null;

        var projections = BuildReturningProjection(query.Returning, query.Table!, table);
        return BuildReturningResultFromIndexes(projections, table, matchedIndexes!);
    }

    /// <summary>
    /// EN: Executes DELETE and materializes RETURNING result rows when requested.
    /// PT: Executa DELETE e materializa linhas de resultado de RETURNING quando solicitado.
    /// </summary>
    private TableResultMock? ExecuteDeleteReturning(SqlDeleteQuery query)
    {
        if (!TryResolveTargetTable(query.Table, out var table) || table == null)
        {
            connection!.ExecuteDeleteSmart(query, Parameters, connection!.ExecutionDialect);
            return null;
        }

        var hadReturning = query.Returning.Count > 0;
        TableResultMock? returningResult = null;
        if (hadReturning)
        {
            var matchedIndexes = MatchRowIndexes(table, query.WhereRaw, query.RawSql);
            var projections = BuildReturningProjection(query.Returning, query.Table!, table);
            returningResult = BuildReturningResultFromIndexes(projections, table, matchedIndexes);
        }

        connection!.ExecuteDeleteSmart(query, Parameters, connection!.ExecutionDialect);

        if (!hadReturning)
            return null;

        return returningResult;
    }

    /// <summary>
    /// EN: Builds a RETURNING result set from affected row indexes without cloning full source rows.
    /// PT: Monta um conjunto de resultado RETURNING a partir dos índices afetados sem clonar linhas completas.
    /// </summary>
    private static TableResultMock BuildReturningResultFromIndexes(
        IReadOnlyList<ReturningProjection> projections,
        ITableMock table,
        IEnumerable<int> rowIndexes)
    {
        var result = new TableResultMock();
        result.Columns = [.. projections
            .Select((p, i) => new TableResultColMock(
                p.TableAlias,
                p.ColumnAlias,
                p.ColumnName,
                i,
                p.DbType,
                p.IsNullable))];

        foreach (var rowIndex in rowIndexes)
        {
            if (rowIndex < 0 || rowIndex >= table.Count)
                continue;

            var row = table[rowIndex];
            var projected = new Dictionary<int, object?>();
            for (var colIndex = 0; colIndex < projections.Count; colIndex++)
                projected[colIndex] = projections[colIndex].Resolver(row);
            result.Add(projected);
        }

        return result;
    }

    /// <summary>
    /// EN: Creates projection metadata and resolvers for RETURNING items.
    /// PT: Cria metadados de projeção e resolvedores para itens de RETURNING.
    /// </summary>
    private List<ReturningProjection> BuildReturningProjection(
        IReadOnlyList<SqlSelectItem> returningItems,
        SqlTableSource tableSource,
        ITableMock table)
    {
        var templates = GetReturningProjectionTemplates(returningItems, tableSource, table);
        Dictionary<string, object?>? parameterValues = null;
        var projections = new List<ReturningProjection>();
        foreach (var template in templates)
        {
            if (template.ColumnIndex is int columnIndex)
            {
                projections.Add(new ReturningProjection(
                    TableAlias: template.TableAlias,
                    ColumnAlias: template.ColumnAlias,
                    ColumnName: template.ColumnName,
                    DbType: template.DbType,
                    IsNullable: template.IsNullable,
                    Resolver: row => row.TryGetValue(columnIndex, out var v) ? v : null));
                continue;
            }

            if (template.ParameterName is string parameterName)
            {
                parameterValues ??= BuildParameterValueLookup();
                parameterValues.TryGetValue(NormalizeParameterName(parameterName), out var value);
                var dbType = value?.GetType().ConvertTypeToDbType() ?? DbType.Object;
                projections.Add(new ReturningProjection(
                    TableAlias: template.TableAlias,
                    ColumnAlias: template.ColumnAlias,
                    ColumnName: template.ColumnName,
                    DbType: dbType,
                    IsNullable: value is null,
                    Resolver: _ => value));
                continue;
            }

            projections.Add(new ReturningProjection(
                TableAlias: template.TableAlias,
                ColumnAlias: template.ColumnAlias,
                ColumnName: template.ColumnName,
                DbType: template.DbType,
                IsNullable: template.IsNullable,
                Resolver: _ => template.LiteralValue));
        }

        return projections;
    }

    /// <summary>
    /// EN: Gets cached RETURNING projection templates for the current table shape and projection list.
    /// PT: Obtém templates cacheados de projeção RETURNING para o formato atual da tabela e da lista de projeções.
    /// </summary>
    private IReadOnlyList<ReturningProjectionTemplate> GetReturningProjectionTemplates(
        IReadOnlyList<SqlSelectItem> returningItems,
        SqlTableSource tableSource,
        ITableMock table)
    {
        var cacheKey = BuildReturningProjectionCacheKey(returningItems, tableSource, table);
        if (_returningProjectionTemplateCache.TryGetValue(cacheKey, out var cached))
            return cached;

        var templates = BuildReturningProjectionTemplates(returningItems, tableSource, table);
        _returningProjectionTemplateCache[cacheKey] = templates;
        return templates;
    }

    /// <summary>
    /// EN: Builds cached RETURNING projection templates from parsed expressions.
    /// PT: Monta templates cacheáveis de projeção RETURNING a partir das expressões parseadas.
    /// </summary>
    private IReadOnlyList<ReturningProjectionTemplate> BuildReturningProjectionTemplates(
        IReadOnlyList<SqlSelectItem> returningItems,
        SqlTableSource tableSource,
        ITableMock table)
    {
        var templates = new List<ReturningProjectionTemplate>();
        var tableAlias = tableSource.Alias ?? tableSource.Name ?? "returning";

        foreach (var item in returningItems)
        {
            var raw = item.Raw.Trim();
            if (raw == "*")
            {
                AppendAllColumnTemplates(templates, tableAlias, table);
                continue;
            }

            var expr = SqlExpressionParser.ParseScalar(raw, connection!.Db.Dialect);
            switch (expr)
            {
                case IdentifierExpr id:
                    AppendColumnTemplate(templates, tableAlias, table, item.Alias, id.Name);
                    break;
                case ColumnExpr colExpr when colExpr.Name == "*":
                    AppendAllColumnTemplates(templates, tableAlias, table);
                    break;
                case ColumnExpr colExpr:
                    AppendColumnTemplate(templates, tableAlias, table, item.Alias, colExpr.Name);
                    break;
                case LiteralExpr literalExpr:
                    var value = literalExpr.Value;
                    templates.Add(new ReturningProjectionTemplate(
                        TableAlias: tableAlias,
                        ColumnAlias: item.Alias ?? raw,
                        ColumnName: item.Alias ?? raw,
                        DbType: value?.GetType().ConvertTypeToDbType() ?? DbType.Object,
                        IsNullable: value is null,
                        ColumnIndex: null,
                        LiteralValue: value,
                        ParameterName: null));
                    break;
                case ParameterExpr parameterExpr:
                    templates.Add(new ReturningProjectionTemplate(
                        TableAlias: tableAlias,
                        ColumnAlias: item.Alias ?? parameterExpr.Name,
                        ColumnName: item.Alias ?? parameterExpr.Name,
                        DbType: DbType.Object,
                        IsNullable: true,
                        ColumnIndex: null,
                        LiteralValue: null,
                        ParameterName: parameterExpr.Name));
                    break;
                default:
                    throw SqlUnsupported.ForDmlProjectionExpressionNotSupportedInExecutor("RETURNING", raw);
            }
        }

        return templates;
    }

    /// <summary>
    /// EN: Appends cached templates for all table columns in ordinal order.
    /// PT: Adiciona templates cacheáveis para todas as colunas da tabela na ordem ordinal.
    /// </summary>
    private static void AppendAllColumnTemplates(
        ICollection<ReturningProjectionTemplate> templates,
        string tableAlias,
        ITableMock table)
    {
        foreach (var entry in table.Columns.OrderBy(kv => kv.Value.Index))
            AppendColumnTemplate(templates, tableAlias, entry.Key, entry.Value, null);
    }

    /// <summary>
    /// EN: Appends a cached template for a single column projection.
    /// PT: Adiciona um template cacheável para a projeção de uma única coluna.
    /// </summary>
    private static void AppendColumnTemplate(
        ICollection<ReturningProjectionTemplate> templates,
        string tableAlias,
        ITableMock table,
        string? alias,
        string rawColumnName)
    {
        var colName = NormalizeColumnReference(rawColumnName);
        AppendColumnTemplate(templates, tableAlias, colName, table.GetColumn(colName), alias);
    }

    /// <summary>
    /// EN: Appends a cached template for a resolved table column.
    /// PT: Adiciona um template cacheável para uma coluna da tabela já resolvida.
    /// </summary>
    private static void AppendColumnTemplate(
        ICollection<ReturningProjectionTemplate> templates,
        string tableAlias,
        string colName,
        ColumnDef col,
        string? alias)
    {
        templates.Add(new ReturningProjectionTemplate(
            TableAlias: tableAlias,
            ColumnAlias: alias ?? colName,
            ColumnName: colName,
            DbType: col.DbType,
            IsNullable: col.Nullable,
            ColumnIndex: col.Index,
            LiteralValue: null,
            ParameterName: null));
    }

    /// <summary>
    /// EN: Finds row indexes matched by simple WHERE conditions used by DML strategies.
    /// PT: Encontra índices de linhas que atendem às condições simples de WHERE usadas pelas estratégias DML.
    /// </summary>
    private List<int> MatchRowIndexes(
        ITableMock table,
        string? whereRaw,
        string rawSql)
    {
        var resolvedWhere = TableMock.ResolveWhereRaw(whereRaw, rawSql);
        var conditions = TableMock.ParseWhereSimple(resolvedWhere);
        var indexes = new List<int>();
        for (var i = 0; i < table.Count; i++)
        {
            if (TableMock.IsMatchSimple(table, Parameters, conditions, table[i]))
                indexes.Add(i);
        }

        return indexes;
    }

    /// <summary>
    /// EN: Normalizes a qualified column reference to a table-local column name.
    /// PT: Normaliza uma referência de coluna qualificada para o nome local da coluna na tabela.
    /// </summary>
    private static string NormalizeColumnReference(string rawColumnName)
    {
        var normalized = rawColumnName.Trim();
        var dot = normalized.LastIndexOf('.');
        if (dot >= 0 && dot + 1 < normalized.Length)
            normalized = normalized[(dot + 1)..];
        return normalized.NormalizeName();
    }

    /// <summary>
    /// EN: Builds a cache key for RETURNING projection templates from table identity, schema and projection text.
    /// PT: Monta uma chave de cache para templates de projeção RETURNING a partir da identidade da tabela, esquema e texto da projeção.
    /// </summary>
    private static string BuildReturningProjectionCacheKey(
        IReadOnlyList<SqlSelectItem> returningItems,
        SqlTableSource tableSource,
        ITableMock table)
    {
        var alias = tableSource.Alias ?? tableSource.Name ?? "returning";
        var schemaKey = string.Join("|", table.Columns.OrderBy(kv => kv.Value.Index).Select(kv => $"{kv.Value.Index}:{kv.Key}"));
        var projectionKey = string.Join("|", returningItems.Select(item => $"{item.Raw.Trim()}=>{item.Alias ?? string.Empty}"));
        return $"{System.Runtime.CompilerServices.RuntimeHelpers.GetHashCode(table)}::{alias}::{schemaKey}::{projectionKey}";
    }

    /// <summary>
    /// EN: Builds a normalized lookup for current parameter values used by cached RETURNING plans.
    /// PT: Monta um lookup normalizado para os valores atuais dos parâmetros usados por planos cacheados de RETURNING.
    /// </summary>
    private Dictionary<string, object?> BuildParameterValueLookup()
    {
        var lookup = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        foreach (DbParameter parameter in Parameters)
            lookup[NormalizeParameterName(parameter.ParameterName)] = parameter.Value is DBNull ? null : parameter.Value;
        return lookup;
    }

    /// <summary>
    /// EN: Normalizes a SQL parameter placeholder name for cache and lookup operations.
    /// PT: Normaliza o nome de um placeholder de parâmetro SQL para operações de cache e lookup.
    /// </summary>
    private static string NormalizeParameterName(string? rawName)
    {
        var normalized = rawName?.Trim() ?? string.Empty;
        if (normalized.Length > 0 && (normalized[0] == '@' || normalized[0] == ':' || normalized[0] == '?'))
            normalized = normalized[1..];
        return normalized;
    }

    /// <summary>
    /// EN: Tries to resolve the target table from an AST table source.
    /// PT: Tenta resolver a tabela alvo a partir de uma fonte de tabela da AST.
    /// </summary>
    private bool TryResolveTargetTable(
        SqlTableSource? tableSource,
        out ITableMock? table)
    {
        table = null!;
        if (tableSource is null || string.IsNullOrWhiteSpace(tableSource.Name))
            return false;

        return connection!.TryGetTable(tableSource.Name!, out table, tableSource.DbName) && table is not null;
    }

    private sealed record ReturningProjection(
        string TableAlias,
        string ColumnAlias,
        string ColumnName,
        DbType DbType,
        bool IsNullable,
        Func<IReadOnlyDictionary<int, object?>, object?> Resolver);

    private sealed record ReturningProjectionTemplate(
        string TableAlias,
        string ColumnAlias,
        string ColumnName,
        DbType DbType,
        bool IsNullable,
        int? ColumnIndex,
        object? LiteralValue,
        string? ParameterName);

    /// <summary>
    /// EN: Implements ExecuteScalar.
    /// PT: Implementa ExecuteScalar.
    /// </summary>
    public override object ExecuteScalar()
    {
        using var reader = ExecuteReader();
        if (reader.Read())
        {
            return reader.GetValue(0);
        }
        return DBNull.Value;
    }

    /// <summary>
    /// EN: Implements Prepare.
    /// PT: Implementa Prepare.
    /// </summary>
    public override void Prepare() { }

    /// <summary>
    /// EN: Disposes the command and resources.
    /// PT: Descarta o comando e os recursos.
    /// </summary>
    /// <param name="disposing">EN: True to dispose managed resources. PT: True para descartar recursos gerenciados.</param>
    protected override void Dispose(bool disposing)
    {
        if (!disposedValue)
        {
            disposedValue = true;
        }
        base.Dispose(disposing);
    }

    object ICloneable.Clone()
    {
        var clone = new MySqlCommandMock(connection, transaction)
        {
            CommandText = CommandText,
            CommandTimeout = CommandTimeout,
            CommandType = CommandType,
            UpdatedRowSource = UpdatedRowSource,
            DesignTimeVisible = DesignTimeVisible,
        };

        foreach (DbParameter parameter in Parameters)
        {
            clone.Parameters.Add(new MySqlParameter(parameter.ParameterName, parameter.Value)
            {
                Direction = parameter.Direction,
                SourceColumn = parameter.SourceColumn,
                SourceVersion = parameter.SourceVersion,
                IsNullable = parameter.IsNullable,
                Size = parameter.Size,
            });
        }

        return clone;
    }
}
