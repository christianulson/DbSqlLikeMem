using System.Diagnostics.CodeAnalysis;
using Npgsql;

namespace DbSqlLikeMem.Npgsql;

/// <summary>
/// EN: Represents a mock database command used to execute SQL text and stored procedures in memory.
/// PT: Representa um comando de banco de dados simulado usado para executar SQL e procedures em memória.
/// </summary>
public class NpgsqlCommandMock(
    NpgsqlConnectionMock? connection,
    NpgsqlTransactionMock? transaction = null
    ) : DbCommand, INpgsqlCommandMock
{
    /// <summary>
    /// EN: Initializes a new command instance without an attached connection or transaction.
    /// PT: Inicializa uma nova instância de comando sem conexão ou transação associada.
    /// </summary>
    public NpgsqlCommandMock()
        : this(null, null)
    {
    }

    private bool disposedValue;
    private readonly Dictionary<string, IReadOnlyList<ReturningProjectionTemplate>> _returningProjectionTemplateCache =
        new(StringComparer.Ordinal);

    /// <summary>
    /// EN: Gets or sets the SQL statement or stored procedure name that will be executed by this command.
    /// PT: Obtém ou define a instrução SQL ou o nome da procedure que será executada por este comando.
    /// </summary>
    [AllowNull]
    public override string CommandText { get; set; } = string.Empty;

    /// <summary>
    /// EN: Gets or sets the time, in seconds, to wait for command execution before timing out.
    /// PT: Obtém ou define o tempo, em segundos, para aguardar a execução do comando antes de expirar.
    /// </summary>
    public override int CommandTimeout { get; set; }

    /// <summary>
    /// EN: Gets or sets whether the command text is raw SQL text or a stored procedure name.
    /// PT: Obtém ou define se o texto do comando é SQL puro ou o nome de uma procedure.
    /// </summary>
    public override CommandType CommandType { get; set; } = CommandType.Text;

    /// <summary>
    /// EN: Gets or sets the connection associated with this command.
    /// PT: Obtém ou define a conexão associada a este comando.
    /// </summary>
    protected override DbConnection? DbConnection
    {
        get => connection;
        set => connection = value as NpgsqlConnectionMock;
    }

    private readonly NpgsqlDataParameterCollectionMock collectionMock = [];
    /// <summary>
    /// EN: Gets the parameter collection associated with this command.
    /// PT: Obtém a coleção de parâmetros associada a este comando.
    /// </summary>
    protected override DbParameterCollection DbParameterCollection => collectionMock;

    /// <summary>
    /// EN: Gets or sets the transaction associated with this command.
    /// PT: Obtém ou define a transação associada a este comando.
    /// </summary>
    protected override DbTransaction? DbTransaction
    {
        get => transaction!;
        set => transaction = value as NpgsqlTransactionMock;
    }

    /// <summary>
    /// EN: Gets or sets updated row source.
    /// PT: Obtém ou define como os resultados do comando são aplicados ao DataRow.
    /// </summary>
    public override UpdateRowSource UpdatedRowSource { get; set; }
    /// <summary>
    /// EN: Gets or sets design time visible.
    /// PT: Obtém ou define visível em tempo de design.
    /// </summary>
    public override bool DesignTimeVisible { get; set; }

    /// <summary>
    /// EN: Cancels the current command execution.
    /// PT: Cancela a execução atual do comando.
    /// </summary>
    public override void Cancel() => DbTransaction?.Rollback();

    /// <summary>
    /// EN: Creates a new db parameter instance.
    /// PT: Cria uma nova instância de parâmetro de banco.
    /// </summary>
    protected override DbParameter CreateDbParameter()
        // Por enquanto reusa NpgsqlParameter (NpgsqlConnector) para não puxar pacote de SqlClient.
        => new NpgsqlParameter();

    /// <summary>
    /// EN: Executes non-query and returns affected rows.
    /// PT: Executa non-consulta e retorna as linhas afetadas.
    /// </summary>
    public override int ExecuteNonQuery()
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(connection, nameof(connection));
        connection!.ClearExecutionPlans();
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(CommandText, nameof(CommandText));

        if (CommandType == CommandType.StoredProcedure)
        {
            var affected = connection!.ExecuteStoredProcedure(CommandText, Parameters);
            connection.SetLastFoundRows(affected);
            return affected;
        }

        return connection.ExecuteNonQueryWithPipeline(
            CommandText.NormalizeString(),
            Parameters,
            allowMerge: true,
            unionUsesSelectMessage: false,
            tryExecuteTransactionControl: TryExecuteTransactionControlCommand);
    }

    /// <summary>
    /// EN: Executes the command and returns a data reader.
    /// PT: Executa o comando e retorna um leitor de dados.
    /// </summary>
    protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(connection, nameof(connection));
        connection!.ClearExecutionPlans();
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(CommandText, nameof(CommandText));

        if (connection.TryHandleExecuteReaderPrelude(
            CommandType,
            CommandText,
            Parameters,
            static () => new NpgsqlDataReaderMock([[]]),
            normalizeSqlInput: true,
            out var earlyReader,
            out var statements))
        {
            return earlyReader!;
        }
        var executor = new NpgsqlAstQueryExecutor(connection!, Parameters);
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

            var query = SqlQueryParser.Parse(sqlRaw, connection.ExecutionDialect, Parameters);
            parsedStatementCount++;

            using var currentQueryScope = connection.BeginCurrentQueryScope(sqlRaw);
            connection.DispatchParsedReaderQuery(
                query,
                Parameters,
                executor,
                tables,
                executeInsert: ExecuteInsertReturning,
                executeUpdate: ExecuteUpdateReturning,
                executeDelete: ExecuteDeleteReturning,
                executeMerge: mergeQ => connection.ExecuteMerge(mergeQ, Parameters, connection.ExecutionDialect));
        }

        connection.FinalizeReaderExecution(tables, parsedStatementCount);

        return new NpgsqlDataReaderMock(tables);
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
        if (table is null)
            throw SqlUnsupported.ForDmlProjectionRequiresValidTargetTable("RETURNING");
        var targetTable = table;

        var hadReturning = query.Returning.Count > 0;
        var beforeCount = targetTable.Count;
        connection!.ExecuteInsert(query, Parameters, connection!.ExecutionDialect);

        if (!hadReturning)
            return null;

        var projections = BuildReturningProjection(query.Returning, query.Table!, targetTable);
        return BuildReturningResultFromIndexes(
            projections,
            targetTable,
            Enumerable.Range(beforeCount, Math.Max(0, targetTable.Count - beforeCount)));
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
        if (table is null)
            throw SqlUnsupported.ForDmlProjectionRequiresValidTargetTable("RETURNING");
        var targetTable = table;

        var hadReturning = query.Returning.Count > 0;
        List<int>? matchedIndexes = null;
        if (hadReturning)
            matchedIndexes = MatchRowIndexes(targetTable, query.WhereRaw, query.RawSql);

        connection!.ExecuteUpdateSmart(query, Parameters, connection!.ExecutionDialect);

        if (!hadReturning)
            return null;

        var projections = BuildReturningProjection(query.Returning, query.Table!, targetTable);
        return BuildReturningResultFromIndexes(projections, targetTable, matchedIndexes!);
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
        if (table is null)
            throw SqlUnsupported.ForDmlProjectionRequiresValidTargetTable("RETURNING");
        var targetTable = table;

        var hadReturning = query.Returning.Count > 0;
        TableResultMock? returningResult = null;
        if (hadReturning)
        {
            var matchedIndexes = MatchRowIndexes(targetTable, query.WhereRaw, query.RawSql);
            var projections = BuildReturningProjection(query.Returning, query.Table!, targetTable);
            returningResult = BuildReturningResultFromIndexes(projections, targetTable, matchedIndexes);
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
                {
                    AppendColumnTemplate(templates, tableAlias, table, item.Alias, id.Name);
                    break;
                }
                case ColumnExpr colExpr when colExpr.Name == "*":
                {
                    AppendAllColumnTemplates(templates, tableAlias, table);
                    break;
                }
                case ColumnExpr colExpr:
                {
                    AppendColumnTemplate(templates, tableAlias, table, item.Alias, colExpr.Name);
                    break;
                }
                case LiteralExpr literalExpr:
                {
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
                }
                case ParameterExpr parameterExpr:
                {
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
                }
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
    /// EN: Resolves command parameter value by SQL placeholder name.
    /// PT: Resolve valor de parâmetro do comando pelo nome do placeholder SQL.
    /// </summary>
    private object? ResolveParameterValue(string rawName)
    {
        var normalized = NormalizeParameterName(rawName);

        foreach (DbParameter parameter in Parameters)
        {
            var parameterName = NormalizeParameterName(parameter.ParameterName);
            if (!parameterName.Equals(normalized, StringComparison.OrdinalIgnoreCase))
                continue;
            return parameter.Value is DBNull ? null : parameter.Value;
        }

        return null;
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


    private bool TryExecuteTransactionControlCommand(string sqlRaw, out int affectedRows)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(connection, nameof(connection));
        return connection!.TryExecuteStandardTransactionControl(
            sqlRaw,
            releaseSavepointAsNoOp: false,
            out affectedRows);
    }

    /// <summary>
    /// EN: Executes the command and returns a scalar value.
    /// PT: Executa o comando e retorna um valor escalar.
    /// </summary>
    public override object ExecuteScalar()
    {
        using var reader = ExecuteReader();
        if (reader.Read())
            return reader.GetValue(0);
        return DBNull.Value;
    }

    /// <summary>
    /// EN: Represents Prepare.
    /// PT: Representa Prepare.
    /// </summary>
    public override void Prepare() { }

    /// <summary>
    /// EN: Releases resources used by this instance.
    /// PT: Libera os recursos usados por esta instância.
    /// </summary>
    protected override void Dispose(bool disposing)
    {
        if (!disposedValue)
        {
            disposedValue = true;
        }
        base.Dispose(disposing);
    }
}
