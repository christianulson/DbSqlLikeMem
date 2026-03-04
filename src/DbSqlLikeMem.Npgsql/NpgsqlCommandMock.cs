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

        var sqlRaw = CommandText.Trim();

        if (TryExecuteTransactionControlCommand(sqlRaw, out var transactionControlResult))
        {
            connection.SetLastFoundRows(transactionControlResult);
            return transactionControlResult;
        }

        // Mantém atalhos existentes (CALL / CREATE TABLE AS SELECT) por compatibilidade do engine atual
        if (sqlRaw.StartsWith("call ", StringComparison.OrdinalIgnoreCase))
        {
            var affected = connection!.ExecuteCall(sqlRaw, Parameters);
            connection.SetLastFoundRows(affected);
            return affected;
        }

        if (sqlRaw.StartsWith("create table", StringComparison.OrdinalIgnoreCase))
            return connection!.ExecuteCreateTableAsSelect(sqlRaw, Parameters, connection!.Db.Dialect);

        var query = SqlQueryParser.Parse(sqlRaw, connection!.Db.Dialect);

        return query switch
        {
            SqlInsertQuery insertQ => connection.ExecuteInsert(insertQ, Parameters, connection.Db.Dialect),
            SqlUpdateQuery updateQ => connection.ExecuteUpdateSmart(updateQ, Parameters, connection.Db.Dialect),
            SqlDeleteQuery deleteQ => connection.ExecuteDeleteSmart(deleteQ, Parameters, connection.Db.Dialect),
            SqlMergeQuery mergeQ => connection.ExecuteMerge(mergeQ, Parameters, connection.Db.Dialect),
            SqlCreateTemporaryTableQuery tempQ => connection.ExecuteCreateTemporaryTableAsSelect(tempQ, Parameters, connection.Db.Dialect),
            SqlCreateViewQuery viewQ => connection.ExecuteCreateView(viewQ, Parameters, connection.Db.Dialect),
            SqlDropViewQuery dropViewQ => connection.ExecuteDropView(dropViewQ, Parameters, connection.Db.Dialect),
            SqlSelectQuery _ => throw new InvalidOperationException(SqlExceptionMessages.UseExecuteReaderForSelect()),
            _ => throw SqlUnsupported.ForCommandType(connection!.Db.Dialect, "ExecuteNonQuery", query.GetType())
        };
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

        if (CommandType == CommandType.StoredProcedure)
        {
            connection!.ExecuteStoredProcedure(CommandText, Parameters);
            connection.SetLastFoundRows(0);
            return new NpgsqlDataReaderMock([[]]);
        }

        var sql = CommandText.NormalizeString();

        var statements = SqlQueryParser
            .SplitStatements(sql, connection!.Db.Dialect)
            .Where(s => !string.IsNullOrWhiteSpace(s))
            .ToList();

        if (statements.Count == 1 && statements[0].TrimStart().StartsWith("CALL", StringComparison.OrdinalIgnoreCase))
        {
            connection!.ExecuteCall(statements[0], Parameters);
            connection!.SetLastFoundRows(0);
            return new NpgsqlDataReaderMock([[]]);
        }
        var executor = new NpgsqlAstQueryExecutor(connection!, Parameters);
        var tables = new List<TableResultMock>();
        var parsedStatementCount = 0;

        foreach (var statementSql in statements)
        {
            var sqlRaw = statementSql.Trim();
            if (string.IsNullOrWhiteSpace(sqlRaw))
                continue;

            if (TryExecuteTransactionControlCommand(sqlRaw, out var transactionControlResult))
            {
                connection.SetLastFoundRows(transactionControlResult);
                parsedStatementCount++;
                continue;
            }

            if (sqlRaw.StartsWith("CALL", StringComparison.OrdinalIgnoreCase))
            {
                connection.ExecuteCall(sqlRaw, Parameters);
                connection.SetLastFoundRows(0);
                parsedStatementCount++;
                continue;
            }

            var query = SqlQueryParser.Parse(sqlRaw, connection.Db.Dialect, Parameters);
            parsedStatementCount++;

            switch (query)
            {
                case SqlSelectQuery selectQ:
                    tables.Add(executor.ExecuteSelect(selectQ));
                    break;

                case SqlUnionQuery unionQ:
                    tables.Add(executor.ExecuteUnion(unionQ.Parts, unionQ.AllFlags, unionQ.OrderBy, unionQ.RowLimit, unionQ.RawSql));
                    break;
                case SqlInsertQuery insertQ:
                {
                    var returning = ExecuteInsertReturning(insertQ);
                    if (returning is not null)
                        tables.Add(returning);
                    break;
                }
                case SqlUpdateQuery updateQ:
                {
                    var returning = ExecuteUpdateReturning(updateQ);
                    if (returning is not null)
                        tables.Add(returning);
                    break;
                }
                case SqlDeleteQuery deleteQ:
                {
                    var returning = ExecuteDeleteReturning(deleteQ);
                    if (returning is not null)
                        tables.Add(returning);
                    break;
                }
                case SqlMergeQuery mergeQ:
                    connection.ExecuteMerge(mergeQ, Parameters, connection.Db.Dialect);
                    break;
                case SqlCreateTemporaryTableQuery tempQ:
                    connection.ExecuteCreateTemporaryTableAsSelect(tempQ, Parameters, connection.Db.Dialect);
                    break;
                case SqlCreateViewQuery viewQ:
                    connection.ExecuteCreateView(viewQ, Parameters, connection.Db.Dialect);
                    break;
                case SqlDropViewQuery dropViewQ:
                    connection.ExecuteDropView(dropViewQ, Parameters, connection.Db.Dialect);
                    break;
                default:
                    throw SqlUnsupported.ForCommandType(connection!.Db.Dialect, "ExecuteReader", query.GetType());
            }
        }

        if (tables.Count == 0 && parsedStatementCount > 0)
            throw new InvalidOperationException(SqlExceptionMessages.ExecuteReaderWithoutSelectQuery());
        connection.Metrics.Selects += tables.Sum(t => t.Count);

        return new NpgsqlDataReaderMock(tables);
    }

    /// <summary>
    /// EN: Executes INSERT and materializes RETURNING result rows when requested.
    /// PT: Executa INSERT e materializa linhas de resultado de RETURNING quando solicitado.
    /// </summary>
    private TableResultMock? ExecuteInsertReturning(SqlInsertQuery query)
    {
        if (!TryResolveTargetTable(query.Table, out var table))
        {
            connection!.ExecuteInsert(query, Parameters, connection!.Db.Dialect);
            return null;
        }
        if (table is null)
            throw new InvalidOperationException("RETURNING requires a valid target table.");
        var targetTable = table;

        var hadReturning = query.Returning.Count > 0;
        var beforeCount = targetTable.Count;
        connection!.ExecuteInsert(query, Parameters, connection!.Db.Dialect);

        if (!hadReturning)
            return null;

        var insertedRows = Math.Max(0, targetTable.Count - beforeCount);
        var rows = new List<IReadOnlyDictionary<int, object?>>();
        for (var i = beforeCount; i < beforeCount + insertedRows; i++)
            rows.Add(SnapshotRow(targetTable[i]));

        return BuildReturningResult(query.Returning, query.Table!, targetTable, rows);
    }

    /// <summary>
    /// EN: Executes UPDATE and materializes RETURNING result rows when requested.
    /// PT: Executa UPDATE e materializa linhas de resultado de RETURNING quando solicitado.
    /// </summary>
    private TableResultMock? ExecuteUpdateReturning(SqlUpdateQuery query)
    {
        if (!TryResolveTargetTable(query.Table, out var table))
        {
            connection!.ExecuteUpdateSmart(query, Parameters, connection!.Db.Dialect);
            return null;
        }
        if (table is null)
            throw new InvalidOperationException("RETURNING requires a valid target table.");
        var targetTable = table;

        var hadReturning = query.Returning.Count > 0;
        List<int>? matchedIndexes = null;
        if (hadReturning)
            matchedIndexes = MatchRowIndexes(targetTable, query.WhereRaw, query.RawSql);

        connection!.ExecuteUpdateSmart(query, Parameters, connection!.Db.Dialect);

        if (!hadReturning)
            return null;

        var rows = new List<IReadOnlyDictionary<int, object?>>();
        foreach (var index in matchedIndexes!)
        {
            if (index < 0 || index >= targetTable.Count)
                continue;
            rows.Add(SnapshotRow(targetTable[index]));
        }

        return BuildReturningResult(query.Returning, query.Table!, targetTable, rows);
    }

    /// <summary>
    /// EN: Executes DELETE and materializes RETURNING result rows when requested.
    /// PT: Executa DELETE e materializa linhas de resultado de RETURNING quando solicitado.
    /// </summary>
    private TableResultMock? ExecuteDeleteReturning(SqlDeleteQuery query)
    {
        if (!TryResolveTargetTable(query.Table, out var table))
        {
            connection!.ExecuteDeleteSmart(query, Parameters, connection!.Db.Dialect);
            return null;
        }
        if (table is null)
            throw new InvalidOperationException("RETURNING requires a valid target table.");
        var targetTable = table;

        var hadReturning = query.Returning.Count > 0;
        List<IReadOnlyDictionary<int, object?>>? snapshotRows = null;
        if (hadReturning)
        {
            var matchedIndexes = MatchRowIndexes(targetTable, query.WhereRaw, query.RawSql);
            snapshotRows = [.. matchedIndexes.Select(i => SnapshotRow(targetTable[i]))];
        }

        connection!.ExecuteDeleteSmart(query, Parameters, connection!.Db.Dialect);

        if (!hadReturning)
            return null;

        return BuildReturningResult(query.Returning, query.Table!, targetTable, snapshotRows!);
    }

    /// <summary>
    /// EN: Builds a RETURNING result set from affected row snapshots.
    /// PT: Monta um conjunto de resultado RETURNING a partir de snapshots de linhas afetadas.
    /// </summary>
    private TableResultMock BuildReturningResult(
        IReadOnlyList<SqlSelectItem> returningItems,
        SqlTableSource tableSource,
        ITableMock table,
        IReadOnlyList<IReadOnlyDictionary<int, object?>> rows)
    {
        var result = new TableResultMock();
        var projections = BuildReturningProjection(returningItems, tableSource, table);
        result.Columns = projections
            .Select((p, i) => new TableResultColMock(
                p.TableAlias,
                p.ColumnAlias,
                p.ColumnName,
                i,
                p.DbType,
                p.IsNullable))
            .Cast<TableResultColMock>()
            .ToList();

        foreach (var row in rows)
        {
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
        var projections = new List<ReturningProjection>();
        var tableAlias = tableSource.Alias ?? tableSource.Name ?? "returning";

        foreach (var item in returningItems)
        {
            var raw = item.Raw.Trim();
            if (raw == "*")
            {
                AppendAllColumnsProjection(projections, tableAlias, table);
                continue;
            }

            var expr = SqlExpressionParser.ParseScalar(raw, connection!.Db.Dialect);
            switch (expr)
            {
                case IdentifierExpr id:
                {
                    var colName = NormalizeColumnReference(id.Name);
                    var col = table.GetColumn(colName);
                    projections.Add(new ReturningProjection(
                        TableAlias: tableAlias,
                        ColumnAlias: item.Alias ?? colName,
                        ColumnName: colName,
                        DbType: col.DbType,
                        IsNullable: col.Nullable,
                        Resolver: row => row.TryGetValue(col.Index, out var v) ? v : null));
                    break;
                }
                case ColumnExpr colExpr when colExpr.Name == "*":
                {
                    AppendAllColumnsProjection(projections, tableAlias, table);
                    break;
                }
                case ColumnExpr colExpr:
                {
                    var colName = NormalizeColumnReference(colExpr.Name);
                    var col = table.GetColumn(colName);
                    projections.Add(new ReturningProjection(
                        TableAlias: tableAlias,
                        ColumnAlias: item.Alias ?? colName,
                        ColumnName: colName,
                        DbType: col.DbType,
                        IsNullable: col.Nullable,
                        Resolver: row => row.TryGetValue(col.Index, out var v) ? v : null));
                    break;
                }
                case LiteralExpr literalExpr:
                {
                    var value = literalExpr.Value;
                    var dbType = value?.GetType().ConvertTypeToDbType() ?? DbType.Object;
                    projections.Add(new ReturningProjection(
                        TableAlias: tableAlias,
                        ColumnAlias: item.Alias ?? raw,
                        ColumnName: item.Alias ?? raw,
                        DbType: dbType,
                        IsNullable: value is null,
                        Resolver: _ => value));
                    break;
                }
                case ParameterExpr parameterExpr:
                {
                    var value = ResolveParameterValue(parameterExpr.Name);
                    var dbType = value?.GetType().ConvertTypeToDbType() ?? DbType.Object;
                    projections.Add(new ReturningProjection(
                        TableAlias: tableAlias,
                        ColumnAlias: item.Alias ?? parameterExpr.Name,
                        ColumnName: item.Alias ?? parameterExpr.Name,
                        DbType: dbType,
                        IsNullable: value is null,
                        Resolver: _ => value));
                    break;
                }
                default:
                    throw new NotSupportedException($"RETURNING expression not supported in executor: '{raw}'.");
            }
        }

        return projections;
    }

    /// <summary>
    /// EN: Appends projections for all table columns in ordinal order.
    /// PT: Adiciona projeções para todas as colunas da tabela na ordem ordinal.
    /// </summary>
    private static void AppendAllColumnsProjection(
        ICollection<ReturningProjection> projections,
        string tableAlias,
        ITableMock table)
    {
        foreach (var col in table.Columns.Values.OrderBy(c => c.Index))
        {
            var name = table.Columns.First(kv => kv.Value.Index == col.Index).Key;
            projections.Add(new ReturningProjection(
                TableAlias: tableAlias,
                ColumnAlias: name,
                ColumnName: name,
                DbType: col.DbType,
                IsNullable: col.Nullable,
                Resolver: row => row.TryGetValue(col.Index, out var v) ? v : null));
        }
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
        var normalized = rawName.Trim();
        if (normalized.Length > 0 && (normalized[0] == '@' || normalized[0] == ':' || normalized[0] == '?'))
            normalized = normalized[1..];

        foreach (DbParameter parameter in Parameters)
        {
            var parameterName = parameter.ParameterName?.TrimStart('@', ':', '?') ?? string.Empty;
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
    /// EN: Creates an immutable snapshot of a row dictionary.
    /// PT: Cria um snapshot imutável de um dicionário de linha.
    /// </summary>
    private static IReadOnlyDictionary<int, object?> SnapshotRow(IReadOnlyDictionary<int, object?> row)
        => row.ToDictionary(_ => _.Key, _ => _.Value);

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


    private bool TryExecuteTransactionControlCommand(string sqlRaw, out int affectedRows)
    {
        affectedRows = 0;

        ArgumentNullExceptionCompatible.ThrowIfNull(connection, nameof(connection));

        if (sqlRaw.Equals("begin", StringComparison.OrdinalIgnoreCase) ||
            sqlRaw.Equals("begin transaction", StringComparison.OrdinalIgnoreCase) ||
            sqlRaw.Equals("start transaction", StringComparison.OrdinalIgnoreCase))
        {
            if (connection!.State != ConnectionState.Open)
                connection.Open();

            if (!connection.HasActiveTransaction)
                connection.BeginTransaction();

            return true;
        }

        if (sqlRaw.StartsWith("savepoint ", StringComparison.OrdinalIgnoreCase))
        {
            connection!.CreateSavepoint(sqlRaw[10..].Trim());
            return true;
        }

        if (sqlRaw.StartsWith("rollback to savepoint ", StringComparison.OrdinalIgnoreCase))
        {
            connection!.RollbackTransaction(sqlRaw[22..].Trim());
            return true;
        }

        if (sqlRaw.StartsWith("release savepoint ", StringComparison.OrdinalIgnoreCase))
        {
            connection!.ReleaseSavepoint(sqlRaw[18..].Trim());
            return true;
        }

        if (sqlRaw.Equals("commit", StringComparison.OrdinalIgnoreCase))
        {
            connection!.CommitTransaction();
            return true;
        }

        if (sqlRaw.Equals("rollback", StringComparison.OrdinalIgnoreCase))
        {
            connection!.RollbackTransaction();
            return true;
        }

        return false;
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
