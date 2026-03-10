using System.Diagnostics.CodeAnalysis;
using Microsoft.Data.SqlClient;
using System.Text;


namespace DbSqlLikeMem.SqlServer;

/// <summary>
/// EN: Represents a mock database command used to execute SQL text and stored procedures in memory.
/// PT: Representa um comando de banco de dados simulado usado para executar SQL e procedures em memória.
/// </summary>
public class SqlServerCommandMock(
    SqlServerConnectionMock? connection,
    SqlServerTransactionMock? transaction = null
    ) : DbCommand, ISqlServerCommandMock
{
    /// <summary>
    /// EN: Initializes a new command instance without an attached connection or transaction.
    /// PT: Inicializa uma nova instância de comando sem conexão ou transação associada.
    /// </summary>
    public SqlServerCommandMock() : this(null, null)
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
        set => connection = value as SqlServerConnectionMock;
    }

    private readonly SqlServerDataParameterCollectionMock collectionMock = [];
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
        set => transaction = value as SqlServerTransactionMock;
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
        => new SqlParameter();

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
            static () => new SqlServerDataReaderMock([[]]),
            normalizeSqlInput: true,
            out var earlyReader,
            out var statements))
        {
            return earlyReader!;
        }
        var executor = new SqlServerAstQueryExecutor(connection!, Parameters);
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

            var effectiveSql = sqlRaw;
            SqlServerOutputClause? outputClause = null;
            if (TryExtractOutputClause(sqlRaw, out var rewrittenSql, out var extractedOutput))
            {
                effectiveSql = rewrittenSql;
                outputClause = extractedOutput;
            }

            var query = SqlQueryParser.Parse(effectiveSql, connection.Db.Dialect, Parameters);
            parsedStatementCount++;

            connection.DispatchParsedReaderQuery(
                query,
                Parameters,
                executor,
                tables,
                executeInsert: insertQ =>
                {
                    if (outputClause is null)
                    {
                        connection.ExecuteInsert(insertQ, Parameters, connection.Db.Dialect);
                        return null;
                    }
                    return ExecuteInsertOutput(insertQ, outputClause);
                },
                executeUpdate: updateQ =>
                {
                    if (outputClause is null)
                    {
                        connection.ExecuteUpdateSmart(updateQ, Parameters, connection.Db.Dialect);
                        return null;
                    }
                    return ExecuteUpdateOutput(updateQ, outputClause);
                },
                executeDelete: deleteQ =>
                {
                    if (outputClause is null)
                    {
                        connection.ExecuteDeleteSmart(deleteQ, Parameters, connection.Db.Dialect);
                        return null;
                    }
                    return ExecuteDeleteOutput(deleteQ, outputClause);
                },
                executeMerge: mergeQ => connection.ExecuteMerge(mergeQ, Parameters, connection.ExecutionDialect));
        }

        connection.FinalizeReaderExecution(tables, parsedStatementCount);

        return new SqlServerDataReaderMock(tables);
    }

    /// <summary>
    /// EN: Executes INSERT and materializes SQL Server OUTPUT rows when requested.
    /// PT: Executa INSERT e materializa linhas de OUTPUT do SQL Server quando solicitado.
    /// </summary>
    private TableResultMock ExecuteInsertOutput(SqlInsertQuery query, SqlServerOutputClause outputClause)
    {
        if (!TryResolveTargetTable(query.Table, out var table) || table == null)
            throw SqlUnsupported.ForDmlProjectionRequiresValidTargetTable("OUTPUT");
        var targetTable = table;

        var beforeCount = targetTable.Count;
        connection!.ExecuteInsert(query, Parameters, connection!.ExecutionDialect);
        var insertedRows = Math.Max(0, targetTable.Count - beforeCount);
        var pairs = new List<(IReadOnlyDictionary<int, object?>? OldRow, IReadOnlyDictionary<int, object?>? NewRow)>();
        for (var i = beforeCount; i < beforeCount + insertedRows; i++)
            pairs.Add((null, SnapshotRow(targetTable[i])));

        return BuildOutputResult(outputClause, query.Table!, targetTable, pairs, SqlServerOutputDefaultQualifier.Inserted);
    }

    /// <summary>
    /// EN: Executes UPDATE and materializes SQL Server OUTPUT rows when requested.
    /// PT: Executa UPDATE e materializa linhas de OUTPUT do SQL Server quando solicitado.
    /// </summary>
    private TableResultMock ExecuteUpdateOutput(SqlUpdateQuery query, SqlServerOutputClause outputClause)
    {
        if (!TryResolveTargetTable(query.Table, out var table) || table == null)
            throw SqlUnsupported.ForDmlProjectionRequiresValidTargetTable("OUTPUT");
        var targetTable = table;

        var matchedIndexes = MatchRowIndexes(targetTable, query.WhereRaw, query.RawSql);
        var beforeRows = matchedIndexes.Select(i => SnapshotRow(targetTable[i])).ToList();
        connection!.ExecuteUpdateSmart(query, Parameters, connection!.ExecutionDialect);

        var pairs = new List<(IReadOnlyDictionary<int, object?>? OldRow, IReadOnlyDictionary<int, object?>? NewRow)>();
        for (var i = 0; i < matchedIndexes.Count; i++)
        {
            var index = matchedIndexes[i];
            if (index < 0 || index >= targetTable.Count)
                continue;
            pairs.Add((beforeRows[i], SnapshotRow(targetTable[index])));
        }

        return BuildOutputResult(outputClause, query.Table!, targetTable, pairs, SqlServerOutputDefaultQualifier.Inserted);
    }

    /// <summary>
    /// EN: Executes DELETE and materializes SQL Server OUTPUT rows when requested.
    /// PT: Executa DELETE e materializa linhas de OUTPUT do SQL Server quando solicitado.
    /// </summary>
    private TableResultMock ExecuteDeleteOutput(SqlDeleteQuery query, SqlServerOutputClause outputClause)
    {
        if (!TryResolveTargetTable(query.Table, out var table) || table == null)
            throw SqlUnsupported.ForDmlProjectionRequiresValidTargetTable("OUTPUT");
        var targetTable = table;

        var matchedIndexes = MatchRowIndexes(targetTable, query.WhereRaw, query.RawSql);
        var deletedRows = matchedIndexes.Select(i => SnapshotRow(targetTable[i])).ToList();
        connection!.ExecuteDeleteSmart(query, Parameters, connection!.ExecutionDialect);

        var pairs = deletedRows
            .Select(row => (OldRow: (IReadOnlyDictionary<int, object?>?)row, NewRow: (IReadOnlyDictionary<int, object?>?)null))
            .ToList();

        return BuildOutputResult(outputClause, query.Table!, targetTable, pairs, SqlServerOutputDefaultQualifier.Deleted);
    }

    /// <summary>
    /// EN: Builds output result set from old/new row pairs.
    /// PT: Monta o conjunto de resultado de output a partir de pares de linhas antigas/novas.
    /// </summary>
    private TableResultMock BuildOutputResult(
        SqlServerOutputClause outputClause,
        SqlTableSource tableSource,
        ITableMock table,
        IReadOnlyList<(IReadOnlyDictionary<int, object?>? OldRow, IReadOnlyDictionary<int, object?>? NewRow)> rowPairs,
        SqlServerOutputDefaultQualifier defaultQualifier)
    {
        var result = new TableResultMock();
        var projections = BuildOutputProjection(outputClause, tableSource, table, defaultQualifier);
        result.Columns = projections
            .Select((p, i) => new TableResultColMock(
                p.TableAlias,
                p.ColumnAlias,
                p.ColumnName,
                i,
                p.DbType,
                p.IsNullable))
            .ToList();

        foreach (var (oldRow, newRow) in rowPairs)
        {
            var row = new Dictionary<int, object?>();
            for (var i = 0; i < projections.Count; i++)
                row[i] = projections[i].Resolver(oldRow, newRow);
            result.Add(row);
        }

        return result;
    }

    /// <summary>
    /// EN: Builds projection metadata and resolvers for SQL Server OUTPUT items.
    /// PT: Monta metadados de projeção e resolvedores para itens de OUTPUT do SQL Server.
    /// </summary>
    private List<SqlServerOutputProjection> BuildOutputProjection(
        SqlServerOutputClause outputClause,
        SqlTableSource tableSource,
        ITableMock table,
        SqlServerOutputDefaultQualifier defaultQualifier)
    {
        var projections = new List<SqlServerOutputProjection>();
        var tableAlias = tableSource.Alias ?? tableSource.Name ?? "output";

        foreach (var item in outputClause.Items)
        {
            if (item.IsWildcard)
            {
                AppendOutputWildcardProjection(projections, tableAlias, table, item.Qualifier, defaultQualifier);
                continue;
            }

            var colName = NormalizeColumnReference(item.ColumnName);
            var col = table.GetColumn(colName);
            var alias = item.Alias ?? colName;
            projections.Add(new SqlServerOutputProjection(
                TableAlias: tableAlias,
                ColumnAlias: alias,
                ColumnName: colName,
                DbType: col.DbType,
                IsNullable: col.Nullable,
                Resolver: (oldRow, newRow) =>
                {
                    var source = ResolveOutputSourceRow(item.Qualifier, defaultQualifier, oldRow, newRow);
                    return source is not null && source.TryGetValue(col.Index, out var value) ? value : null;
                }));
        }

        return projections;
    }

    /// <summary>
    /// EN: Appends wildcard OUTPUT projection using requested pseudo-table source.
    /// PT: Adiciona projeção OUTPUT wildcard usando a pseudo-tabela solicitada.
    /// </summary>
    private static void AppendOutputWildcardProjection(
        ICollection<SqlServerOutputProjection> projections,
        string tableAlias,
        ITableMock table,
        SqlServerOutputQualifier qualifier,
        SqlServerOutputDefaultQualifier defaultQualifier)
    {
        foreach (var col in table.Columns.Values.OrderBy(c => c.Index))
        {
            var colName = table.Columns.First(kv => kv.Value.Index == col.Index).Key;
            projections.Add(new SqlServerOutputProjection(
                TableAlias: tableAlias,
                ColumnAlias: colName,
                ColumnName: colName,
                DbType: col.DbType,
                IsNullable: col.Nullable,
                Resolver: (oldRow, newRow) =>
                {
                    var source = ResolveOutputSourceRow(qualifier, defaultQualifier, oldRow, newRow);
                    return source is not null && source.TryGetValue(col.Index, out var value) ? value : null;
                }));
        }
    }

    /// <summary>
    /// EN: Resolves pseudo-table source for OUTPUT item.
    /// PT: Resolve a fonte de pseudo-tabela para item de OUTPUT.
    /// </summary>
    private static IReadOnlyDictionary<int, object?>? ResolveOutputSourceRow(
        SqlServerOutputQualifier qualifier,
        SqlServerOutputDefaultQualifier defaultQualifier,
        IReadOnlyDictionary<int, object?>? oldRow,
        IReadOnlyDictionary<int, object?>? newRow)
    {
        return qualifier switch
        {
            SqlServerOutputQualifier.Inserted => newRow,
            SqlServerOutputQualifier.Deleted => oldRow,
            _ => defaultQualifier == SqlServerOutputDefaultQualifier.Inserted ? newRow : oldRow
        };
    }

    /// <summary>
    /// EN: Parses SQL Server OUTPUT clause and rewrites statement without OUTPUT for AST parser.
    /// PT: Faz parse da cláusula OUTPUT do SQL Server e reescreve o comando sem OUTPUT para o parser AST.
    /// </summary>
    private static bool TryExtractOutputClause(
        string sql,
        out string rewrittenSql,
        out SqlServerOutputClause outputClause)
    {
        rewrittenSql = sql;
        outputClause = null!;

        if (TryExtractInsertOutput(sql, out rewrittenSql, out outputClause))
            return true;

        if (TryExtractUpdateOutput(sql, out rewrittenSql, out outputClause))
            return true;

        return TryExtractDeleteOutput(sql, out rewrittenSql, out outputClause);
    }

    private static bool TryExtractInsertOutput(
        string sql,
        out string rewrittenSql,
        out SqlServerOutputClause outputClause)
    {
        rewrittenSql = sql;
        outputClause = null!;
        var match = Regex.Match(
            sql,
            @"^(?<prefix>\s*INSERT\b[\s\S]*?)\bOUTPUT\b(?<output>[\s\S]*?)\bVALUES\b(?<suffix>[\s\S]*)$",
            RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);
        if (!match.Success)
            return false;

        var outputText = match.Groups["output"].Value;
        var items = ParseOutputItems(outputText);
        rewrittenSql = $"{match.Groups["prefix"].Value} VALUES{match.Groups["suffix"].Value}";
        outputClause = new SqlServerOutputClause(items);
        return true;
    }

    private static bool TryExtractUpdateOutput(
        string sql,
        out string rewrittenSql,
        out SqlServerOutputClause outputClause)
    {
        rewrittenSql = sql;
        outputClause = null!;
        var match = Regex.Match(
            sql,
            @"^(?<prefix>\s*UPDATE\b[\s\S]*?\bSET\b[\s\S]*?)\bOUTPUT\b(?<output>[\s\S]*?)(?<suffix>\bFROM\b[\s\S]*|\bWHERE\b[\s\S]*|$)",
            RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);
        if (!match.Success)
            return false;

        var outputText = match.Groups["output"].Value;
        var items = ParseOutputItems(outputText);
        rewrittenSql = $"{match.Groups["prefix"].Value} {match.Groups["suffix"].Value}".TrimEnd();
        outputClause = new SqlServerOutputClause(items);
        return true;
    }

    private static bool TryExtractDeleteOutput(
        string sql,
        out string rewrittenSql,
        out SqlServerOutputClause outputClause)
    {
        rewrittenSql = sql;
        outputClause = null!;
        var match = Regex.Match(
            sql,
            @"^(?<prefix>\s*DELETE\b[\s\S]*?\bFROM\b[\s\S]*?)\bOUTPUT\b(?<output>[\s\S]*?)(?<suffix>\bWHERE\b[\s\S]*|$)",
            RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);
        if (!match.Success)
            return false;

        var outputText = match.Groups["output"].Value;
        var items = ParseOutputItems(outputText);
        rewrittenSql = $"{match.Groups["prefix"].Value} {match.Groups["suffix"].Value}".TrimEnd();
        outputClause = new SqlServerOutputClause(items);
        return true;
    }

    /// <summary>
    /// EN: Parses comma-separated OUTPUT item list.
    /// PT: Faz parse da lista de itens OUTPUT separados por vírgula.
    /// </summary>
    private static IReadOnlyList<SqlServerOutputItem> ParseOutputItems(string outputText)
    {
        var items = SplitTopLevelComma(outputText)
            .Select(ParseOutputItem)
            .ToList();
        if (items.Count == 0)
            throw SqlUnsupported.ForProjectionClauseEmpty("OUTPUT");
        return items;
    }

    private static SqlServerOutputItem ParseOutputItem(string rawItem)
    {
        var text = rawItem.Trim();
        if (string.IsNullOrWhiteSpace(text))
            throw SqlUnsupported.ForProjectionItemEmpty("OUTPUT");

        var aliasMatch = Regex.Match(
            text,
            @"^(?<expr>[\s\S]+?)(?:\s+AS\s+(?<alias>\[[^\]]+\]|[A-Za-z_][A-Za-z0-9_]*))?$",
            RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);
        var expr = aliasMatch.Success ? aliasMatch.Groups["expr"].Value.Trim() : text;
        var alias = aliasMatch.Success && aliasMatch.Groups["alias"].Success
            ? aliasMatch.Groups["alias"].Value.Trim().Trim('[', ']').NormalizeName()
            : null;

        var wildcardMatch = Regex.Match(
            expr,
            @"^(?<q>inserted|deleted)\.\*$",
            RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);
        if (wildcardMatch.Success)
        {
            var qualifier = ParseOutputQualifier(wildcardMatch.Groups["q"].Value);
            return new SqlServerOutputItem(qualifier, "*", alias, IsWildcard: true);
        }

        var qualifiedMatch = Regex.Match(
            expr,
            @"^(?:(?<q>inserted|deleted)\.)?(?<c>\[[^\]]+\]|[A-Za-z_][A-Za-z0-9_]*)$",
            RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);
        if (!qualifiedMatch.Success)
            throw SqlUnsupported.ForDmlProjectionExpressionNotSupportedInExecutor("OUTPUT", expr);

        var qual = qualifiedMatch.Groups["q"].Success
            ? ParseOutputQualifier(qualifiedMatch.Groups["q"].Value)
            : SqlServerOutputQualifier.Unspecified;
        var column = qualifiedMatch.Groups["c"].Value.Trim().Trim('[', ']').NormalizeName();
        return new SqlServerOutputItem(qual, column, alias, IsWildcard: false);
    }

    private static SqlServerOutputQualifier ParseOutputQualifier(string rawQualifier)
        => rawQualifier.Equals("deleted", StringComparison.OrdinalIgnoreCase)
            ? SqlServerOutputQualifier.Deleted
            : SqlServerOutputQualifier.Inserted;

    /// <summary>
    /// EN: Splits comma-separated text honoring simple quote and parenthesis nesting.
    /// PT: Divide texto separado por vírgula respeitando aspas simples e aninhamento de parênteses.
    /// </summary>
    private static List<string> SplitTopLevelComma(string text)
    {
        var items = new List<string>();
        var current = new StringBuilder();
        var depth = 0;
        var inSingle = false;

        for (var i = 0; i < text.Length; i++)
        {
            var ch = text[i];
            if (ch == '\'' && (i == 0 || text[i - 1] != '\\'))
                inSingle = !inSingle;

            if (!inSingle)
            {
                if (ch == '(') depth++;
                else if (ch == ')' && depth > 0) depth--;
                else if (ch == ',' && depth == 0)
                {
                    items.Add(current.ToString().Trim());
                    current.Clear();
                    continue;
                }
            }

            current.Append(ch);
        }

        if (current.Length > 0)
            items.Add(current.ToString().Trim());

        return items.Where(i => !string.IsNullOrWhiteSpace(i)).ToList();
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

    private sealed record SqlServerOutputClause(IReadOnlyList<SqlServerOutputItem> Items);
    private sealed record SqlServerOutputItem(SqlServerOutputQualifier Qualifier, string ColumnName, string? Alias, bool IsWildcard);
    private sealed record SqlServerOutputProjection(
        string TableAlias,
        string ColumnAlias,
        string ColumnName,
        DbType DbType,
        bool IsNullable,
        Func<IReadOnlyDictionary<int, object?>?, IReadOnlyDictionary<int, object?>?, object?> Resolver);

    private enum SqlServerOutputQualifier
    {
        Unspecified = 0,
        Inserted = 1,
        Deleted = 2
    }

    private enum SqlServerOutputDefaultQualifier
    {
        Inserted = 0,
        Deleted = 1
    }


    private bool TryExecuteTransactionControlCommand(string sqlRaw, out int affectedRows)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(connection, nameof(connection));
        return connection!.TryExecuteStandardTransactionControl(
            sqlRaw,
            releaseSavepointAsNoOp: true,
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
