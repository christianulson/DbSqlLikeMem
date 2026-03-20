using System.Diagnostics.CodeAnalysis;
using Oracle.ManagedDataAccess.Client;
using System.Text;

namespace DbSqlLikeMem.Oracle;

/// <summary>
/// EN: Represents a mock database command used to execute SQL text and stored procedures in memory.
/// PT: Representa um comando de banco de dados simulado usado para executar SQL e procedures em memória.
/// </summary>
public class OracleCommandMock(
    OracleConnectionMock? connection,
    OracleTransactionMock? transaction = null
    ) : DbCommand, IOracleCommandMock
{
    /// <summary>
    /// EN: Initializes a new command instance without an attached connection or transaction.
    /// PT: Inicializa uma nova instância de comando sem conexão ou transação associada.
    /// </summary>
    public OracleCommandMock() : this(null, null)
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
        set => connection = value as OracleConnectionMock;
    }

    private readonly OracleDataParameterCollectionMock collectionMock = [];
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
        set => transaction = value as OracleTransactionMock;
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
        => new OracleParameter();

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
            connection.SetLastFoundRows(affected.AffectedRows);
            return affected.AffectedRows;
        }

        var normalizedCommandText = CommandText.NormalizeString();
        if (TryExecuteOracleFunctionDdl(normalizedCommandText, out var functionDdlAffectedRows))
            return functionDdlAffectedRows.AffectedRows;

        return connection.ExecuteNonQueryWithPipeline(
            normalizedCommandText,
            Parameters,
            allowMerge: true,
            unionUsesSelectMessage: false,
            tryExecuteTransactionControl: TryExecuteTransactionControlCommand,
            tryExecuteSpecialCommand: TryExecuteNonQuerySpecialCommand);
    }

    private bool TryExecuteOracleFunctionDdl(string sqlRaw, out DmlExecutionResult affectedRows)
    {
        affectedRows = new DmlExecutionResult();

        if (!LooksLikeOracleFunctionDdl(sqlRaw))
            return false;

        var query = SqlQueryParser.Parse(sqlRaw, connection!.ExecutionDialect, Parameters);
        if (query is not SqlCreateFunctionQuery createFunctionQuery)
            return false;

        affectedRows = connection.ExecuteCreateFunction(createFunctionQuery, Parameters, connection.ExecutionDialect);
        return true;
    }

    private static bool LooksLikeOracleFunctionDdl(string sqlRaw)
    {
        var trimmed = sqlRaw.TrimStart();
        return trimmed.StartsWith("CREATE FUNCTION ", StringComparison.OrdinalIgnoreCase)
            || trimmed.StartsWith("CREATE OR REPLACE FUNCTION ", StringComparison.OrdinalIgnoreCase);
    }

    private bool TryExecuteNonQuerySpecialCommand(string sqlRaw, out DmlExecutionResult affectedRows)
    {
        affectedRows = new DmlExecutionResult();

        if (TryExtractOracleReturningIntoClause(sqlRaw, out var rewrittenSql, out var clause))
        {
            var query = SqlQueryParser.Parse(rewrittenSql, connection!.Db.Dialect);
            affectedRows = ExecuteNonQueryWithReturningInto(query, clause);
            return true;
        }

        return false;
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
        using var _ = connection.Metrics.BeginAmbientScope();
        using var currentQueryScope = connection.BeginCurrentQueryScope(CommandText);

        if (connection.TryHandleExecuteReaderPrelude(
            CommandType,
            CommandText,
            Parameters,
            static () => new OracleDataReaderMock([[]]),
            normalizeSqlInput: true,
            out var earlyReader,
            out var statements))
        {
            return earlyReader!;
        }
        var executor = new OracleAstQueryExecutor(connection!, Parameters);
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

            using var statementQueryScope = connection.BeginCurrentQueryScope(sqlRaw);
            connection.DispatchParsedReaderQuery(
                query,
                Parameters,
                executor,
                tables,
                executeUpdate: updateQ =>
                {
                    connection.ExecuteUpdate(updateQ, Parameters);
                    return null;
                },
                executeDelete: deleteQ =>
                {
                    connection.ExecuteDelete(deleteQ, Parameters);
                    return null;
                });
        }

        connection.FinalizeReaderExecution(tables, parsedStatementCount);

        return new OracleDataReaderMock(tables);
    }

    private bool TryExecuteTransactionControlCommand(string sqlRaw, out DmlExecutionResult affectedRows)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(connection, nameof(connection));
        return connection!.TryExecuteStandardTransactionControl(
            sqlRaw,
            releaseSavepointAsNoOp: false,
            out affectedRows);
    }

    /// <summary>
    /// EN: Executes DML and populates Oracle RETURNING INTO output parameters.
    /// PT: Executa DML e preenche parâmetros de saída de RETURNING INTO no Oracle.
    /// </summary>
    private DmlExecutionResult ExecuteNonQueryWithReturningInto(
        SqlQueryBase query,
        OracleReturningIntoClause clause)
    {
        var affectedRows = query switch
        {
            SqlInsertQuery insertQuery => ExecuteInsertWithReturningInto(insertQuery, clause),
            SqlUpdateQuery updateQuery => ExecuteUpdateWithReturningInto(updateQuery, clause),
            SqlDeleteQuery deleteQuery => ExecuteDeleteWithReturningInto(deleteQuery, clause),
            _ => throw SqlUnsupported.ForReturningIntoOnlySupportedInExecuteNonQuery()
        };

        return affectedRows;
    }

    /// <summary>
    /// EN: Executes INSERT for RETURNING INTO and reads only the first inserted row needed by output parameters.
    /// PT: Executa INSERT para RETURNING INTO e lê apenas a primeira linha inserida necessária aos parâmetros de saída.
    /// </summary>
    private DmlExecutionResult ExecuteInsertWithReturningInto(
        SqlInsertQuery query,
        OracleReturningIntoClause clause)
    {
        if (!TryResolveTargetTable(query.Table, out var table) || table == null)
            throw SqlUnsupported.ForDmlProjectionRequiresValidTargetTable("RETURNING INTO");

        var beforeCount = table.Count;
        var affected = connection!.ExecuteInsert(query, Parameters, connection!.ExecutionDialect);
        var sourceRow = beforeCount >= 0 && beforeCount < table.Count ? table[beforeCount] : null;
        PopulateReturningIntoParameters(clause, table, sourceRow);
        return affected;
    }

    /// <summary>
    /// EN: Executes UPDATE for RETURNING INTO and reads only the first updated row needed by output parameters.
    /// PT: Executa UPDATE para RETURNING INTO e lê apenas a primeira linha atualizada necessária aos parâmetros de saída.
    /// </summary>
    private DmlExecutionResult ExecuteUpdateWithReturningInto(
        SqlUpdateQuery query,
        OracleReturningIntoClause clause)
    {
        if (!TryResolveTargetTable(query.Table, out var table) || table == null)
            throw SqlUnsupported.ForDmlProjectionRequiresValidTargetTable("RETURNING INTO");

        var matchedIndexes = MatchRowIndexes(table, query.WhereRaw, query.RawSql);
        var affected = connection!.ExecuteUpdate(query, Parameters);
        var sourceRow = TryGetFirstMatchedRow(table, matchedIndexes);
        PopulateReturningIntoParameters(clause, table, sourceRow);
        return affected;
    }

    /// <summary>
    /// EN: Executes DELETE for RETURNING INTO and snapshots only the first deleted row needed by output parameters.
    /// PT: Executa DELETE para RETURNING INTO e gera snapshot apenas da primeira linha excluída necessária aos parâmetros de saída.
    /// </summary>
    private DmlExecutionResult ExecuteDeleteWithReturningInto(
        SqlDeleteQuery query,
        OracleReturningIntoClause clause)
    {
        if (!TryResolveTargetTable(query.Table, out var table) || table == null)
            throw SqlUnsupported.ForDmlProjectionRequiresValidTargetTable("RETURNING INTO");

        var matchedIndexes = MatchRowIndexes(table, query.WhereRaw, query.RawSql);
        var sourceRow = TryGetFirstMatchedRowSnapshot(table, matchedIndexes);
        var affected = connection!.ExecuteDelete(query, Parameters);
        PopulateReturningIntoParameters(clause, table, sourceRow);
        return affected;
    }

    /// <summary>
     /// EN: Populates Oracle output parameters from first affected row according to RETURNING INTO mapping.
     /// PT: Preenche parâmetros de saída do Oracle a partir da primeira linha afetada conforme mapeamento RETURNING INTO.
     /// </summary>
    private void PopulateReturningIntoParameters(
        OracleReturningIntoClause clause,
        ITableMock table,
        IReadOnlyDictionary<int, object?>? sourceRow)
    {
        for (var i = 0; i < clause.ColumnNames.Count; i++)
        {
            var columnName = NormalizeColumnReference(clause.ColumnNames[i]);
            var parameterName = clause.ParameterNames[i];
            var parameter = ResolveParameter(parameterName);
            if (parameter is null)
                continue;

            if (sourceRow is null)
            {
                parameter.Value = DBNull.Value;
                continue;
            }

            var col = table.GetColumn(columnName);
            parameter.Value = sourceRow.TryGetValue(col.Index, out var value)
                ? value ?? DBNull.Value
                : DBNull.Value;
        }
    }

    /// <summary>
    /// EN: Resolves parameter by name accepting Oracle prefixes.
    /// PT: Resolve parâmetro por nome aceitando prefixes do Oracle.
    /// </summary>
    private DbParameter? ResolveParameter(string rawName)
    {
        var normalized = rawName.Trim().TrimStart(':', '@', '?');
        foreach (DbParameter parameter in Parameters)
        {
            var candidate = parameter.ParameterName?.Trim().TrimStart(':', '@', '?');
            if (string.Equals(candidate, normalized, StringComparison.OrdinalIgnoreCase))
                return parameter;
        }

        return null;
    }

    /// <summary>
    /// EN: Parses Oracle RETURNING ... INTO ... suffix and rewrites SQL for core parser.
    /// PT: Faz parse do sufixo Oracle RETURNING ... INTO ... e reescreve SQL para o parser core.
    /// </summary>
    private static bool TryExtractOracleReturningIntoClause(
        string sql,
        out string rewrittenSql,
        out OracleReturningIntoClause clause)
    {
        rewrittenSql = sql;
        clause = null!;

        const string returningKeyword = SqlConst.RETURNING;
        const string intoKeyword = SqlConst.INTO;

        var returningIndex = FindLastTopLevelKeyword(sql, returningKeyword);
        if (returningIndex < 0)
            return false;

        var intoIndex = FindFirstTopLevelKeyword(sql, intoKeyword, returningIndex + returningKeyword.Length);
        if (intoIndex < 0)
            return false;

        var colsText = sql.Substring(returningIndex + returningKeyword.Length, intoIndex - (returningIndex + returningKeyword.Length));
        var parsText = sql[(intoIndex + intoKeyword.Length)..];

        var cols = SplitTopLevelComma(colsText)
            .Select(c => c.Trim())
            .Where(c => !string.IsNullOrWhiteSpace(c))
            .ToList();
        var pars = SplitTopLevelComma(parsText)
            .Select(p => p.Trim().TrimEnd(';'))
            .Where(p => !string.IsNullOrWhiteSpace(p))
            .ToList();
        if (cols.Count == 0 || cols.Count != pars.Count)
            throw SqlUnsupported.ForReturningIntoColumnParameterCountMismatch();

        rewrittenSql = sql[..returningIndex].TrimEnd();
        clause = new OracleReturningIntoClause(cols, pars);
        return true;
    }

    /// <summary>
    /// EN: Finds the first top-level keyword occurrence outside quoted literals and identifiers.
    /// PT: Encontra a primeira ocorrência de palavra-chave em nível de topo fora de literais e identificadores entre aspas.
    /// </summary>
    private static int FindFirstTopLevelKeyword(string text, string keyword, int startIndex)
    {
        var inSingle = false;
        var inDouble = false;

        for (var i = 0; i < text.Length; i++)
        {
            var ch = text[i];

            if (i < startIndex)
            {
                if (ch == '\'' && !inDouble)
                {
                    if (inSingle && i + 1 < text.Length && text[i + 1] == '\'')
                    {
                        i++;
                        continue;
                    }

                    inSingle = !inSingle;
                }
                else if (ch == '"' && !inSingle)
                {
                    if (inDouble && i + 1 < text.Length && text[i + 1] == '"')
                    {
                        i++;
                        continue;
                    }

                    inDouble = !inDouble;
                }

                continue;
            }

            if (ch == '\'' && !inDouble)
            {
                if (inSingle && i + 1 < text.Length && text[i + 1] == '\'')
                {
                    i++;
                    continue;
                }

                inSingle = !inSingle;
                continue;
            }

            if (ch == '"' && !inSingle)
            {
                if (inDouble && i + 1 < text.Length && text[i + 1] == '"')
                {
                    i++;
                    continue;
                }

                inDouble = !inDouble;
                continue;
            }

            if (inSingle || inDouble)
                continue;

            if (i + keyword.Length > text.Length)
                break;

            if (!text.AsSpan(i, keyword.Length).Equals(keyword, StringComparison.OrdinalIgnoreCase))
                continue;

            var before = i > 0 ? text[i - 1] : '\0';
            var after = i + keyword.Length < text.Length ? text[i + keyword.Length] : '\0';
            if (IsIdentifierChar(before) || IsIdentifierChar(after))
                continue;

            return i;
        }

        return -1;
    }

    /// <summary>
    /// EN: Finds the last top-level keyword occurrence outside quoted literals and identifiers.
    /// PT: Encontra a última ocorrência de palavra-chave em nível de topo fora de literais e identificadores entre aspas.
    /// </summary>
    private static int FindLastTopLevelKeyword(string text, string keyword)
    {
        var current = FindFirstTopLevelKeyword(text, keyword, 0);
        if (current < 0)
            return -1;

        var last = current;
        while (true)
        {
            current = FindFirstTopLevelKeyword(text, keyword, current + keyword.Length);
            if (current < 0)
                return last;

            last = current;
        }
    }

    /// <summary>
    /// EN: Checks whether character can be part of an unquoted identifier for keyword boundary checks.
    /// PT: Verifica se o caractere pode compor um identificador sem aspas para validação de fronteira de palavra-chave.
    /// </summary>
    private static bool IsIdentifierChar(char ch)
        => ch == '_' || char.IsLetterOrDigit(ch);

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
    /// EN: Tries to get the first valid matched row by index without cloning.
    /// PT: Tenta obter a primeira linha válida encontrada pelo índice sem clonar.
    /// </summary>
    private static IReadOnlyDictionary<int, object?>? TryGetFirstMatchedRow(
        ITableMock table,
        IEnumerable<int> matchedIndexes)
    {
        foreach (var index in matchedIndexes)
        {
            if (index >= 0 && index < table.Count)
                return table[index];
        }

        return null;
    }

    /// <summary>
    /// EN: Tries to get an immutable snapshot of the first valid matched row.
    /// PT: Tenta obter um snapshot imutável da primeira linha válida encontrada.
    /// </summary>
    private static IReadOnlyDictionary<int, object?>? TryGetFirstMatchedRowSnapshot(
        ITableMock table,
        IEnumerable<int> matchedIndexes)
    {
        var row = TryGetFirstMatchedRow(table, matchedIndexes);
        return row is null ? null : SnapshotRow(row);
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

    private sealed record OracleReturningIntoClause(
        IReadOnlyList<string> ColumnNames,
        IReadOnlyList<string> ParameterNames);

    /// <summary>
    /// EN: Executes the command and returns a scalar value.
    /// PT: Executa o comando e retorna um valor escalar.
    /// </summary>
    public override object ExecuteScalar()
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(connection, nameof(connection));
        using var _ = connection!.Metrics.BeginAmbientScope();
        using var currentQueryScope = connection.BeginCurrentQueryScope(CommandText);
        if (connection.TryHandleExecuteScalarPrelude(
            CommandType,
            CommandText,
            Parameters,
            static () => new OracleDataReaderMock([[]]),
            normalizeSqlInput: true,
            TryExecuteTransactionControlCommand,
            out var scalar))
        {
            return scalar!;
        }

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
