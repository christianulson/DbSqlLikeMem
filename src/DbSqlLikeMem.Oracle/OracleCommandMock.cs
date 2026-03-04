using System.Diagnostics.CodeAnalysis;
using Oracle.ManagedDataAccess.Client;
using System.Text;
using System.Text.RegularExpressions;

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

        var effectiveSql = sqlRaw;
        OracleReturningIntoClause? returningIntoClause = null;
        if (TryExtractOracleReturningIntoClause(sqlRaw, out var rewrittenSql, out var clause))
        {
            effectiveSql = rewrittenSql;
            returningIntoClause = clause;
        }

        var query = SqlQueryParser.Parse(effectiveSql, connection!.Db.Dialect);

        if (returningIntoClause is not null)
            return ExecuteNonQueryWithReturningInto(query, returningIntoClause);

        return query switch
        {
            SqlInsertQuery insertQ => connection.ExecuteInsert(insertQ, Parameters, connection.Db.Dialect),
            SqlUpdateQuery updateQ => connection.ExecuteUpdateSmart(updateQ, Parameters, connection.Db.Dialect),
            SqlDeleteQuery deleteQ => connection.ExecuteDeleteSmart(deleteQ, Parameters, connection.Db.Dialect),
            SqlMergeQuery mergeQ => connection.ExecuteMerge(mergeQ, Parameters, connection.Db.Dialect),
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
            return new OracleDataReaderMock([[]]);
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
            return new OracleDataReaderMock([[]]);
        }
        var executor = new OracleAstQueryExecutor(connection!, Parameters);
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
                    connection.ExecuteInsert(insertQ, Parameters, connection.Db.Dialect);
                    break;
                case SqlUpdateQuery updateQ:
                    connection.ExecuteUpdate(updateQ, Parameters);
                    break;
                case SqlDeleteQuery deleteQ:
                    connection.ExecuteDelete(deleteQ, Parameters);
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

        return new OracleDataReaderMock(tables);
    }

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
    /// EN: Executes DML and populates Oracle RETURNING INTO output parameters.
    /// PT: Executa DML e preenche parâmetros de saída de RETURNING INTO no Oracle.
    /// </summary>
    private int ExecuteNonQueryWithReturningInto(
        SqlQueryBase query,
        OracleReturningIntoClause clause)
    {
        var affectedRows = query switch
        {
            SqlInsertQuery insertQuery => ExecuteInsertWithReturningInto(insertQuery, clause, out _),
            SqlUpdateQuery updateQuery => ExecuteUpdateWithReturningInto(updateQuery, clause, out _),
            SqlDeleteQuery deleteQuery => ExecuteDeleteWithReturningInto(deleteQuery, clause, out _),
            _ => throw new NotSupportedException("RETURNING INTO is only supported for INSERT/UPDATE/DELETE in ExecuteNonQuery.")
        };

        return affectedRows;
    }

    /// <summary>
    /// EN: Executes INSERT for RETURNING INTO and captures inserted row snapshots.
    /// PT: Executa INSERT para RETURNING INTO e captura snapshots das linhas inseridas.
    /// </summary>
    private int ExecuteInsertWithReturningInto(
        SqlInsertQuery query,
        OracleReturningIntoClause clause,
        out IReadOnlyList<IReadOnlyDictionary<int, object?>> affectedRows)
    {
        if (!TryResolveTargetTable(query.Table, out var table))
            throw new InvalidOperationException("RETURNING INTO requires a valid target table.");

        var beforeCount = table.Count;
        var affected = connection!.ExecuteInsert(query, Parameters, connection.Db.Dialect);
        var insertedRows = Math.Max(0, table.Count - beforeCount);
        affectedRows = Enumerable.Range(beforeCount, insertedRows)
            .Select(i => SnapshotRow(table[i]))
            .ToList();
        PopulateReturningIntoParameters(clause, table, affectedRows);
        return affected;
    }

    /// <summary>
    /// EN: Executes UPDATE for RETURNING INTO and captures updated row snapshots.
    /// PT: Executa UPDATE para RETURNING INTO e captura snapshots das linhas atualizadas.
    /// </summary>
    private int ExecuteUpdateWithReturningInto(
        SqlUpdateQuery query,
        OracleReturningIntoClause clause,
        out IReadOnlyList<IReadOnlyDictionary<int, object?>> affectedRows)
    {
        if (!TryResolveTargetTable(query.Table, out var table))
            throw new InvalidOperationException("RETURNING INTO requires a valid target table.");

        var matchedIndexes = MatchRowIndexes(table, query.WhereRaw, query.RawSql);
        var affected = connection!.ExecuteUpdate(query, Parameters);
        affectedRows = matchedIndexes
            .Where(i => i >= 0 && i < table.Count)
            .Select(i => SnapshotRow(table[i]))
            .ToList();
        PopulateReturningIntoParameters(clause, table, affectedRows);
        return affected;
    }

    /// <summary>
    /// EN: Executes DELETE for RETURNING INTO and captures deleted row snapshots.
    /// PT: Executa DELETE para RETURNING INTO e captura snapshots das linhas excluídas.
    /// </summary>
    private int ExecuteDeleteWithReturningInto(
        SqlDeleteQuery query,
        OracleReturningIntoClause clause,
        out IReadOnlyList<IReadOnlyDictionary<int, object?>> affectedRows)
    {
        if (!TryResolveTargetTable(query.Table, out var table))
            throw new InvalidOperationException("RETURNING INTO requires a valid target table.");

        var matchedIndexes = MatchRowIndexes(table, query.WhereRaw, query.RawSql);
        var snapshots = matchedIndexes
            .Where(i => i >= 0 && i < table.Count)
            .Select(i => SnapshotRow(table[i]))
            .ToList();
        var affected = connection!.ExecuteDelete(query, Parameters);
        affectedRows = snapshots;
        PopulateReturningIntoParameters(clause, table, affectedRows);
        return affected;
    }

    /// <summary>
    /// EN: Populates Oracle output parameters from first affected row according to RETURNING INTO mapping.
    /// PT: Preenche parâmetros de saída do Oracle a partir da primeira linha afetada conforme mapeamento RETURNING INTO.
    /// </summary>
    private void PopulateReturningIntoParameters(
        OracleReturningIntoClause clause,
        ITableMock table,
        IReadOnlyList<IReadOnlyDictionary<int, object?>> affectedRows)
    {
        var sourceRow = affectedRows.FirstOrDefault();
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

        var match = Regex.Match(
            sql,
            @"^(?<stmt>[\s\S]*?)\bRETURNING\b(?<cols>[\s\S]*?)\bINTO\b(?<pars>[\s\S]*)$",
            RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);
        if (!match.Success)
            return false;

        var cols = SplitTopLevelComma(match.Groups["cols"].Value)
            .Select(c => c.Trim())
            .Where(c => !string.IsNullOrWhiteSpace(c))
            .ToList();
        var pars = SplitTopLevelComma(match.Groups["pars"].Value)
            .Select(p => p.Trim().TrimEnd(';'))
            .Where(p => !string.IsNullOrWhiteSpace(p))
            .ToList();
        if (cols.Count == 0 || cols.Count != pars.Count)
            throw new InvalidOperationException("RETURNING INTO must map the same number of columns and parameters.");

        rewrittenSql = match.Groups["stmt"].Value.TrimEnd();
        clause = new OracleReturningIntoClause(cols, pars);
        return true;
    }

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
        out ITableMock table)
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
