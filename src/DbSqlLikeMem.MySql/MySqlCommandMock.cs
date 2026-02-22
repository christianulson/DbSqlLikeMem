namespace DbSqlLikeMem.MySql;

/// <summary>
/// EN: Mock command for MySQL connections.
/// PT: Comando simulado para conexões MySQL.
/// </summary>
public class MySqlCommandMock(
    MySqlConnectionMock? connection,
    MySqlTransactionMock? transaction = null
    ) : DbCommand
{
    /// <summary>
    /// Contructor
    /// </summary>
    public MySqlCommandMock()
        : this(null, null)
    {
    }

    private bool disposedValue;

    /// <summary>
    /// Gets or sets the command text.
    /// Obtém ou define o texto do comando.
    /// </summary>
    [AllowNull]
    public override string CommandText { get; set; } = string.Empty;

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override int CommandTimeout { get; set; }

    /// <summary>
    /// Auto-generated summary.
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
            if (string.Compare(CommandText.Substring(0, 6), "INSERT", StringComparison.OrdinalIgnoreCase) == 0)
            {
                var tk = new SqlTokenizer(CommandText, connection!.Db.Dialect);
                var mySqlTokenizer = tk.Tokenize();
                //string text = Connection.driver.Property("sql_mode").ToUpperInvariant();
                //mySqlTokenizer.AnsiQuotes = text.IndexOf("ANSI_QUOTES") != -1;
                //mySqlTokenizer.BackslashEscapes = text.IndexOf("NO_BACKSLASH_ESCAPES") == -1;
                var i = 0;
                for (string text2 = mySqlTokenizer[i].Text; text2 != null; text2 = mySqlTokenizer[i].Text)
                {
                    if (string.Equals(mySqlTokenizer[i].Text, "VALUES", StringComparison.OrdinalIgnoreCase)
                        && mySqlTokenizer[i].Kind != SqlTokenKind.Symbol)
                    {
                        i++;
                        text2 = mySqlTokenizer[i].Text;
                        int num = 1;
                        while (text2 != null)
                        {
                            BatchableCommandText += text2;
                            i++;
                            text2 = mySqlTokenizer[i].Text;
                            if (text2 == "(")
                            {
                                num++;
                            }
                            else if (text2 == ")")
                            {
                                num--;
                            }

                            if (num == 0)
                            {
                                break;
                            }
                        }

                        if (text2 != null)
                        {
                            BatchableCommandText += text2;
                        }
                        i++;
                        text2 = mySqlTokenizer[i].Text;
                        if (text2 != null && (text2 == "," || string.Equals(text2, "ON", StringComparison.OrdinalIgnoreCase)))
                        {
                            BatchableCommandText = null;
                            break;
                        }
                    }
                }
            }
            else
            {
                BatchableCommandText = CommandText;
            }
        }

        return BatchableCommandText;
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
    /// Auto-generated summary.
    /// </summary>
    public override UpdateRowSource UpdatedRowSource { get; set; }
    
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override bool DesignTimeVisible { get; set; }

    /// <summary>
    /// Auto-generated summary.
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
    /// Auto-generated summary.
    /// </summary>
    public override int ExecuteNonQuery()
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(connection, nameof(connection));
        connection!.ClearExecutionPlans();
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(CommandText, nameof(CommandText));

        // 1. Stored Procedure (sem parse SQL)
        if (CommandType == CommandType.StoredProcedure)
        {
            return connection!.ExecuteStoredProcedure(CommandText, Parameters);
        }

        var sqlRaw = CommandText.Trim();

        if (TryExecuteTransactionControlCommand(sqlRaw, out var transactionControlResult))
            return transactionControlResult;

        // 2. Comandos especiais que talvez o Parser ainda não suporte nativamente (DDL, CALL)
        if (sqlRaw.StartsWith("call ", StringComparison.OrdinalIgnoreCase))
        {
            return connection!.ExecuteCall(sqlRaw, Parameters);
        }

        if (sqlRaw.StartsWith("create temporary table", StringComparison.OrdinalIgnoreCase) ||
            sqlRaw.StartsWith("create temp table", StringComparison.OrdinalIgnoreCase))
        {
            var q = SqlQueryParser.Parse(sqlRaw, connection!.Db.Dialect);
            if (q is not SqlCreateTemporaryTableQuery ct)
                throw new InvalidOperationException(SqlExceptionMessages.InvalidCreateTemporaryTableStatement());
            return connection.ExecuteCreateTemporaryTableAsSelect(ct, Parameters, connection.Db.Dialect);
        }

        if (sqlRaw.StartsWith("create view", StringComparison.OrdinalIgnoreCase) ||
            sqlRaw.StartsWith("create or replace view", StringComparison.OrdinalIgnoreCase))
        {
            var q = SqlQueryParser.Parse(sqlRaw, connection!.Db.Dialect);
            if (q is not SqlCreateViewQuery cv)
                throw new InvalidOperationException(SqlExceptionMessages.InvalidCreateViewStatement());
            return connection.ExecuteCreateView(cv, Parameters, connection.Db.Dialect);
        }

        if (sqlRaw.StartsWith("create table", StringComparison.OrdinalIgnoreCase))
        {
            return connection!.ExecuteCreateTableAsSelect(sqlRaw, Parameters, connection!.Db.Dialect);
        }

        if (sqlRaw.StartsWith("drop view", StringComparison.OrdinalIgnoreCase))
        {
            return ExecuteDropView(sqlRaw);
        }

        if (!connection!.Db.Dialect.SupportsDeleteWithoutFrom && IsDeleteMissingFrom(sqlRaw))
            throw new InvalidOperationException(SqlExceptionMessages.InvalidDeleteExpectedFromKeyword());

        // 3. Parse via AST para comandos DML (Insert, Update, Delete)
        var query = SqlQueryParser.Parse(sqlRaw, connection.Db.Dialect);

        return query switch
        {
            SqlInsertQuery insertQ => connection.ExecuteInsert(insertQ, Parameters, connection.Db.Dialect),
            SqlUpdateQuery updateQ => connection.ExecuteUpdateSmart(updateQ, Parameters, connection.Db.Dialect),
            SqlDeleteQuery deleteQ => connection.ExecuteDeleteSmart(deleteQ, Parameters, connection.Db.Dialect),
            SqlCreateViewQuery cv => connection.ExecuteCreateView(cv, Parameters, connection.Db.Dialect),
            SqlDropViewQuery dropViewQ => connection.ExecuteDropView(dropViewQ, Parameters, connection.Db.Dialect),
            SqlSelectQuery _ => throw new InvalidOperationException(SqlExceptionMessages.UseExecuteReaderForSelect()),
            _ => throw SqlUnsupported.ForCommandType(connection!.Db.Dialect, "ExecuteNonQuery", query.GetType())
        };
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

    private int ExecuteDropView(string sqlRaw)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(connection, nameof(connection));

        var parts = sqlRaw
            .TrimEnd(';')
            .Split(' ')
            .Select(_=>_.Trim())
            .ToArray();

        if (parts.Length < 3 || !parts[0].Equals("DROP", StringComparison.OrdinalIgnoreCase) || !parts[1].Equals("VIEW", StringComparison.OrdinalIgnoreCase))
            throw new InvalidOperationException(SqlExceptionMessages.InvalidDropViewStatement());

        var i = 2;
        var ifExists = false;
        if (parts.Length > 4
            && parts[i].Equals("IF", StringComparison.OrdinalIgnoreCase)
            && parts[i + 1].Equals("EXISTS", StringComparison.OrdinalIgnoreCase))
        {
            ifExists = true;
            i += 2;
        }

        if (i >= parts.Length)
            throw new InvalidOperationException(SqlExceptionMessages.InvalidDropViewStatement());

        var viewName = parts[i].Trim().Trim('`', '"').NormalizeName();
        connection!.DropView(viewName, ifExists);
        return 0;
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

        if (CommandType == CommandType.StoredProcedure)
        {
            connection!.ExecuteStoredProcedure(CommandText, Parameters);
            return new MySqlDataReaderMock([[]]);
        }

        var sql = CommandText.NormalizeString();

        // Erro CA1847 e CA1307: Substituído por Contains com char ou StringComparison
        if (sql.StartsWith("CALL", StringComparison.OrdinalIgnoreCase))
        {
            connection!.ExecuteCall(sql, Parameters);
            return new MySqlDataReaderMock([[]]);
        }

        var executor = AstQueryExecutorFactory.Create(connection!.Db.Dialect, connection, Parameters);

        // Correção do erro de Contains e CA1847/CA1307


        // Parse Multiplo (ex: "SELECT 1; SELECT 2;" ou "CREATE TEMPORARY TABLE ...; SELECT ...")
        var tables = new List<TableResultMock>();
        var hasAnyQuery = false;

        foreach (var q in SqlQueryParser.ParseMulti(sql, connection.Db.Dialect, Parameters))
        {
            hasAnyQuery = true;

            switch (q)
            {
                case SqlCreateTemporaryTableQuery ct:
                    connection.ExecuteCreateTemporaryTableAsSelect(ct, Parameters, connection.Db.Dialect);
                    break;

                case SqlCreateViewQuery cv:
                    connection.ExecuteCreateView(cv, Parameters, connection.Db.Dialect);
                    break;
                case SqlDropViewQuery dropViewQ:
                    connection.ExecuteDropView(dropViewQ, Parameters, connection.Db.Dialect);
                    break;

                case SqlInsertQuery insertQ:
                    connection.ExecuteInsert(insertQ, Parameters, connection.Db.Dialect);
                    break;

                case SqlUpdateQuery updateQ:
                    connection.ExecuteUpdateSmart(updateQ, Parameters, connection.Db.Dialect);
                    break;

                case SqlDeleteQuery deleteQ:
                    connection.ExecuteDeleteSmart(deleteQ, Parameters, connection.Db.Dialect);
                    break;

                case SqlSelectQuery selectQ:
                    tables.Add(executor.ExecuteSelect(selectQ));
                    break;

                case SqlUnionQuery unionQ:
                    tables.Add(executor.ExecuteUnion(unionQ.Parts, unionQ.AllFlags, unionQ.OrderBy, unionQ.RowLimit, unionQ.RawSql));
                    break;

                default:
                    throw SqlUnsupported.ForCommandType(connection!.Db.Dialect, "ExecuteReader", q.GetType());
            }
        }

        if (tables.Count == 0 && hasAnyQuery)
            throw new InvalidOperationException(SqlExceptionMessages.ExecuteReaderWithoutSelectQuery());

        connection.Metrics.Selects += tables.Sum(t => t.Count);

        return new MySqlDataReaderMock(tables);
    }

    /// <summary>
    /// Auto-generated summary.
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
    /// Auto-generated summary.
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
}
