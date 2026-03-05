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
        var executor = AstQueryExecutorFactory.Create(connection!.Db.Dialect, connection, Parameters);

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

            var q = SqlQueryParser.Parse(sqlRaw, connection.Db.Dialect, Parameters);
            parsedStatementCount++;

            connection.DispatchParsedReaderQuery(q, Parameters, executor, tables);
        }

        connection.FinalizeReaderExecution(tables, parsedStatementCount);

        return new MySqlDataReaderMock(tables);
    }

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
}
