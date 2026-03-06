namespace DbSqlLikeMem.Db2;
#if NET462
using DB2Parameter = IBM.Data.DB2.iSeries.iDB2Parameter;
#endif
/// <summary>
/// EN: Represents a mock database command used to execute SQL text and stored procedures in memory.
/// PT: Representa um comando de banco de dados simulado usado para executar SQL e procedures em memória.
/// </summary>
public class Db2CommandMock(
    Db2ConnectionMock? connection,
    Db2TransactionMock? transaction = null
    ) : DbCommand, IDb2CommandMock
{
    /// <summary>
    /// EN: Initializes a new command instance without an attached connection or transaction.
    /// PT: Inicializa uma nova instância de comando sem conexão ou transação associada.
    /// </summary>
    public Db2CommandMock() : this(null, null)
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
        set => connection = value as Db2ConnectionMock;
    }

    private readonly Db2DataParameterCollectionMock collectionMock = [];

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
        set => transaction = value as Db2TransactionMock;
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
        => new DB2Parameter();

    /// <summary>
    /// EN: Executes non-query and returns affected rows.
    /// PT: Executa non-consulta e retorna as linhas afetadas.
    /// </summary>
    public override int ExecuteNonQuery()
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(connection, nameof(connection));
        connection!.ClearExecutionPlans();
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(CommandText, nameof(CommandText));

        // 1. Stored Procedure (sem parse SQL)
        if (CommandType == CommandType.StoredProcedure)
        {
            var affected = connection.ExecuteStoredProcedure(CommandText, Parameters);
            connection.SetLastFoundRows(affected);
            return affected;
        }

        return connection.ExecuteNonQueryWithPipeline(
            CommandText.NormalizeString(),
            Parameters,
            allowMerge: true,
            unionUsesSelectMessage: true,
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
        ArgumentNullExceptionCompatible.ThrowIfNull(CommandText, nameof(CommandText));

        if (connection.TryHandleExecuteReaderPrelude(
            CommandType,
            CommandText,
            Parameters,
            static () => new Db2DataReaderMock([[]]),
            normalizeSqlInput: true,
            out var earlyReader,
            out var statements))
        {
            return earlyReader!;
        }
        var executor = AstQueryExecutorFactory.Create(connection.Db.Dialect, connection, Parameters);

        // Parse múltiplo (ex: "SELECT 1; SELECT 2;" ou "BEGIN; SELECT ROW_COUNT();")
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

            connection.DispatchParsedReaderQuery(
                q,
                Parameters,
                executor,
                tables,
                executeMerge: mergeQ => connection.ExecuteMerge(mergeQ, Parameters, connection.Db.Dialect));
        }

        connection.FinalizeReaderExecution(tables, parsedStatementCount);

        return new Db2DataReaderMock(tables);
    }


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
        {
            return reader.GetValue(0);
        }
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
