namespace DbSqlLikeMem.Db2;

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
        ArgumentNullException.ThrowIfNull(connection);
        connection.ClearExecutionPlans();
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(CommandText, nameof(CommandText));

        // 1. Stored Procedure (sem parse SQL)
        if (CommandType == CommandType.StoredProcedure)
        {
            return connection.ExecuteStoredProcedure(CommandText, Parameters);
        }

        var sqlRaw = CommandText.Trim();

        if (TryExecuteTransactionControlCommand(sqlRaw, out var transactionControlResult))
            return transactionControlResult;

        // 2. Comandos especiais que talvez o Parser ainda não suporte nativamente (DDL, CALL)
        if (sqlRaw.StartsWith("call ", StringComparison.OrdinalIgnoreCase))
        {
            return connection.ExecuteCall(sqlRaw, Parameters);
        }

        if (sqlRaw.StartsWith("create temporary table", StringComparison.OrdinalIgnoreCase) ||
            sqlRaw.StartsWith("create temp table", StringComparison.OrdinalIgnoreCase))
        {
            var q = SqlQueryParser.Parse(sqlRaw, connection.Db.Dialect);
            if (q is not SqlCreateTemporaryTableQuery ct)
                throw new InvalidOperationException(SqlExceptionMessages.InvalidCreateTemporaryTableStatement());
            return connection.ExecuteCreateTemporaryTableAsSelect(ct, Parameters, connection.Db.Dialect);
        }

        if (sqlRaw.StartsWith("create view", StringComparison.OrdinalIgnoreCase) ||
            sqlRaw.StartsWith("create or replace view", StringComparison.OrdinalIgnoreCase))
        {
            var q = SqlQueryParser.Parse(sqlRaw, connection.Db.Dialect);
            if (q is not SqlCreateViewQuery cv)
                throw new InvalidOperationException(SqlExceptionMessages.InvalidCreateViewStatement());
            return connection.ExecuteCreateView(cv, Parameters, connection.Db.Dialect);
        }

        if (sqlRaw.StartsWith("create table", StringComparison.OrdinalIgnoreCase))
        {
            return connection.ExecuteCreateTableAsSelect(sqlRaw, Parameters, connection.Db.Dialect);
        }

        if (sqlRaw.StartsWith("drop view", StringComparison.OrdinalIgnoreCase))
        {
            return ExecuteDropView(sqlRaw);
        }

        // 3. Parse via AST para comandos DML (Insert, Update, Delete)
        var query = SqlQueryParser.Parse(sqlRaw, connection.Db.Dialect);

        return query switch
        {
            SqlInsertQuery insertQ => connection.ExecuteInsert(insertQ, Parameters, connection.Db.Dialect),
            SqlUpdateQuery updateQ => connection.ExecuteUpdateSmart(updateQ, Parameters, connection.Db.Dialect),
            SqlDeleteQuery deleteQ => connection.ExecuteDeleteSmart(deleteQ, Parameters, connection.Db.Dialect),
            SqlMergeQuery mergeQ => connection.ExecuteMerge(mergeQ, Parameters, connection.Db.Dialect),
            SqlCreateViewQuery cv => connection.ExecuteCreateView(cv, Parameters, connection.Db.Dialect),
            SqlDropViewQuery dropViewQ => connection.ExecuteDropView(dropViewQ, Parameters, connection.Db.Dialect),
            SqlSelectQuery _ => throw new InvalidOperationException(SqlExceptionMessages.UseExecuteReaderForSelect()),
            SqlUnionQuery _ => throw new InvalidOperationException(SqlExceptionMessages.UseExecuteReaderForSelectUnion()),
            _ => throw SqlUnsupported.ForCommandType(connection!.Db.Dialect, "ExecuteNonQuery", query.GetType())
        };
    }

    private int ExecuteDropView(string sqlRaw)
    {
        ArgumentNullException.ThrowIfNull(connection);

        var parts = sqlRaw
            .TrimEnd(';')
            .Split(' ', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

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
        connection.DropView(viewName, ifExists);
        return 0;
    }

    /// <summary>
    /// EN: Executes the command and returns a data reader.
    /// PT: Executa o comando e retorna um leitor de dados.
    /// </summary>
    protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
    {
        ArgumentNullException.ThrowIfNull(connection);
        connection.ClearExecutionPlans();
        ArgumentNullException.ThrowIfNull(CommandText);

        if (CommandType == CommandType.StoredProcedure)
        {
            connection.ExecuteStoredProcedure(CommandText, Parameters);
            return new Db2DataReaderMock([[]]);
        }

        var sql = CommandText.NormalizeString();

        // Erro CA1847 e CA1307: Substituído por Contains com char ou StringComparison
        if (sql.StartsWith("CALL", StringComparison.OrdinalIgnoreCase))
        {
            connection.ExecuteCall(sql, Parameters);
            return new Db2DataReaderMock([[]]);
        }

        var executor = AstQueryExecutorFactory.Create(connection.Db.Dialect, connection, Parameters);

        // Parse Multiplo (ex: "SELECT 1; SELECT 2;" ou "CREATE TEMPORARY TABLE ...; SELECT ...")
        var queries = SqlQueryParser.ParseMulti(sql, connection.Db.Dialect, Parameters).ToList();

        var tables = new List<TableResultMock>();

        foreach (var q in queries)
        {
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

                case SqlMergeQuery mergeQ:
                    connection.ExecuteMerge(mergeQ, Parameters, connection.Db.Dialect);
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

        if (tables.Count == 0 && queries.Count > 0)
            throw new InvalidOperationException(SqlExceptionMessages.ExecuteReaderWithoutSelectQuery());

        connection.Metrics.Selects += tables.Sum(t => t.Count);

        return new Db2DataReaderMock(tables);
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
