using System.Diagnostics.CodeAnalysis;
using Microsoft.Data.SqlClient;


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
    /// EN: Summary for DbConnection.
    /// PT: Resumo para DbConnection.
    /// </summary>
    protected override DbConnection? DbConnection
    {
        get => connection;
        set => connection = value as SqlServerConnectionMock;
    }

    private readonly SqlServerDataParameterCollectionMock collectionMock = [];
    /// <summary>
    /// EN: Summary for member.
    /// PT: Resumo para member.
    /// </summary>
    protected override DbParameterCollection DbParameterCollection => collectionMock;

    /// <summary>
    /// EN: Summary for DbTransaction.
    /// PT: Resumo para DbTransaction.
    /// </summary>
    protected override DbTransaction? DbTransaction
    {
        get => transaction!;
        set => transaction = value as SqlServerTransactionMock;
    }

    /// <summary>
    /// EN: Summary for member.
    /// PT: Resumo para member.
    /// </summary>
    public override UpdateRowSource UpdatedRowSource { get; set; }
    /// <summary>
    /// EN: Summary for member.
    /// PT: Resumo para member.
    /// </summary>
    public override bool DesignTimeVisible { get; set; }

    /// <summary>
    /// EN: Summary for Cancel.
    /// PT: Resumo para Cancel.
    /// </summary>
    public override void Cancel() => DbTransaction?.Rollback();

    /// <summary>
    /// EN: Summary for CreateDbParameter.
    /// PT: Resumo para CreateDbParameter.
    /// </summary>
    protected override DbParameter CreateDbParameter()
        => new SqlParameter();

    /// <summary>
    /// EN: Summary for ExecuteNonQuery.
    /// PT: Resumo para ExecuteNonQuery.
    /// </summary>
    public override int ExecuteNonQuery()
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(connection, nameof(connection));
        connection!.ClearExecutionPlans();
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(CommandText, nameof(CommandText));

        if (CommandType == CommandType.StoredProcedure)
            return connection!.ExecuteStoredProcedure(CommandText, Parameters);

        var sqlRaw = CommandText.Trim();

        if (TryExecuteTransactionControlCommand(sqlRaw, out var transactionControlResult))
            return transactionControlResult;

        // Mantém atalhos existentes (CALL / CREATE TABLE AS SELECT) por compatibilidade do engine atual
        if (sqlRaw.StartsWith("call ", StringComparison.OrdinalIgnoreCase))
            return connection!.ExecuteCall(sqlRaw, Parameters);

        if (sqlRaw.StartsWith("create table", StringComparison.OrdinalIgnoreCase))
            return connection!.ExecuteCreateTableAsSelect(sqlRaw, Parameters, connection!.Db.Dialect);

        var query = SqlQueryParser.Parse(sqlRaw, connection!.Db.Dialect);

        return query switch
        {
            SqlInsertQuery insertQ => connection.ExecuteInsert(insertQ, Parameters, connection.Db.Dialect),
            SqlUpdateQuery updateQ => connection.ExecuteUpdateSmart(updateQ, Parameters, connection.Db.Dialect),
            SqlDeleteQuery deleteQ => connection.ExecuteDeleteSmart(deleteQ, Parameters, connection.Db.Dialect),
            SqlCreateTemporaryTableQuery tempQ => connection.ExecuteCreateTemporaryTableAsSelect(tempQ, Parameters, connection.Db.Dialect),
            SqlCreateViewQuery viewQ => connection.ExecuteCreateView(viewQ, Parameters, connection.Db.Dialect),
            SqlDropViewQuery dropViewQ => connection.ExecuteDropView(dropViewQ, Parameters, connection.Db.Dialect),
            SqlMergeQuery mergeQ => connection.ExecuteMerge(mergeQ, Parameters, connection.Db.Dialect),
            SqlSelectQuery _ => throw new InvalidOperationException(SqlExceptionMessages.UseExecuteReaderForSelect()),
            _ => throw SqlUnsupported.ForCommandType(connection!.Db.Dialect, "ExecuteNonQuery", query.GetType())
        };
    }

    /// <summary>
    /// EN: Summary for ExecuteDbDataReader.
    /// PT: Resumo para ExecuteDbDataReader.
    /// </summary>
    protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(connection, nameof(connection));
        connection!.ClearExecutionPlans();
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(CommandText, nameof(CommandText));

        if (CommandType == CommandType.StoredProcedure)
        {
            connection!.ExecuteStoredProcedure(CommandText, Parameters);
            return new SqlServerDataReaderMock([[]]);
        }

        var sql = CommandText.NormalizeString();

        if (sql.StartsWith("CALL", StringComparison.OrdinalIgnoreCase))
        {
            connection!.ExecuteCall(sql, Parameters);
            return new SqlServerDataReaderMock([[]]);
        }

        var executor = new SqlServerAstQueryExecutor(connection!, Parameters);

        var queries = SqlQueryParser.ParseMulti(sql, connection!.Db.Dialect, Parameters).ToList();
        var tables = new List<TableResultMock>();

        foreach (var query in queries)
        {
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
                    connection.ExecuteUpdateSmart(updateQ, Parameters, connection.Db.Dialect);
                    break;
                case SqlDeleteQuery deleteQ:
                    connection.ExecuteDeleteSmart(deleteQ, Parameters, connection.Db.Dialect);
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
                case SqlMergeQuery mergeQ:
                    connection.ExecuteMerge(mergeQ, Parameters, connection.Db.Dialect);
                    break;
                default:
                    throw SqlUnsupported.ForCommandType(connection!.Db.Dialect, "ExecuteReader", query.GetType());
            }
        }

        if (tables.Count == 0 && queries.Count > 0)
            throw new InvalidOperationException(SqlExceptionMessages.ExecuteReaderWithoutSelectQuery());
        connection.Metrics.Selects += tables.Sum(t => t.Count);

        return new SqlServerDataReaderMock(tables);
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
    /// EN: Summary for ExecuteScalar.
    /// PT: Resumo para ExecuteScalar.
    /// </summary>
    public override object ExecuteScalar()
    {
        using var reader = ExecuteReader();
        if (reader.Read())
            return reader.GetValue(0);
        return DBNull.Value;
    }

    /// <summary>
    /// EN: Summary for Prepare.
    /// PT: Resumo para Prepare.
    /// </summary>
    public override void Prepare() { }

    /// <summary>
    /// EN: Summary for Dispose.
    /// PT: Resumo para Dispose.
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
