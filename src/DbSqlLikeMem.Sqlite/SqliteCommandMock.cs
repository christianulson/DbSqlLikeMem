using Microsoft.Data.Sqlite;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;

namespace DbSqlLikeMem.Sqlite;

/// <summary>
/// EN: Represents a mock database command used to execute SQL text and stored procedures in memory.
/// PT: Representa um comando de banco de dados simulado usado para executar SQL e procedures em memória.
/// </summary>
public class SqliteCommandMock(
    SqliteConnectionMock? connection,
    SqliteTransactionMock? transaction = null
    ) : DbCommand, ISqliteCommandMock
{
    /// <summary>
    /// EN: Initializes a new command instance without an attached connection or transaction.
    /// PT: Inicializa uma nova instância de comando sem conexão ou transação associada.
    /// </summary>
    public SqliteCommandMock() : this(null, null)
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
        set => connection = value as SqliteConnectionMock;
    }

    private readonly SqliteDataParameterCollectionMock collectionMock = [];

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
        set => transaction = value as SqliteTransactionMock;
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
        => new SqliteParameter();

    /// <summary>
    /// EN: Summary for ExecuteNonQuery.
    /// PT: Resumo para ExecuteNonQuery.
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

        // 3. Parse via AST para comandos DML (Insert, Update, Delete)
        var query = SqlQueryParser.Parse(sqlRaw, connection!.Db.Dialect);

        return query switch
        {
            SqlInsertQuery insertQ => connection.ExecuteInsert(insertQ, Parameters, connection.Db.Dialect),
            SqlUpdateQuery updateQ => connection.ExecuteUpdateSmart(updateQ, Parameters, connection.Db.Dialect),
            SqlDeleteQuery deleteQ => connection.ExecuteDeleteSmart(deleteQ, Parameters, connection.Db.Dialect),
            SqlCreateViewQuery cv => connection.ExecuteCreateView(cv, Parameters, connection.Db.Dialect),
            SqlDropViewQuery dropViewQ => connection.ExecuteDropView(dropViewQ, Parameters, connection.Db.Dialect),
            SqlSelectQuery _ => throw new InvalidOperationException(SqlExceptionMessages.UseExecuteReaderForSelect()),
            SqlUnionQuery _ => throw new InvalidOperationException(SqlExceptionMessages.UseExecuteReaderForSelectUnion()),
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
            return new SqliteDataReaderMock([[]]);
        }

        var sql = CommandText.NormalizeString();

        // Erro CA1847 e CA1307: Substituído por Contains com char ou StringComparison
        if (sql.StartsWith("CALL", StringComparison.OrdinalIgnoreCase))
        {
            connection!.ExecuteCall(sql, Parameters);
            return new SqliteDataReaderMock([[]]);
        }

        var executor = AstQueryExecutorFactory.Create(connection!.Db.Dialect, connection, Parameters);

        // Parse Multiplo (ex: "SELECT 1; SELECT 2;" ou "CREATE TEMPORARY TABLE ...; SELECT ...")
        var queries = SqlQueryParser.ParseMulti(sql, connection.Db.Dialect).ToList();

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

        return new SqliteDataReaderMock(tables);
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
        {
            return reader.GetValue(0);
        }
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
