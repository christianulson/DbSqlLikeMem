using DbCommand = System.Data.Common.DbCommand;
using DbConnection = System.Data.Common.DbConnection;
using DbConnectionStringBuilder = System.Data.Common.DbConnectionStringBuilder;
using DbDataAdapter = System.Data.Common.DbDataAdapter;
using DbDataReader = System.Data.Common.DbDataReader;
using DbParameter = System.Data.Common.DbParameter;
using DbParameterCollection = System.Data.Common.DbParameterCollection;
using DbProviderFactory = System.Data.Common.DbProviderFactory;
using DbTransaction = System.Data.Common.DbTransaction;
using DbSqlLikeMem.SqlServer;
using Microsoft.Data.SqlClient;
#if NET6_0_OR_GREATER
using DbBatch = System.Data.Common.DbBatch;
using DbBatchCommand = System.Data.Common.DbBatchCommand;
using DbBatchCommandCollection = System.Data.Common.DbBatchCommandCollection;
#endif

namespace DbSqlLikeMem.SqlAzure;

/// <summary>
/// EN: SQL Azure compatibility levels used by the SQL Azure provider emulation.
/// PT: Niveis de compatibilidade do SQL Azure usados pela emulacao do provedor SQL Azure.
/// </summary>
public static class SqlAzureDbCompatibilityLevels
{
    public const int SqlServer2008 = 100;
    public const int SqlServer2012 = 110;
    public const int SqlServer2014 = 120;
    public const int SqlServer2016 = 130;
    public const int SqlServer2017 = 140;
    public const int SqlServer2019 = 150;
    public const int SqlServer2022 = 160;
    public const int SqlServer2025 = 170;

    public const int Default = SqlServer2022;

    public static IEnumerable<int> Versions()
    {
        yield return SqlServer2008;
        yield return SqlServer2012;
        yield return SqlServer2014;
        yield return SqlServer2016;
        yield return SqlServer2017;
        yield return SqlServer2019;
        yield return SqlServer2022;
        yield return SqlServer2025;
    }
}

/// <summary>
/// EN: Alias helper exposing SQL Azure compatibility levels through a Versions() API.
/// PT: Helper de alias que expõe níveis de compatibilidade do SQL Azure por meio de uma API Versions().
/// </summary>
public static class SqlAzureDbVersions
{
    public static IEnumerable<int> Versions() => SqlAzureDbCompatibilityLevels.Versions();
}

/// <summary>
/// EN: In-memory database mock configured for SQL Azure compatibility levels.
/// PT: Banco simulado em memoria configurado para niveis de compatibilidade do SQL Azure.
/// </summary>
public class SqlAzureDbMock : SqlServerDbMock
{
    public SqlAzureDbMock(int? version = null) : base(version ?? SqlAzureDbCompatibilityLevels.Default)
    {
    }

    protected override SchemaMock NewSchema(
        string schemaName,
        IDictionary<string, (IEnumerable<Col> columns, IEnumerable<Dictionary<int, object?>>? rows)>? tables = null)
        => new SqlAzureSchemaMock(schemaName, this, tables);
}

/// <summary>
/// EN: Schema mock for SQL Azure databases.
/// PT: Esquema simulado para bancos SQL Azure.
/// </summary>
public class SqlAzureSchemaMock(
    string schemaName,
    SqlAzureDbMock db,
    IDictionary<string, (IEnumerable<Col> columns, IEnumerable<Dictionary<int, object?>>? rows)>? tables = null
    ) : SqlServerSchemaMock(schemaName, db, tables)
{
    protected override TableMock NewTable(
        string tableName,
        IEnumerable<Col> columns,
        IEnumerable<Dictionary<int, object?>>? rows = null)
        => new SqlAzureTableMock(tableName, this, columns, rows);
}

/// <summary>
/// EN: Table mock specialized for SQL Azure schema operations.
/// PT: Tabela simulada especializada para operacoes de esquema no SQL Azure.
/// </summary>
public class SqlAzureTableMock(
    string tableName,
    SqlAzureSchemaMock schema,
    IEnumerable<Col> columns,
    IEnumerable<Dictionary<int, object?>>? rows = null
    ) : SqlServerTableMock(tableName, schema, columns, rows)
{
    public override Exception UnknownColumn(string columnName)
        => SqlAzureExceptionFactory.UnknownColumn(columnName);

    public override Exception DuplicateKey(string tbl, string key, object? val)
        => SqlAzureExceptionFactory.DuplicateKey(tbl, key, val);

    public override Exception ColumnCannotBeNull(string col)
        => SqlAzureExceptionFactory.ColumnCannotBeNull(col);

    public override Exception ForeignKeyFails(string col, string refTbl)
        => SqlAzureExceptionFactory.ForeignKeyFails(col, refTbl);

    public override Exception ReferencedRow(string tbl)
        => SqlAzureExceptionFactory.ReferencedRow(tbl);
}

/// <summary>
/// EN: Represents SQL Azure connection mock.
/// PT: Representa conexao simulada do SQL Azure.
/// </summary>
public class SqlAzureConnectionMock : SqlServerConnectionMock
{
    public SqlAzureConnectionMock(
       SqlAzureDbMock? db = null,
       string? defaultDatabase = null
   ) : base(db ?? new SqlAzureDbMock(), defaultDatabase)
    {
        _serverVersion = $"SQL Azure {Db.Version}";
    }

    protected override DbTransaction CreateTransaction(IsolationLevel isolationLevel)
        => new SqlServerTransactionMock(this, isolationLevel);

    protected override DbCommand CreateDbCommandCore(DbTransaction? transaction)
        => new SqlAzureCommandMock(this, transaction as SqlServerTransactionMock);

    internal override Exception NewException(string message, int code)
        => new SqlAzureMockException(message, code);
}

/// <summary>
/// EN: Represents SQL Azure command mock.
/// PT: Representa comando simulado do SQL Azure.
/// </summary>
public class SqlAzureCommandMock(
    SqlAzureConnectionMock? connection,
    SqlServerTransactionMock? transaction = null
    ) : SqlServerCommandMock(connection, transaction)
{
    private readonly SqlAzureDataParameterCollectionMock collectionMock = [];

    public SqlAzureCommandMock() : this(null, null)
    {
    }

    protected override DbParameterCollection DbParameterCollection => collectionMock;
}

#pragma warning disable CA1032 // Implement standard exception constructors
/// <summary>
/// EN: Represents SQL Azure mock exception.
/// PT: Representa exceção simulada do SQL Azure.
/// </summary>
public sealed class SqlAzureMockException : SqlMockException
#pragma warning restore CA1032 // Implement standard exception constructors
{
    /// <summary>
    /// EN: Represents SQL Azure mock exception.
    /// PT: Representa exceção simulada do SQL Azure.
    /// </summary>
    public SqlAzureMockException(string message, int code) : base(message, code)
    {
    }

    /// <summary>
    /// EN: Represents SQL Azure mock exception.
    /// PT: Representa exceção simulada do SQL Azure.
    /// </summary>
    public SqlAzureMockException() : base()
    {
    }

    /// <summary>
    /// EN: Represents SQL Azure mock exception.
    /// PT: Representa exceção simulada do SQL Azure.
    /// </summary>
    public SqlAzureMockException(string? message) : base(message)
    {
    }

    /// <summary>
    /// EN: Represents SQL Azure mock exception.
    /// PT: Representa exceção simulada do SQL Azure.
    /// </summary>
    public SqlAzureMockException(string? message, Exception? innerException) : base(message, innerException)
    {
    }
}

/// <summary>
/// EN: Represents SQL Azure data reader mock.
/// PT: Representa leitor de dados simulado do SQL Azure.
/// </summary>
public sealed class SqlAzureDataReaderMock(
    IList<TableResultMock> tables
    ) : DbDataReaderMockBase(tables)
{
}

/// <summary>
/// EN: Represents SQL Azure parameter collection mock.
/// PT: Representa colecao de parametros simulada do SQL Azure.
/// </summary>
public class SqlAzureDataParameterCollectionMock : SqlServerDataParameterCollectionMock
{
}

/// <summary>
/// EN: Represents SQL Azure data adapter mock with typed SQL Azure command accessors.
/// PT: Representa adaptador de dados simulado do SQL Azure com acessores tipados de comando.
/// </summary>
public sealed class SqlAzureDataAdapterMock : DbDataAdapter
{
    public new SqlAzureCommandMock? DeleteCommand
    {
        get => base.DeleteCommand as SqlAzureCommandMock;
        set => base.DeleteCommand = value;
    }

    public new SqlAzureCommandMock? InsertCommand
    {
        get => base.InsertCommand as SqlAzureCommandMock;
        set => base.InsertCommand = value;
    }

    public new SqlAzureCommandMock? SelectCommand
    {
        get => base.SelectCommand as SqlAzureCommandMock;
        set => base.SelectCommand = value;
    }

    public new SqlAzureCommandMock? UpdateCommand
    {
        get => base.UpdateCommand as SqlAzureCommandMock;
        set => base.UpdateCommand = value;
    }

    public SqlAzureDataAdapterMock()
    {
    }

    public SqlAzureDataAdapterMock(SqlAzureCommandMock selectCommand) => SelectCommand = selectCommand;

    public SqlAzureDataAdapterMock(string selectCommandText, SqlAzureConnectionMock connection)
        => SelectCommand = new SqlAzureCommandMock(connection) { CommandText = selectCommandText };
}

/// <summary>
/// EN: Represents SQL Azure data source mock.
/// PT: Representa fonte de dados simulada do SQL Azure.
/// </summary>
public sealed class SqlAzureDataSourceMock(SqlAzureDbMock? db = null)
#if NET7_0_OR_GREATER
    : DbDataSource
#endif
{
    public
#if NET7_0_OR_GREATER
    override
#endif
    string ConnectionString => string.Empty;

#if NET7_0_OR_GREATER
    protected override DbConnection CreateDbConnection() => new SqlAzureConnectionMock(db);
#else
    public SqlAzureConnectionMock CreateDbConnection() => new SqlAzureConnectionMock(db);
#endif

    public
#if NET7_0_OR_GREATER
    new
#endif
    SqlAzureConnectionMock CreateConnection() => new SqlAzureConnectionMock(db);
}

/// <summary>
/// EN: Represents SQL Azure provider factory mock.
/// PT: Representa fabrica simulada do provedor SQL Azure.
/// </summary>
public sealed class SqlAzureConnectorFactoryMock : DbProviderFactory
{
    private static SqlAzureConnectorFactoryMock? instance;
    private readonly SqlAzureDbMock? db;

    public static SqlAzureConnectorFactoryMock GetInstance(SqlAzureDbMock? db = null)
        => instance ??= new SqlAzureConnectorFactoryMock(db);

    internal SqlAzureConnectorFactoryMock(SqlAzureDbMock? db = null)
    {
        this.db = db;
    }

    public override DbCommand CreateCommand() => new SqlAzureCommandMock();

    public override DbConnection CreateConnection() => new SqlAzureConnectionMock(db);

    public override DbConnectionStringBuilder CreateConnectionStringBuilder() => new DbConnectionStringBuilder();

    public override DbParameter CreateParameter() => new SqlParameter();

#if NETCOREAPP3_0_OR_GREATER || NETSTANDARD2_1_OR_GREATER
    public override bool CanCreateDataAdapter => true;
#endif

    public override DbDataAdapter CreateDataAdapter() => new SqlAzureDataAdapterMock();

    public override bool CanCreateDataSourceEnumerator => false;

#if NET6_0_OR_GREATER
    public override bool CanCreateBatch => true;

    public override DbBatch CreateBatch() => new SqlAzureBatchMock();

    public override DbBatchCommand CreateBatchCommand() => new SqlAzureBatchCommandMock();
#endif

#if NET7_0_OR_GREATER
    public override DbDataSource CreateDataSource(string connectionString) => new SqlAzureDataSourceMock(db);
#else
    public SqlAzureDataSourceMock CreateDataSource(string connectionString) => new(db);
#endif
}

#if NET6_0_OR_GREATER
/// <summary>
/// EN: Represents SQL Azure batch mock.
/// PT: Representa lote simulado do SQL Azure.
/// </summary>
public sealed class SqlAzureBatchMock : DbBatch
{
    private SqlAzureConnectionMock? connection;
    private SqlServerTransactionMock? transaction;

    public SqlAzureBatchMock() => BatchCommands = new SqlAzureBatchCommandCollectionMock();

    public SqlAzureBatchMock(SqlAzureConnectionMock connection, SqlServerTransactionMock? transaction = null) : this()
    {
        Connection = connection;
        Transaction = transaction;
    }

    public new SqlAzureConnectionMock? Connection
    {
        get => connection;
        set => connection = value;
    }

    protected override DbConnection? DbConnection
    {
        get => connection;
        set => connection = (SqlAzureConnectionMock?)value;
    }

    public new SqlServerTransactionMock? Transaction
    {
        get => transaction;
        set => transaction = value;
    }

    protected override DbTransaction? DbTransaction
    {
        get => transaction;
        set => transaction = (SqlServerTransactionMock?)value;
    }

    public override int Timeout { get; set; }

    public new SqlAzureBatchCommandCollectionMock BatchCommands { get; }

    protected override DbBatchCommandCollection DbBatchCommands => BatchCommands;

    public override void Cancel() => Transaction?.Rollback();

    public override int ExecuteNonQuery()
    {
        if (Connection is null)
            throw new InvalidOperationException("Connection must be set before executing a batch.");

        var affected = 0;
        foreach (var batchCommand in BatchCommands.Commands)
        {
            using var command = new SqlAzureCommandMock(Connection, Transaction)
            {
                CommandText = batchCommand.CommandText,
                CommandType = batchCommand.CommandType,
                CommandTimeout = Timeout
            };

            foreach (DbParameter parameter in batchCommand.Parameters)
                command.Parameters.Add(parameter);

            affected += command.ExecuteNonQuery();
        }

        return affected;
    }

    protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
    {
        if (Connection is null)
            throw new InvalidOperationException("Connection must be set before executing a batch.");

        var tables = new List<TableResultMock>();

        foreach (var batchCommand in BatchCommands.Commands)
        {
            using var command = new SqlAzureCommandMock(Connection, Transaction)
            {
                CommandText = batchCommand.CommandText,
                CommandType = batchCommand.CommandType,
                CommandTimeout = Timeout
            };

            foreach (DbParameter parameter in batchCommand.Parameters)
                command.Parameters.Add(parameter);

            try
            {
                using var reader = command.ExecuteReader(behavior);
                do
                {
                    var rows = new List<object[]>();
                    while (reader.Read())
                    {
                        var row = new object[reader.FieldCount];
                        reader.GetValues(row);
                        rows.Add(row);
                    }

                    if (reader.FieldCount > 0)
                        tables.Add(CreateTableResult(rows, reader));
                } while (reader.NextResult());
            }
            catch (InvalidOperationException ex) when (ex.Message == SqlExceptionMessages.ExecuteReaderWithoutSelectQuery())
            {
                command.ExecuteNonQuery();
            }
        }

        return new SqlAzureDataReaderMock(tables);
    }

    private static TableResultMock CreateTableResult(IReadOnlyCollection<object[]> rows, IDataRecord schemaRecord)
    {
        var table = new TableResultMock();

        for (var col = 0; col < schemaRecord.FieldCount; col++)
        {
            table.Columns.Add(new TableResultColMock(
                tableAlias: string.Empty,
                columnAlias: schemaRecord.GetName(col),
                columnName: schemaRecord.GetName(col),
                columIndex: col,
                dbType: schemaRecord.GetFieldType(col).ConvertTypeToDbType(),
                isNullable: true));
        }

        foreach (var row in rows)
        {
            var rowData = new Dictionary<int, object?>();
            for (var col = 0; col < row.Length; col++)
                rowData[col] = row[col] == DBNull.Value ? null : row[col];
            table.Add(rowData);
        }

        return table;
    }

    public override object? ExecuteScalar()
    {
        if (BatchCommands.Count == 0)
            return null;

        var first = BatchCommands.Commands[0];
        using var command = new SqlAzureCommandMock(Connection, Transaction)
        {
            CommandText = first.CommandText,
            CommandType = first.CommandType,
            CommandTimeout = Timeout
        };

        foreach (DbParameter parameter in first.Parameters)
            command.Parameters.Add(parameter);

        return command.ExecuteScalar();
    }

    public override System.Threading.Tasks.Task<int> ExecuteNonQueryAsync(System.Threading.CancellationToken cancellationToken = default)
        => System.Threading.Tasks.Task.FromResult(ExecuteNonQuery());

    protected override System.Threading.Tasks.Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, System.Threading.CancellationToken cancellationToken = default)
        => System.Threading.Tasks.Task.FromResult<DbDataReader>(ExecuteDbDataReader(behavior));

    public override System.Threading.Tasks.Task<object?> ExecuteScalarAsync(System.Threading.CancellationToken cancellationToken = default)
        => System.Threading.Tasks.Task.FromResult(ExecuteScalar());

    public override System.Threading.Tasks.Task PrepareAsync(System.Threading.CancellationToken cancellationToken = default)
    {
        Prepare();
        return System.Threading.Tasks.Task.CompletedTask;
    }

    public override void Prepare() { }

    protected override DbBatchCommand CreateDbBatchCommand() => new SqlAzureBatchCommandMock();
}

/// <summary>
/// EN: Represents SQL Azure batch command mock.
/// PT: Representa comando em lote simulado do SQL Azure.
/// </summary>
public sealed class SqlAzureBatchCommandMock : DbBatchCommand
{
    private readonly SqlAzureCommandMock command = new();
    private int recordsAffected = 0;

    public override string CommandText { get; set; } = string.Empty;
    public override CommandType CommandType { get; set; } = CommandType.Text;
    public override int RecordsAffected => recordsAffected;
    protected override DbParameterCollection DbParameterCollection => command.Parameters;
}

/// <summary>
/// EN: Represents SQL Azure batch command collection mock.
/// PT: Representa colecao de comandos em lote simulada do SQL Azure.
/// </summary>
public sealed class SqlAzureBatchCommandCollectionMock : DbBatchCommandCollection
{
    internal List<SqlAzureBatchCommandMock> Commands { get; } = [];

    public override int Count => Commands.Count;
    public override bool IsReadOnly => false;

    public override void Add(DbBatchCommand item)
    {
        if (item is SqlAzureBatchCommandMock b)
            Commands.Add(b);
    }

    public override void Clear() => Commands.Clear();
    public override bool Contains(DbBatchCommand item) => Commands.Contains((SqlAzureBatchCommandMock)item);

    public override void CopyTo(DbBatchCommand[] array, int arrayIndex)
        => Commands.Cast<DbBatchCommand>().ToArray().CopyTo(array, arrayIndex);

    public override IEnumerator<DbBatchCommand> GetEnumerator() => Commands.Cast<DbBatchCommand>().GetEnumerator();
    public override int IndexOf(DbBatchCommand item) => Commands.IndexOf((SqlAzureBatchCommandMock)item);
    public override void Insert(int index, DbBatchCommand item) => Commands.Insert(index, (SqlAzureBatchCommandMock)item);
    public override bool Remove(DbBatchCommand item) => Commands.Remove((SqlAzureBatchCommandMock)item);
    public override void RemoveAt(int index) => Commands.RemoveAt(index);
    protected override DbBatchCommand GetBatchCommand(int index) => Commands[index];
    protected override void SetBatchCommand(int index, DbBatchCommand batchCommand) => Commands[index] = (SqlAzureBatchCommandMock)batchCommand;
}
#endif
