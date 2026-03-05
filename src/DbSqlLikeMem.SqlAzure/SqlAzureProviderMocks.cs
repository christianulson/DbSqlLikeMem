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
    /// <summary>
    /// EN: Compatibility level value that emulates SQL Server 2008 behavior.
    /// PT: Valor de nivel de compatibilidade que emula o comportamento do SQL Server 2008.
    /// </summary>
    public const int SqlServer2008 = 100;
    /// <summary>
    /// EN: Compatibility level value that emulates SQL Server 2012 behavior.
    /// PT: Valor de nivel de compatibilidade que emula o comportamento do SQL Server 2012.
    /// </summary>
    public const int SqlServer2012 = 110;
    /// <summary>
    /// EN: Compatibility level value that emulates SQL Server 2014 behavior.
    /// PT: Valor de nivel de compatibilidade que emula o comportamento do SQL Server 2014.
    /// </summary>
    public const int SqlServer2014 = 120;
    /// <summary>
    /// EN: Compatibility level value that emulates SQL Server 2016 behavior.
    /// PT: Valor de nivel de compatibilidade que emula o comportamento do SQL Server 2016.
    /// </summary>
    public const int SqlServer2016 = 130;
    /// <summary>
    /// EN: Compatibility level value that emulates SQL Server 2017 behavior.
    /// PT: Valor de nivel de compatibilidade que emula o comportamento do SQL Server 2017.
    /// </summary>
    public const int SqlServer2017 = 140;
    /// <summary>
    /// EN: Compatibility level value that emulates SQL Server 2019 behavior.
    /// PT: Valor de nivel de compatibilidade que emula o comportamento do SQL Server 2019.
    /// </summary>
    public const int SqlServer2019 = 150;
    /// <summary>
    /// EN: Compatibility level value that emulates SQL Server 2022 behavior.
    /// PT: Valor de nivel de compatibilidade que emula o comportamento do SQL Server 2022.
    /// </summary>
    public const int SqlServer2022 = 160;
    /// <summary>
    /// EN: Compatibility level value that emulates SQL Server 2025 behavior.
    /// PT: Valor de nivel de compatibilidade que emula o comportamento do SQL Server 2025.
    /// </summary>
    public const int SqlServer2025 = 170;

    /// <summary>
    /// EN: Default compatibility level used when no explicit version is provided.
    /// PT: Nivel de compatibilidade padrao usado quando nenhuma versao explicita e informada.
    /// </summary>
    public const int Default = SqlServer2022;

    /// <summary>
    /// EN: Returns all SQL Azure compatibility levels supported by this mock provider.
    /// PT: Retorna todos os niveis de compatibilidade do SQL Azure suportados por este provedor simulado.
    /// </summary>
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
    /// <summary>
    /// EN: Returns all SQL Azure compatibility levels exposed by this alias helper.
    /// PT: Retorna todos os niveis de compatibilidade do SQL Azure expostos por este helper de alias.
    /// </summary>
    public static IEnumerable<int> Versions() => SqlAzureDbCompatibilityLevels.Versions();
}

/// <summary>
/// EN: In-memory database mock configured for SQL Azure compatibility levels.
/// PT: Banco simulado em memoria configurado para niveis de compatibilidade do SQL Azure.
/// </summary>
public class SqlAzureDbMock : SqlServerDbMock
{
    /// <summary>
    /// EN: Creates an in-memory SQL Azure database mock for the provided compatibility version.
    /// PT: Cria um banco simulado em memoria do SQL Azure para a versao de compatibilidade informada.
    /// </summary>
    public SqlAzureDbMock(int? version = null) : base(version ?? SqlAzureDbCompatibilityLevels.Default)
    {
    }

    /// <summary>
    /// EN: Creates a SQL Azure schema mock attached to this database.
    /// PT: Cria um esquema simulado do SQL Azure associado a este banco.
    /// </summary>
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
    /// <summary>
    /// EN: Creates a SQL Azure table mock inside this schema.
    /// PT: Cria uma tabela simulada do SQL Azure dentro deste esquema.
    /// </summary>
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
    /// <summary>
    /// EN: Creates the SQL Azure unknown-column exception for invalid column access.
    /// PT: Cria a excecao de coluna desconhecida do SQL Azure para acesso de coluna invalida.
    /// </summary>
    public override Exception UnknownColumn(string columnName)
        => SqlAzureExceptionFactory.UnknownColumn(columnName);

    /// <summary>
    /// EN: Creates the SQL Azure duplicate-key exception for unique key violations.
    /// PT: Cria a excecao de chave duplicada do SQL Azure para violacoes de chave unica.
    /// </summary>
    public override Exception DuplicateKey(string tbl, string key, object? val)
        => SqlAzureExceptionFactory.DuplicateKey(tbl, key, val);

    /// <summary>
    /// EN: Creates the SQL Azure exception for null values in non-nullable columns.
    /// PT: Cria a excecao do SQL Azure para valores nulos em colunas nao anulaveis.
    /// </summary>
    public override Exception ColumnCannotBeNull(string col)
        => SqlAzureExceptionFactory.ColumnCannotBeNull(col);

    /// <summary>
    /// EN: Creates the SQL Azure foreign-key failure exception.
    /// PT: Cria a excecao de falha de chave estrangeira do SQL Azure.
    /// </summary>
    public override Exception ForeignKeyFails(string col, string refTbl)
        => SqlAzureExceptionFactory.ForeignKeyFails(col, refTbl);

    /// <summary>
    /// EN: Creates the SQL Azure referenced-row exception for delete/update restrictions.
    /// PT: Cria a excecao de linha referenciada do SQL Azure para restricoes de exclusao/atualizacao.
    /// </summary>
    public override Exception ReferencedRow(string tbl)
        => SqlAzureExceptionFactory.ReferencedRow(tbl);
}

/// <summary>
/// EN: Represents SQL Azure connection mock.
/// PT: Representa conexao simulada do SQL Azure.
/// </summary>
public class SqlAzureConnectionMock : SqlServerConnectionMock
{
    /// <summary>
    /// EN: Creates a SQL Azure connection mock with optional in-memory database and default database name.
    /// PT: Cria uma conexao simulada do SQL Azure com banco em memoria opcional e nome de banco padrao.
    /// </summary>
    public SqlAzureConnectionMock(
       SqlAzureDbMock? db = null,
       string? defaultDatabase = null
   ) : base(db ?? [], defaultDatabase)
    {
        _serverVersion = $"SQL Azure {Db.Version}";
    }

    /// <summary>
    /// EN: Creates a transaction instance bound to this SQL Azure mock connection.
    /// PT: Cria uma instancia de transacao vinculada a esta conexao simulada do SQL Azure.
    /// </summary>
    protected override DbTransaction CreateTransaction(IsolationLevel isolationLevel)
        => new SqlServerTransactionMock(this, isolationLevel);

    /// <summary>
    /// EN: Creates the core SQL Azure command instance for this connection and optional transaction.
    /// PT: Cria a instancia principal de comando SQL Azure para esta conexao e transacao opcional.
    /// </summary>
    protected override DbCommand CreateDbCommandCore(DbTransaction? transaction)
        => new SqlAzureCommandMock(this, transaction as SqlServerTransactionMock);

    /// <summary>
    /// EN: Creates the SQL Azure-specific mock exception used by this connection.
    /// PT: Cria a excecao simulada especifica do SQL Azure usada por esta conexao.
    /// </summary>
    protected internal override Exception NewException(string message, int code)
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

    /// <summary>
    /// EN: Creates an empty SQL Azure command mock without connection and transaction.
    /// PT: Cria um comando simulado vazio do SQL Azure sem conexao e sem transacao.
    /// </summary>
    public SqlAzureCommandMock() : this(null, null)
    {
    }

    /// <summary>
    /// EN: Gets the parameter collection used by this SQL Azure command mock.
    /// PT: Obtem a colecao de parametros usada por este comando simulado do SQL Azure.
    /// </summary>
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
    /// <summary>
    /// EN: Gets or sets the typed SQL Azure command used to delete rows.
    /// PT: Obtem ou define o comando tipado do SQL Azure usado para excluir linhas.
    /// </summary>
    public new SqlAzureCommandMock? DeleteCommand
    {
        get => base.DeleteCommand as SqlAzureCommandMock;
        set => base.DeleteCommand = value;
    }

    /// <summary>
    /// EN: Gets or sets the typed SQL Azure command used to insert rows.
    /// PT: Obtem ou define o comando tipado do SQL Azure usado para inserir linhas.
    /// </summary>
    public new SqlAzureCommandMock? InsertCommand
    {
        get => base.InsertCommand as SqlAzureCommandMock;
        set => base.InsertCommand = value;
    }

    /// <summary>
    /// EN: Gets or sets the typed SQL Azure command used to select rows.
    /// PT: Obtem ou define o comando tipado do SQL Azure usado para selecionar linhas.
    /// </summary>
    public new SqlAzureCommandMock? SelectCommand
    {
        get => base.SelectCommand as SqlAzureCommandMock;
        set => base.SelectCommand = value;
    }

    /// <summary>
    /// EN: Gets or sets the typed SQL Azure command used to update rows.
    /// PT: Obtem ou define o comando tipado do SQL Azure usado para atualizar linhas.
    /// </summary>
    public new SqlAzureCommandMock? UpdateCommand
    {
        get => base.UpdateCommand as SqlAzureCommandMock;
        set => base.UpdateCommand = value;
    }

    /// <summary>
    /// EN: Creates an empty SQL Azure data adapter mock.
    /// PT: Cria um adaptador de dados simulado do SQL Azure vazio.
    /// </summary>
    public SqlAzureDataAdapterMock()
    {
    }

    /// <summary>
    /// EN: Creates a SQL Azure data adapter mock using the provided select command.
    /// PT: Cria um adaptador de dados simulado do SQL Azure usando o comando de selecao informado.
    /// </summary>
    public SqlAzureDataAdapterMock(SqlAzureCommandMock selectCommand) => SelectCommand = selectCommand;

    /// <summary>
    /// EN: Creates a SQL Azure data adapter mock from select command text and connection.
    /// PT: Cria um adaptador de dados simulado do SQL Azure a partir do texto de selecao e da conexao.
    /// </summary>
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
    /// <summary>
    /// EN: Gets the connection string exposed by this SQL Azure data source mock.
    /// PT: Obtem a string de conexao exposta por esta fonte de dados simulada do SQL Azure.
    /// </summary>
    public
#if NET7_0_OR_GREATER
    override
#endif
    string ConnectionString => string.Empty;

#if NET7_0_OR_GREATER
    /// <summary>
    /// EN: Creates a database connection instance for this SQL Azure data source mock.
    /// PT: Cria uma instancia de conexao de banco para esta fonte de dados simulada do SQL Azure.
    /// </summary>
    protected override DbConnection CreateDbConnection() => new SqlAzureConnectionMock(db);
#else
    /// <summary>
    /// EN: Creates a typed SQL Azure connection for this SQL Azure data source mock.
    /// PT: Cria uma conexao tipada do SQL Azure para esta fonte de dados simulada do SQL Azure.
    /// </summary>
    public SqlAzureConnectionMock CreateDbConnection() => new SqlAzureConnectionMock(db);
#endif

    /// <summary>
    /// EN: Creates a typed SQL Azure connection from this data source mock.
    /// PT: Cria uma conexao tipada do SQL Azure a partir desta fonte de dados simulada.
    /// </summary>
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

    /// <summary>
    /// EN: Gets the singleton SQL Azure provider factory mock instance.
    /// PT: Obtem a instancia singleton da fabrica simulada do provedor SQL Azure.
    /// </summary>
    public static SqlAzureConnectorFactoryMock GetInstance(SqlAzureDbMock? db = null)
        => instance ??= new SqlAzureConnectorFactoryMock(db);

    internal SqlAzureConnectorFactoryMock(SqlAzureDbMock? db = null)
    {
        this.db = db;
    }

    /// <summary>
    /// EN: Creates a SQL Azure command mock instance.
    /// PT: Cria uma instancia de comando simulado do SQL Azure.
    /// </summary>
    public override DbCommand CreateCommand() => new SqlAzureCommandMock();

    /// <summary>
    /// EN: Creates a SQL Azure connection mock instance.
    /// PT: Cria uma instancia de conexao simulada do SQL Azure.
    /// </summary>
    public override DbConnection CreateConnection() => new SqlAzureConnectionMock(db);

    /// <summary>
    /// EN: Creates a generic connection string builder for provider scenarios.
    /// PT: Cria um construtor generico de string de conexao para cenarios de provedor.
    /// </summary>
    public override DbConnectionStringBuilder CreateConnectionStringBuilder() => [];

    /// <summary>
    /// EN: Creates a provider parameter instance compatible with SQL Azure mocks.
    /// PT: Cria uma instancia de parametro de provedor compativel com mocks de SQL Azure.
    /// </summary>
    public override DbParameter CreateParameter() => new SqlParameter();

#if NETCOREAPP3_0_OR_GREATER || NETSTANDARD2_1_OR_GREATER
    /// <summary>
    /// EN: Indicates whether this provider factory can create data adapter instances.
    /// PT: Indica se esta fabrica de provedor pode criar instancias de adaptador de dados.
    /// </summary>
    public override bool CanCreateDataAdapter => true;
#endif

    /// <summary>
    /// EN: Creates a SQL Azure data adapter mock instance.
    /// PT: Cria uma instancia de adaptador de dados simulado do SQL Azure.
    /// </summary>
    public override DbDataAdapter CreateDataAdapter() => new SqlAzureDataAdapterMock();

    /// <summary>
    /// EN: Indicates whether this provider factory can create a data source enumerator.
    /// PT: Indica se esta fabrica de provedor pode criar um enumerador de fontes de dados.
    /// </summary>
    public override bool CanCreateDataSourceEnumerator => false;

#if NET6_0_OR_GREATER
    /// <summary>
    /// EN: Indicates whether this provider factory can create batch objects.
    /// PT: Indica se esta fabrica de provedor pode criar objetos de lote.
    /// </summary>
    public override bool CanCreateBatch => true;

    /// <summary>
    /// EN: Creates a SQL Azure batch mock instance.
    /// PT: Cria uma instancia de lote simulado do SQL Azure.
    /// </summary>
    public override DbBatch CreateBatch() => new SqlAzureBatchMock();

    /// <summary>
    /// EN: Creates a SQL Azure batch command mock instance.
    /// PT: Cria uma instancia de comando de lote simulado do SQL Azure.
    /// </summary>
    public override DbBatchCommand CreateBatchCommand() => new SqlAzureBatchCommandMock();
#endif

#if NET7_0_OR_GREATER
    /// <summary>
    /// EN: Creates a SQL Azure data source mock for the provided connection string.
    /// PT: Cria uma fonte de dados simulada do SQL Azure para a string de conexao informada.
    /// </summary>
    public override DbDataSource CreateDataSource(string connectionString) => new SqlAzureDataSourceMock(db);
#else
    /// <summary>
    /// EN: Creates a SQL Azure data source mock for the provided connection string.
    /// PT: Cria uma fonte de dados simulada do SQL Azure para a string de conexao informada.
    /// </summary>
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

    /// <summary>
    /// EN: Creates an empty SQL Azure batch mock with an empty command collection.
    /// PT: Cria um lote simulado do SQL Azure vazio com colecao de comandos vazia.
    /// </summary>
    public SqlAzureBatchMock() => BatchCommands = [];

    /// <summary>
    /// EN: Creates a SQL Azure batch mock bound to a connection and optional transaction.
    /// PT: Cria um lote simulado do SQL Azure associado a uma conexao e transacao opcional.
    /// </summary>
    public SqlAzureBatchMock(SqlAzureConnectionMock connection, SqlServerTransactionMock? transaction = null) : this()
    {
        Connection = connection;
        Transaction = transaction;
    }

    /// <summary>
    /// EN: Gets or sets the typed SQL Azure connection used by this batch.
    /// PT: Obtem ou define a conexao tipada do SQL Azure usada por este lote.
    /// </summary>
    public new SqlAzureConnectionMock? Connection
    {
        get => connection;
        set => connection = value;
    }

    /// <summary>
    /// EN: Gets or sets the base database connection used by this batch.
    /// PT: Obtem ou define a conexao de banco base usada por este lote.
    /// </summary>
    protected override DbConnection? DbConnection
    {
        get => connection;
        set => connection = (SqlAzureConnectionMock?)value;
    }

    /// <summary>
    /// EN: Gets or sets the typed transaction used by this batch.
    /// PT: Obtem ou define a transacao tipada usada por este lote.
    /// </summary>
    public new SqlServerTransactionMock? Transaction
    {
        get => transaction;
        set => transaction = value;
    }

    /// <summary>
    /// EN: Gets or sets the base transaction used by this batch.
    /// PT: Obtem ou define a transacao base usada por este lote.
    /// </summary>
    protected override DbTransaction? DbTransaction
    {
        get => transaction;
        set => transaction = (SqlServerTransactionMock?)value;
    }

    /// <summary>
    /// EN: Gets or sets the command timeout applied to batch commands.
    /// PT: Obtem ou define o timeout de comando aplicado aos comandos do lote.
    /// </summary>
    public override int Timeout { get; set; }

    /// <summary>
    /// EN: Gets the typed command collection held by this batch.
    /// PT: Obtem a colecao tipada de comandos mantida por este lote.
    /// </summary>
    public new SqlAzureBatchCommandCollectionMock BatchCommands { get; }

    /// <summary>
    /// EN: Gets the base batch command collection for framework consumption.
    /// PT: Obtem a colecao base de comandos em lote para consumo do framework.
    /// </summary>
    protected override DbBatchCommandCollection DbBatchCommands => BatchCommands;

    /// <summary>
    /// EN: Cancels the current batch by rolling back the active transaction when available.
    /// PT: Cancela o lote atual fazendo rollback da transacao ativa quando disponivel.
    /// </summary>
    public override void Cancel() => Transaction?.Rollback();

    /// <summary>
    /// EN: Executes all batch commands as non-query operations and returns total affected rows.
    /// PT: Executa todos os comandos do lote como operacoes sem consulta e retorna o total de linhas afetadas.
    /// </summary>
    public override int ExecuteNonQuery()
    {
        var connection = BatchExecutionGuards.RequireConnection(Connection);
        return BatchSyncExecutionRunner.ExecuteNonQueryCommands(
            connection,
            BatchCommands.Commands,
            CreateExecutableCommand);
    }

    /// <summary>
    /// EN: Executes all batch commands and returns a combined data reader for result sets.
    /// PT: Executa todos os comandos do lote e retorna um data reader combinado para os conjuntos de resultados.
    /// </summary>
    protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
    {
        var connection = BatchExecutionGuards.RequireConnection(Connection);
        return BatchSyncExecutionRunner.ExecuteReaderCommands(
            connection,
            BatchCommands.Commands,
            CreateExecutableCommand,
            behavior,
            static tables => new SqlAzureDataReaderMock(tables));
    }

    /// <summary>
    /// EN: Executes the first batch command and returns its scalar result.
    /// PT: Executa o primeiro comando do lote e retorna seu resultado escalar.
    /// </summary>
    public override object? ExecuteScalar()
    {
        var connection = BatchExecutionGuards.RequireConnection(Connection);
        return BatchScalarExecutionRunner.ExecuteFirstScalar(
            connection,
            BatchCommands.Commands,
            CreateExecutableCommand);
    }

    /// <summary>
    /// EN: Asynchronously executes all batch commands as non-query operations.
    /// PT: Executa assincronamente todos os comandos do lote como operacoes sem consulta.
    /// </summary>
    public override Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken = default)
    {
        var connection = BatchExecutionGuards.RequireConnection(Connection);
        return BatchAsyncExecutionRunner
            .ExecuteNonQueryCommandsAsync(
                connection,
                BatchCommands.Commands,
                CreateExecutableCommand,
                cancellationToken)
;
    }

    /// <summary>
    /// EN: Asynchronously executes all batch commands and returns a combined data reader.
    /// PT: Executa assincronamente todos os comandos do lote e retorna um data reader combinado.
    /// </summary>
    protected override Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken = default)
    {
        var connection = BatchExecutionGuards.RequireConnection(Connection);
        return BatchAsyncExecutionRunner
            .ExecuteReaderCommandsAsync(
                connection,
                BatchCommands.Commands,
                CreateExecutableCommand,
                behavior,
                static tables => (DbDataReader)new SqlAzureDataReaderMock(tables),
                cancellationToken)
;
    }

    /// <summary>
    /// EN: Asynchronously executes the first batch command and returns its scalar result.
    /// PT: Executa assincronamente o primeiro comando do lote e retorna seu resultado escalar.
    /// </summary>
    public override Task<object?> ExecuteScalarAsync(CancellationToken cancellationToken = default)
    {
        var connection = BatchExecutionGuards.RequireConnection(Connection);
        return BatchScalarExecutionRunner.ExecuteFirstScalarAsync(
            connection,
            BatchCommands.Commands,
            CreateExecutableCommand,
            cancellationToken);
    }

    private SqlAzureCommandMock CreateExecutableCommand(SqlAzureBatchCommandMock batchCommand)
    {
        var connection = BatchExecutionGuards.RequireConnection(Connection);
        return BatchCommandFactory.Create(
            connection,
            () => new SqlAzureCommandMock(connection, Transaction),
            batchCommand,
            Timeout);
    }

    /// <summary>
    /// EN: Asynchronously prepares the batch for execution.
    /// PT: Prepara assincronamente o lote para execucao.
    /// </summary>
    public override Task PrepareAsync(CancellationToken cancellationToken = default)
    {
        Prepare();
        return System.Threading.Tasks.Task.CompletedTask;
    }

    /// <summary>
    /// EN: Prepares the batch for execution.
    /// PT: Prepara o lote para execucao.
    /// </summary>
    public override void Prepare() { }

    /// <summary>
    /// EN: Creates a new typed SQL Azure batch command instance.
    /// PT: Cria uma nova instancia tipada de comando em lote do SQL Azure.
    /// </summary>
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

    /// <summary>
    /// EN: Gets or sets the SQL text executed by this batch command.
    /// PT: Obtem ou define o texto SQL executado por este comando em lote.
    /// </summary>
    public override string CommandText { get; set; } = string.Empty;
    /// <summary>
    /// EN: Gets or sets the command type used by this batch command.
    /// PT: Obtem ou define o tipo de comando usado por este comando em lote.
    /// </summary>
    public override CommandType CommandType { get; set; } = CommandType.Text;
    /// <summary>
    /// EN: Gets the number of records affected by the command execution.
    /// PT: Obtem o numero de registros afetados pela execucao do comando.
    /// </summary>
    public override int RecordsAffected => recordsAffected;
    /// <summary>
    /// EN: Gets the parameter collection associated with this batch command.
    /// PT: Obtem a colecao de parametros associada a este comando em lote.
    /// </summary>
    protected override DbParameterCollection DbParameterCollection => command.Parameters;
}

/// <summary>
/// EN: Represents SQL Azure batch command collection mock.
/// PT: Representa colecao de comandos em lote simulada do SQL Azure.
/// </summary>
public sealed class SqlAzureBatchCommandCollectionMock : DbBatchCommandCollection
{
    internal List<SqlAzureBatchCommandMock> Commands { get; } = [];

    /// <summary>
    /// EN: Gets the number of commands currently stored in the collection.
    /// PT: Obtem o numero de comandos atualmente armazenados na colecao.
    /// </summary>
    public override int Count => Commands.Count;
    /// <summary>
    /// EN: Gets whether the collection is read-only.
    /// PT: Obtem se a colecao e somente leitura.
    /// </summary>
    public override bool IsReadOnly => false;

    /// <summary>
    /// EN: Adds a batch command to the collection when it is SQL Azure compatible.
    /// PT: Adiciona um comando em lote a colecao quando ele e compativel com SQL Azure.
    /// </summary>
    public override void Add(DbBatchCommand item)
    {
        if (item is SqlAzureBatchCommandMock b)
            Commands.Add(b);
    }

    /// <summary>
    /// EN: Removes all commands from the collection.
    /// PT: Remove todos os comandos da colecao.
    /// </summary>
    public override void Clear() => Commands.Clear();
    /// <summary>
    /// EN: Checks whether the specified command exists in the collection.
    /// PT: Verifica se o comando especificado existe na colecao.
    /// </summary>
    public override bool Contains(DbBatchCommand item) => Commands.Contains((SqlAzureBatchCommandMock)item);

    /// <summary>
    /// EN: Copies collection commands to an array starting at the target index.
    /// PT: Copia os comandos da colecao para um array a partir do indice de destino.
    /// </summary>
    public override void CopyTo(DbBatchCommand[] array, int arrayIndex)
        => Commands.Cast<DbBatchCommand>().ToArray().CopyTo(array, arrayIndex);

    /// <summary>
    /// EN: Returns an enumerator for iterating over the commands in the collection.
    /// PT: Retorna um enumerador para iterar pelos comandos da colecao.
    /// </summary>
    public override IEnumerator<DbBatchCommand> GetEnumerator() => Commands.Cast<DbBatchCommand>().GetEnumerator();
    /// <summary>
    /// EN: Returns the index of the specified command in the collection.
    /// PT: Retorna o indice do comando especificado na colecao.
    /// </summary>
    public override int IndexOf(DbBatchCommand item) => Commands.IndexOf((SqlAzureBatchCommandMock)item);
    /// <summary>
    /// EN: Inserts a command into the collection at the specified index.
    /// PT: Insere um comando na colecao no indice especificado.
    /// </summary>
    public override void Insert(int index, DbBatchCommand item) => Commands.Insert(index, (SqlAzureBatchCommandMock)item);
    /// <summary>
    /// EN: Removes the specified command from the collection.
    /// PT: Remove o comando especificado da colecao.
    /// </summary>
    public override bool Remove(DbBatchCommand item) => Commands.Remove((SqlAzureBatchCommandMock)item);
    /// <summary>
    /// EN: Removes the command at the specified index from the collection.
    /// PT: Remove da colecao o comando no indice especificado.
    /// </summary>
    public override void RemoveAt(int index) => Commands.RemoveAt(index);
    /// <summary>
    /// EN: Gets the command located at the specified index.
    /// PT: Obtem o comando localizado no indice especificado.
    /// </summary>
    protected override DbBatchCommand GetBatchCommand(int index) => Commands[index];
    /// <summary>
    /// EN: Replaces the command at the specified index with a new command instance.
    /// PT: Substitui o comando no indice especificado por uma nova instancia de comando.
    /// </summary>
    protected override void SetBatchCommand(int index, DbBatchCommand batchCommand) => Commands[index] = (SqlAzureBatchCommandMock)batchCommand;
}
#endif
