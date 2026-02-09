using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using Oracle.ManagedDataAccess.Client;

namespace DbSqlLikeMem.Oracle;

/// <summary>
/// Oracle command mock. Faz parse com OracleDialect e executa via engine atual (OracleAstQueryExecutor).
/// </summary>
public class OracleCommandMock(
    OracleConnectionMock? connection = null,
    OracleTransactionMock? transaction = null
    ) : DbCommand
{
    private bool disposedValue;

    [AllowNull]
    public override string CommandText { get; set; } = string.Empty;

    public override int CommandTimeout { get; set; }
    public override CommandType CommandType { get; set; } = CommandType.Text;

    /// <summary>
    /// EN: Gets or sets the associated connection.
    /// PT: Obtém ou define a conexão associada.
    /// </summary>
    protected override DbConnection? DbConnection
    {
        get => connection;
        set => connection = value as OracleConnectionMock;
    }

    private readonly OracleDataParameterCollectionMock collectionMock = [];
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
        set => transaction = value as OracleTransactionMock;
    }

    public override UpdateRowSource UpdatedRowSource { get; set; }
    public override bool DesignTimeVisible { get; set; }

    public override void Cancel() => DbTransaction?.Rollback();

    /// <summary>
    /// EN: Creates a new Oracle parameter.
    /// PT: Cria um novo parâmetro Oracle.
    /// </summary>
    /// <returns>EN: Parameter instance. PT: Instância do parâmetro.</returns>
    protected override DbParameter CreateDbParameter()
        // Por enquanto reusa OracleParameter (OracleConnector) para não puxar pacote de SqlClient.
        => new OracleParameter();

    public override int ExecuteNonQuery()
    {
        ArgumentNullException.ThrowIfNull(connection);
        ArgumentException.ThrowIfNullOrWhiteSpace(CommandText);

        if (CommandType == CommandType.StoredProcedure)
            return connection.ExecuteStoredProcedure(CommandText, Parameters);

        var sqlRaw = CommandText.Trim();

        // Mantém atalhos existentes (CALL / CREATE TABLE AS SELECT) por compatibilidade do engine atual
        if (sqlRaw.StartsWith("call ", StringComparison.OrdinalIgnoreCase))
            return connection.ExecuteCall(sqlRaw, Parameters);

        if (sqlRaw.StartsWith("create table", StringComparison.OrdinalIgnoreCase))
            return connection.ExecuteCreateTableAsSelect(sqlRaw, Parameters, connection.Db.Dialect);

        var query = SqlQueryParser.Parse(sqlRaw, connection.Db.Dialect);

        return query switch
        {
            SqlInsertQuery insertQ => connection.ExecuteInsert(insertQ, Parameters, connection.Db.Dialect),
            SqlUpdateQuery updateQ => connection.ExecuteUpdate(updateQ, Parameters),
            SqlDeleteQuery deleteQ => connection.ExecuteDelete(deleteQ, Parameters),
            SqlSelectQuery _ => throw new InvalidOperationException("Use ExecuteReader para comandos SELECT."),
            _ => throw new NotSupportedException($"Tipo de query não suportado em ExecuteNonQuery: {query.GetType().Name}")
        };
    }

    /// <summary>
    /// EN: Executes the command and returns a data reader.
    /// PT: Executa o comando e retorna um data reader.
    /// </summary>
    /// <param name="behavior">EN: Command behavior. PT: Comportamento do comando.</param>
    /// <returns>EN: Data reader. PT: Data reader.</returns>
    protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
    {
        ArgumentNullException.ThrowIfNull(connection);
        ArgumentNullException.ThrowIfNull(CommandText);

        if (CommandType == CommandType.StoredProcedure)
        {
            connection.ExecuteStoredProcedure(CommandText, Parameters);
            return new OracleDataReaderMock([[]]);
        }

        var sql = CommandText.NormalizeString();

        if (sql.StartsWith("CALL", StringComparison.OrdinalIgnoreCase))
        {
            connection.ExecuteCall(sql, Parameters);
            return new OracleDataReaderMock([[]]);
        }

        var executor = new OracleAstQueryExecutor(connection, Parameters);

        if (sql.Contains("UNION", StringComparison.OrdinalIgnoreCase) && !sql.Contains(';', StringComparison.Ordinal))
        {
            var chain = SqlQueryParser.ParseUnionChain(sql, connection.Db.Dialect);
            var unionTable = executor.ExecuteUnion(chain.Parts.Cast<SqlSelectQuery>().ToList(), chain.AllFlags, sql);
            connection.Metrics.Selects += unionTable.Count;
            return new OracleDataReaderMock([unionTable]);
        }

        var queries = SqlQueryParser.ParseMulti(sql, connection.Db.Dialect).ToList();
        var selectQueries = queries.OfType<SqlSelectQuery>().ToList();

        if (selectQueries.Count == 0 && queries.Count > 0)
            throw new InvalidOperationException("ExecuteReader foi chamado, mas nenhuma query SELECT foi encontrada.");

        var tables = selectQueries.ConvertAll(executor.ExecuteSelect);
        connection.Metrics.Selects += tables.Sum(t => t.Count);

        return new OracleDataReaderMock(tables);
    }

    public override object ExecuteScalar()
    {
        using var reader = ExecuteReader();
        if (reader.Read())
            return reader.GetValue(0);
        return DBNull.Value;
    }

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
