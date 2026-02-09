using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using Microsoft.Data.SqlClient;


namespace DbSqlLikeMem.SqlServer;

/// <summary>
/// SqlServer command mock. Faz parse com SqlServerDialect e executa via engine atual (SqlServerAstQueryExecutor).
/// </summary>
public class SqlServerCommandMock(
    SqlServerConnectionMock? connection = null,
    SqlServerTransactionMock? transaction = null
    ) : DbCommand
{
    private bool disposedValue;

    [AllowNull]
    public override string CommandText { get; set; } = string.Empty;

    public override int CommandTimeout { get; set; }
    public override CommandType CommandType { get; set; } = CommandType.Text;

    protected override DbConnection? DbConnection
    {
        get => connection;
        set => connection = value as SqlServerConnectionMock;
    }

    private readonly SqlServerDataParameterCollectionMock collectionMock = [];
    protected override DbParameterCollection DbParameterCollection => collectionMock;

    protected override DbTransaction? DbTransaction
    {
        get => transaction!;
        set => transaction = value as SqlServerTransactionMock;
    }

    public override UpdateRowSource UpdatedRowSource { get; set; }
    public override bool DesignTimeVisible { get; set; }

    public override void Cancel() => DbTransaction?.Rollback();

    protected override DbParameter CreateDbParameter()
        => new SqlParameter();

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

    protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
    {
        ArgumentNullException.ThrowIfNull(connection);
        ArgumentNullException.ThrowIfNull(CommandText);

        if (CommandType == CommandType.StoredProcedure)
        {
            connection.ExecuteStoredProcedure(CommandText, Parameters);
            return new SqlServerDataReaderMock([[]]);
        }

        var sql = CommandText.NormalizeString();

        if (sql.StartsWith("CALL", StringComparison.OrdinalIgnoreCase))
        {
            connection.ExecuteCall(sql, Parameters);
            return new SqlServerDataReaderMock([[]]);
        }

        var executor = new SqlServerAstQueryExecutor(connection, Parameters);

        if (sql.Contains("UNION", StringComparison.OrdinalIgnoreCase) && !sql.Contains(';', StringComparison.Ordinal))
        {
            var chain = SqlQueryParser.ParseUnionChain(sql, connection.Db.Dialect);
            var unionTable = executor.ExecuteUnion(chain.Parts.Cast<SqlSelectQuery>().ToList(), chain.AllFlags, sql);
            connection.Metrics.Selects += unionTable.Count;
            return new SqlServerDataReaderMock([unionTable]);
        }

        var queries = SqlQueryParser.ParseMulti(sql, connection.Db.Dialect).ToList();
        var selectQueries = queries.OfType<SqlSelectQuery>().ToList();

        if (selectQueries.Count == 0 && queries.Count > 0)
            throw new InvalidOperationException("ExecuteReader foi chamado, mas nenhuma query SELECT foi encontrada.");

        var tables = selectQueries.ConvertAll(executor.ExecuteSelect);
        connection.Metrics.Selects += tables.Sum(t => t.Count);

        return new SqlServerDataReaderMock(tables);
    }

    public override object ExecuteScalar()
    {
        using var reader = ExecuteReader();
        if (reader.Read())
            return reader.GetValue(0);
        return DBNull.Value;
    }

    public override void Prepare() { }

    protected override void Dispose(bool disposing)
    {
        if (!disposedValue)
        {
            disposedValue = true;
        }
        base.Dispose(disposing);
    }
}
