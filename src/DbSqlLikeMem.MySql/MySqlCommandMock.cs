namespace DbSqlLikeMem.MySql;

/// <summary>
/// EN: Mock command for MySQL connections.
/// PT: Comando mock para conexões MySQL.
/// </summary>
public class MySqlCommandMock(
    MySqlConnectionMock? connection = null,
    MySqlTransactionMock? transaction = null
    ) : DbCommand
{
    private bool disposedValue;

    /// <summary>
    /// Auto-generated summary.
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
    protected override DbConnection? DbConnection
    {
        get => connection;
        set => connection = value as MySqlConnectionMock;
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
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(CommandText, nameof(CommandText));

        // 1. Stored Procedure (sem parse SQL)
        if (CommandType == CommandType.StoredProcedure)
        {
            return connection!.ExecuteStoredProcedure(CommandText, Parameters);
        }

        var sqlRaw = CommandText.Trim();

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
                throw new InvalidOperationException("Invalid CREATE TEMPORARY TABLE statement.");
            return connection.ExecuteCreateTemporaryTableAsSelect(ct, Parameters, connection.Db.Dialect);
        }

        if (sqlRaw.StartsWith("create view", StringComparison.OrdinalIgnoreCase) ||
            sqlRaw.StartsWith("create or replace view", StringComparison.OrdinalIgnoreCase))
        {
            var q = SqlQueryParser.Parse(sqlRaw, connection!.Db.Dialect);
            if (q is not SqlCreateViewQuery cv)
                throw new InvalidOperationException("Invalid CREATE VIEW statement.");
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
            throw new InvalidOperationException("Invalid DELETE statement: expected FROM keyword.");

        // 3. Parse via AST para comandos DML (Insert, Update, Delete)
        var query = SqlQueryParser.Parse(sqlRaw, connection.Db.Dialect);

        return query switch
        {
            SqlInsertQuery insertQ => connection.ExecuteInsert(insertQ, Parameters, connection.Db.Dialect),
            SqlUpdateQuery updateQ => connection.ExecuteUpdateSmart(updateQ, Parameters, connection.Db.Dialect),
            SqlDeleteQuery deleteQ => connection.ExecuteDeleteSmart(deleteQ, Parameters, connection.Db.Dialect),
            SqlCreateViewQuery cv => connection.ExecuteCreateView(cv, Parameters, connection.Db.Dialect),
            SqlDropViewQuery dropViewQ => connection.ExecuteDropView(dropViewQ, Parameters, connection.Db.Dialect),
            SqlSelectQuery _ => throw new InvalidOperationException("Use ExecuteReader para comandos SELECT."),
            _ => throw new NotSupportedException($"Tipo de query não suportado em ExecuteNonQuery: {query.GetType().Name}")
        };
    }

    private static bool IsDeleteMissingFrom(string sqlRaw)
    {
        if (!sqlRaw.StartsWith("delete ", StringComparison.OrdinalIgnoreCase))
            return false;

        if (sqlRaw.StartsWith("delete from ", StringComparison.OrdinalIgnoreCase))
            return false;

        return !System.Text.RegularExpressions.Regex.IsMatch(
            sqlRaw,
            @"^\s*delete\s+[^\s]+\s+from\s+",
            System.Text.RegularExpressions.RegexOptions.IgnoreCase);
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
            throw new InvalidOperationException("Invalid DROP VIEW statement.");

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
            throw new InvalidOperationException("Invalid DROP VIEW statement.");

        var viewName = parts[i].Trim().Trim('`', '"').NormalizeName();
        connection!.DropView(viewName, ifExists);
        return 0;
    }

    /// <summary>
    /// EN: Executes the command and returns a data reader.
    /// PT: Executa o comando e retorna um data reader.
    /// </summary>
    /// <param name="behavior">EN: Command behavior. PT: Comportamento do comando.</param>
    /// <returns>EN: Data reader. PT: Data reader.</returns>
    protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(connection, nameof(connection));
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
        if (sql.Contains("UNION", StringComparison.OrdinalIgnoreCase) && !sql.Contains(';'))
        {
            var chain = SqlQueryParser.ParseUnionChain(sql, connection.Db.Dialect);
            // Garantindo o Cast correto para SqlSelectQuery
            var unionTable = executor.ExecuteUnion([.. chain.Parts.Cast<SqlSelectQuery>()], chain.AllFlags, sql);
            connection.Metrics.Selects += unionTable.Count;
            return new MySqlDataReaderMock([unionTable]);
        }


        // Parse Multiplo (ex: "SELECT 1; SELECT 2;" ou "CREATE TEMPORARY TABLE ...; SELECT ...")
        var tables = new List<TableResultMock>();
        var hasAnyQuery = false;

        foreach (var q in SqlQueryParser.ParseMulti(sql, connection.Db.Dialect))
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

                default:
                    throw new NotSupportedException($"Tipo de query não suportado em ExecuteReader: {q.GetType().Name}");
            }
        }

        if (tables.Count == 0 && hasAnyQuery)
            throw new InvalidOperationException("ExecuteReader foi chamado, mas nenhuma query SELECT foi encontrada.");

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
