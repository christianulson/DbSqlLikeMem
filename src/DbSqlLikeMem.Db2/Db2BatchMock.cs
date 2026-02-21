namespace DbSqlLikeMem.Db2;

#if NET6_0_OR_GREATER
/// <summary>
/// EN: Summary for Db2BatchMock.
/// PT: Resumo para Db2BatchMock.
/// </summary>
public sealed class Db2BatchMock : DbBatch
{
    private Db2ConnectionMock? connection;
    private Db2TransactionMock? transaction;

    /// <summary>
    /// EN: Summary for Db2BatchMock.
    /// PT: Resumo para Db2BatchMock.
    /// </summary>
    public Db2BatchMock() => BatchCommands = new Db2BatchCommandCollectionMock();

    /// <summary>
    /// EN: Summary for Db2BatchMock.
    /// PT: Resumo para Db2BatchMock.
    /// </summary>
    public Db2BatchMock(Db2ConnectionMock connection, Db2TransactionMock? transaction = null) : this()
    {
        Connection = connection;
        Transaction = transaction;
    }

    /// <summary>
    /// EN: Summary for Connection.
    /// PT: Resumo para Connection.
    /// </summary>
    public new Db2ConnectionMock? Connection
    {
        get => connection;
        set => connection = value;
    }

    /// <summary>
    /// EN: Summary for DbConnection.
    /// PT: Resumo para DbConnection.
    /// </summary>
    protected override DbConnection? DbConnection
    {
        get => connection;
        set => connection = (Db2ConnectionMock?)value;
    }

    /// <summary>
    /// EN: Summary for Transaction.
    /// PT: Resumo para Transaction.
    /// </summary>
    public new Db2TransactionMock? Transaction
    {
        get => transaction;
        set => transaction = value;
    }

    /// <summary>
    /// EN: Summary for DbTransaction.
    /// PT: Resumo para DbTransaction.
    /// </summary>
    protected override DbTransaction? DbTransaction
    {
        get => transaction;
        set => transaction = (Db2TransactionMock?)value;
    }

    /// <summary>
    /// EN: Summary for member.
    /// PT: Resumo para member.
    /// </summary>
    public override int Timeout { get; set; }

    /// <summary>
    /// EN: Summary for member.
    /// PT: Resumo para member.
    /// </summary>
    public new Db2BatchCommandCollectionMock BatchCommands { get; }

    /// <summary>
    /// EN: Summary for member.
    /// PT: Resumo para member.
    /// </summary>
    protected override DbBatchCommandCollection DbBatchCommands => BatchCommands;

    /// <summary>
    /// EN: Summary for Cancel.
    /// PT: Resumo para Cancel.
    /// </summary>
    public override void Cancel() => Transaction?.Rollback();

    /// <summary>
    /// EN: Summary for ExecuteNonQuery.
    /// PT: Resumo para ExecuteNonQuery.
    /// </summary>
    public override int ExecuteNonQuery()
    {
        if (Connection is null)
            throw new InvalidOperationException("Connection must be set before executing a batch.");

        var affected = 0;
        foreach (var batchCommand in BatchCommands.Commands)
        {
            using var command = new Db2CommandMock(Connection, Transaction)
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

    /// <summary>
    /// EN: Summary for ExecuteDbDataReader.
    /// PT: Resumo para ExecuteDbDataReader.
    /// </summary>
    protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
    {
        if (Connection is null)
            throw new InvalidOperationException("Connection must be set before executing a batch.");

        var tables = new List<TableResultMock>();

        foreach (var batchCommand in BatchCommands.Commands)
        {
            using var command = new Db2CommandMock(Connection, Transaction)
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

        return new Db2DataReaderMock(tables);
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

    /// <summary>
    /// EN: Summary for ExecuteScalar.
    /// PT: Resumo para ExecuteScalar.
    /// </summary>
    public override object? ExecuteScalar()
    {
        if (BatchCommands.Count == 0)
            return null;

        var first = BatchCommands.Commands[0];
        using var command = new Db2CommandMock(Connection, Transaction)
        {
            CommandText = first.CommandText,
            CommandType = first.CommandType,
            CommandTimeout = Timeout
        };

        foreach (DbParameter parameter in first.Parameters)
            command.Parameters.Add(parameter);

        return command.ExecuteScalar();
    }

    /// <summary>
    /// EN: Summary for ExecuteNonQueryAsync.
    /// PT: Resumo para ExecuteNonQueryAsync.
    /// </summary>
    public override System.Threading.Tasks.Task<int> ExecuteNonQueryAsync(System.Threading.CancellationToken cancellationToken = default)
        => System.Threading.Tasks.Task.FromResult(ExecuteNonQuery());

    /// <summary>
    /// EN: Summary for ExecuteDbDataReaderAsync.
    /// PT: Resumo para ExecuteDbDataReaderAsync.
    /// </summary>
    protected override System.Threading.Tasks.Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, System.Threading.CancellationToken cancellationToken = default)
        => System.Threading.Tasks.Task.FromResult<DbDataReader>(ExecuteDbDataReader(behavior));

    /// <summary>
    /// EN: Summary for ExecuteScalarAsync.
    /// PT: Resumo para ExecuteScalarAsync.
    /// </summary>
    public override System.Threading.Tasks.Task<object?> ExecuteScalarAsync(System.Threading.CancellationToken cancellationToken = default)
        => System.Threading.Tasks.Task.FromResult(ExecuteScalar());

    /// <summary>
    /// EN: Summary for PrepareAsync.
    /// PT: Resumo para PrepareAsync.
    /// </summary>
    public override System.Threading.Tasks.Task PrepareAsync(System.Threading.CancellationToken cancellationToken = default)
    {
        Prepare();
        return System.Threading.Tasks.Task.CompletedTask;
    }

    /// <summary>
    /// EN: Summary for Prepare.
    /// PT: Resumo para Prepare.
    /// </summary>
    public override void Prepare() { }

    /// <summary>
    /// EN: Summary for CreateDbBatchCommand.
    /// PT: Resumo para CreateDbBatchCommand.
    /// </summary>
    protected override DbBatchCommand CreateDbBatchCommand() => new Db2BatchCommandMock();
}

/// <summary>
/// EN: Summary for Db2BatchCommandMock.
/// PT: Resumo para Db2BatchCommandMock.
/// </summary>
public sealed class Db2BatchCommandMock : DbBatchCommand, IDb2CommandMock
{
    private readonly Db2CommandMock command = new();

    /// <summary>
    /// EN: Summary for member.
    /// PT: Resumo para member.
    /// </summary>
    public override string CommandText { get; set; } = string.Empty;

    /// <summary>
    /// EN: Summary for member.
    /// PT: Resumo para member.
    /// </summary>
    public override CommandType CommandType { get; set; } = CommandType.Text;

    /// <summary>
    /// EN: Summary for member.
    /// PT: Resumo para member.
    /// </summary>
    private int recordsAffected = 0;

    /// <summary>
    /// EN: Summary for member.
    /// PT: Resumo para member.
    /// </summary>
    public override int RecordsAffected => recordsAffected;

    /// <summary>
    /// EN: Summary for member.
    /// PT: Resumo para member.
    /// </summary>
    protected override DbParameterCollection DbParameterCollection => command.Parameters;
}

/// <summary>
/// EN: Summary for Db2BatchCommandCollectionMock.
/// PT: Resumo para Db2BatchCommandCollectionMock.
/// </summary>
public sealed class Db2BatchCommandCollectionMock : DbBatchCommandCollection
{
    internal List<Db2BatchCommandMock> Commands { get; } = [];

    /// <summary>
    /// EN: Summary for member.
    /// PT: Resumo para member.
    /// </summary>
    public override int Count => Commands.Count;

    /// <summary>
    /// EN: Summary for member.
    /// PT: Resumo para member.
    /// </summary>
    public override bool IsReadOnly => false;

    /// <summary>
    /// EN: Summary for Add.
    /// PT: Resumo para Add.
    /// </summary>
    public override void Add(DbBatchCommand item)
    {
        if (item is Db2BatchCommandMock b)
            Commands.Add(b);
    }

    /// <summary>
    /// EN: Summary for Clear.
    /// PT: Resumo para Clear.
    /// </summary>
    public override void Clear() => Commands.Clear();

    /// <summary>
    /// EN: Summary for Contains.
    /// PT: Resumo para Contains.
    /// </summary>
    public override bool Contains(DbBatchCommand item) => Commands.Contains((Db2BatchCommandMock)item);

    /// <summary>
    /// EN: Summary for CopyTo.
    /// PT: Resumo para CopyTo.
    /// </summary>
    public override void CopyTo(DbBatchCommand[] array, int arrayIndex)
        => Commands.Cast<DbBatchCommand>().ToArray().CopyTo(array, arrayIndex);

    /// <summary>
    /// EN: Summary for GetEnumerator.
    /// PT: Resumo para GetEnumerator.
    /// </summary>
    public override IEnumerator<DbBatchCommand> GetEnumerator() => Commands.Cast<DbBatchCommand>().GetEnumerator();

    /// <summary>
    /// EN: Summary for IndexOf.
    /// PT: Resumo para IndexOf.
    /// </summary>
    public override int IndexOf(DbBatchCommand item) => Commands.IndexOf((Db2BatchCommandMock)item);

    /// <summary>
    /// EN: Summary for Insert.
    /// PT: Resumo para Insert.
    /// </summary>
    public override void Insert(int index, DbBatchCommand item) => Commands.Insert(index, (Db2BatchCommandMock)item);

    /// <summary>
    /// EN: Summary for Remove.
    /// PT: Resumo para Remove.
    /// </summary>
    public override bool Remove(DbBatchCommand item) => Commands.Remove((Db2BatchCommandMock)item);

    /// <summary>
    /// EN: Summary for RemoveAt.
    /// PT: Resumo para RemoveAt.
    /// </summary>
    public override void RemoveAt(int index) => Commands.RemoveAt(index);

    /// <summary>
    /// EN: Summary for GetBatchCommand.
    /// PT: Resumo para GetBatchCommand.
    /// </summary>
    protected override DbBatchCommand GetBatchCommand(int index) => Commands[index];

    /// <summary>
    /// EN: Summary for SetBatchCommand.
    /// PT: Resumo para SetBatchCommand.
    /// </summary>
    protected override void SetBatchCommand(int index, DbBatchCommand batchCommand) => Commands[index] = (Db2BatchCommandMock)batchCommand;
}
#endif
