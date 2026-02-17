using System.ComponentModel;

namespace DbSqlLikeMem.MySql;

/// <summary>
/// MySQL mock type used to emulate provider behavior for tests.
/// Tipo de mock MySQL usado para emular o comportamento do provedor em testes.
/// </summary>
public sealed class MySqlDataAdapterMock : DbDataAdapter, IDbDataAdapter
{
    private bool loadingDefaults;

    private int updateBatchSize;

    private List<IDbCommand>? commandBatch;

    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    public new MySqlCommandMock DeleteCommand
    {
        get
        {
            return (MySqlCommandMock)base.DeleteCommand;
        }
        set
        {
            base.DeleteCommand = value;
        }
    }

    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    public new MySqlCommandMock InsertCommand
    {
        get
        {
            return (MySqlCommandMock)base.InsertCommand;
        }
        set
        {
            base.InsertCommand = value;
        }
    }

    [Category("Fill")]
    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    public new MySqlCommandMock SelectCommand
    {
        get
        {
            return (MySqlCommandMock)base.SelectCommand;
        }
        set
        {
            base.SelectCommand = value;
        }
    }

    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    public new MySqlCommandMock UpdateCommand
    {
        get
        {
            return (MySqlCommandMock)base.UpdateCommand;
        }
        set
        {
            base.UpdateCommand = value;
        }
    }

    internal bool LoadDefaults
    {
        get
        {
            return loadingDefaults;
        }
        set
        {
            loadingDefaults = value;
        }
    }

    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    public override int UpdateBatchSize
    {
        get
        {
            return updateBatchSize;
        }
        set
        {
            updateBatchSize = value;
        }
    }

    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    public event MySqlRowUpdatingEventHandler? RowUpdating;

    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    public event MySqlRowUpdatedEventHandler? RowUpdated;

    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    public MySqlDataAdapterMock()
    {
        loadingDefaults = true;
        updateBatchSize = 1;
    }

    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    public MySqlDataAdapterMock(MySqlCommandMock selectCommand)
        : this()
    {
        SelectCommand = selectCommand;
    }

    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    public MySqlDataAdapterMock(string selectCommandText, MySqlConnectionMock connection)
        : this()
    {
        SelectCommand = new MySqlCommandMock(connection) { CommandText = selectCommandText };
    }

    private void OpenConnectionIfClosed(DataRowState state, List<MySqlConnectionMock> openedConnections)
    {
        MySqlCommandMock? mySqlCommand = null;
        switch (state)
        {
            default:
                return;
            case DataRowState.Added:
                mySqlCommand = InsertCommand;
                break;
            case DataRowState.Deleted:
                mySqlCommand = DeleteCommand;
                break;
            case DataRowState.Modified:
                mySqlCommand = UpdateCommand;
                break;
        }

        if (mySqlCommand != null && mySqlCommand.Connection != null && mySqlCommand.Connection.State == ConnectionState.Closed)
        {
            mySqlCommand.Connection.Open();
            openedConnections.Add(mySqlCommand.Connection);
        }
    }

    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    protected override int Update(DataRow[] dataRows, DataTableMapping tableMapping)
    {
        List<MySqlConnectionMock> list = [];
        try
        {
            foreach (DataRow dataRow in dataRows)
            {
                OpenConnectionIfClosed(dataRow.RowState, list);
            }

            return base.Update(dataRows, tableMapping);
        }
        finally
        {
            foreach (MySqlConnectionMock item in list)
            {
                item.Close();
            }
        }
    }

    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    protected override void InitializeBatching() => commandBatch = new List<IDbCommand>();

    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    protected override int AddToBatch(IDbCommand command)
    {
        List<IDbCommand> batch = commandBatch ?? throw new InvalidOperationException("Batching has not been initialized.");
        MySqlCommandMock mySqlCommand = (MySqlCommandMock)command;
        if (mySqlCommand.BatchableCommandText == null)
        {
            mySqlCommand.GetCommandTextForBatching();
        }

        IDbCommand item = (IDbCommand)((ICloneable)command).Clone();
        batch.Add(item);
        return batch.Count - 1;
    }

    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    protected override int ExecuteBatch()
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(commandBatch, nameof(commandBatch));
        int num = 0;
        int num2 = 0;
        while (num2 < commandBatch!.Count)
        {
            MySqlCommandMock mySqlCommand = (MySqlCommandMock)commandBatch[num2++];
            int num3 = num2;
            while (num3 < commandBatch.Count)
            {
                MySqlCommandMock mySqlCommand2 = (MySqlCommandMock)commandBatch[num3];
                if (mySqlCommand2.BatchableCommandText == null
                    || mySqlCommand2.CommandText != mySqlCommand.CommandText)
                {
                    break;
                }

                mySqlCommand.AddToBatch(mySqlCommand2);
                num3++;
                num2++;
            }

            num += mySqlCommand.ExecuteNonQuery();
        }

        return num;
    }

    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    protected override void ClearBatch()
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(commandBatch, nameof(commandBatch));
        if (commandBatch!.Count > 0)
        {
            MySqlCommandMock mySqlCommand = (MySqlCommandMock)commandBatch[0];
            mySqlCommand.Batch?.Clear();
        }

        commandBatch.Clear();
    }

    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    protected override void TerminateBatching()
    {
        ClearBatch();
        commandBatch = null;
    }

    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    protected override IDataParameter GetBatchedParameter(int commandIdentifier, int parameterIndex)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(commandBatch, nameof(commandBatch));
        object? parameter = commandBatch[commandIdentifier].Parameters[parameterIndex];
        return (IDataParameter)parameter!;
    }

    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    protected override RowUpdatedEventArgs CreateRowUpdatedEvent(DataRow dataRow, IDbCommand command, StatementType statementType, DataTableMapping tableMapping)
        => new MySqlRowUpdatedEventArgs(dataRow, command, statementType, tableMapping);

    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    protected override RowUpdatingEventArgs CreateRowUpdatingEvent(DataRow dataRow, IDbCommand command, StatementType statementType, DataTableMapping tableMapping)
        => new MySqlRowUpdatingEventArgs(dataRow, command, statementType, tableMapping);

    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    protected override void OnRowUpdating(RowUpdatingEventArgs value)
    {
        if (this.RowUpdating != null)
        {
            this.RowUpdating(this, value as MySqlRowUpdatingEventArgs);
        }
    }

    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    protected override void OnRowUpdated(RowUpdatedEventArgs value)
    {
        if (this.RowUpdated != null)
        {
            this.RowUpdated(this, value as MySqlRowUpdatedEventArgs);
        }
    }

    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    public Task<int> FillAsync(DataSet dataSet) => FillAsync(dataSet, CancellationToken.None);

    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    public Task<int> FillAsync(DataSet dataSet, CancellationToken cancellationToken)
    {
        TaskCompletionSource<int> taskCompletionSource = new();
        if (cancellationToken == CancellationToken.None || !cancellationToken.IsCancellationRequested)
        {
            try
            {
                int result = Fill(dataSet);
                taskCompletionSource.SetResult(result);
            }
            catch (Exception exception)
            {
                taskCompletionSource.SetException(exception);
            }
        }
        else
        {
            taskCompletionSource.SetCanceled();
        }

        return taskCompletionSource.Task;
    }

    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    public Task<int> FillAsync(DataTable dataTable) => FillAsync(dataTable, CancellationToken.None);

    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    public Task<int> FillAsync(DataTable dataTable, CancellationToken cancellationToken)
    {
        TaskCompletionSource<int> taskCompletionSource = new();
        if (cancellationToken == CancellationToken.None || !cancellationToken.IsCancellationRequested)
        {
            try
            {
                int result = Fill(dataTable);
                taskCompletionSource.SetResult(result);
            }
            catch (Exception exception)
            {
                taskCompletionSource.SetException(exception);
            }
        }
        else
        {
            taskCompletionSource.SetCanceled();
        }

        return taskCompletionSource.Task;
    }

    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    public Task<int> FillAsync(DataSet dataSet, string srcTable) => FillAsync(dataSet, srcTable, CancellationToken.None);

    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    public Task<int> FillAsync(DataSet dataSet, string srcTable, CancellationToken cancellationToken)
    {
        TaskCompletionSource<int> taskCompletionSource = new();
        if (cancellationToken == CancellationToken.None || !cancellationToken.IsCancellationRequested)
        {
            try
            {
                int result = Fill(dataSet, srcTable);
                taskCompletionSource.SetResult(result);
            }
            catch (Exception exception)
            {
                taskCompletionSource.SetException(exception);
            }
        }
        else
        {
            taskCompletionSource.SetCanceled();
        }

        return taskCompletionSource.Task;
    }

    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    public Task<int> FillAsync(DataTable dataTable, IDataReader dataReader) => FillAsync(dataTable, dataReader, CancellationToken.None);

    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    public Task<int> FillAsync(DataTable dataTable, IDataReader dataReader, CancellationToken cancellationToken)
    {
        TaskCompletionSource<int> taskCompletionSource = new();
        if (cancellationToken == CancellationToken.None || !cancellationToken.IsCancellationRequested)
        {
            try
            {
                int result = Fill(dataTable, dataReader);
                taskCompletionSource.SetResult(result);
            }
            catch (Exception exception)
            {
                taskCompletionSource.SetException(exception);
            }
        }
        else
        {
            taskCompletionSource.SetCanceled();
        }

        return taskCompletionSource.Task;
    }

    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    public Task<int> FillAsync(DataTable dataTable, IDbCommand command, CommandBehavior behavior) => FillAsync(dataTable, command, behavior, CancellationToken.None);

    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    public Task<int> FillAsync(DataTable dataTable, IDbCommand command, CommandBehavior behavior, CancellationToken cancellationToken)
    {
        TaskCompletionSource<int> taskCompletionSource = new();
        if (cancellationToken == CancellationToken.None || !cancellationToken.IsCancellationRequested)
        {
            try
            {
                int result = Fill(dataTable, command, behavior);
                taskCompletionSource.SetResult(result);
            }
            catch (Exception exception)
            {
                taskCompletionSource.SetException(exception);
            }
        }
        else
        {
            taskCompletionSource.SetCanceled();
        }

        return taskCompletionSource.Task;
    }

    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    public Task<int> FillAsync(int startRecord, int maxRecords, params DataTable[] dataTables) => FillAsync(startRecord, maxRecords, CancellationToken.None, dataTables);

    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    public Task<int> FillAsync(int startRecord, int maxRecords, CancellationToken cancellationToken, params DataTable[] dataTables)
    {
        TaskCompletionSource<int> taskCompletionSource = new();
        if (cancellationToken == CancellationToken.None || !cancellationToken.IsCancellationRequested)
        {
            try
            {
                int result = Fill(startRecord, maxRecords, dataTables);
                taskCompletionSource.SetResult(result);
            }
            catch (Exception exception)
            {
                taskCompletionSource.SetException(exception);
            }
        }
        else
        {
            taskCompletionSource.SetCanceled();
        }

        return taskCompletionSource.Task;
    }

    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    public Task<int> FillAsync(DataSet dataSet, int startRecord, int maxRecords, string srcTable) => FillAsync(dataSet, startRecord, maxRecords, srcTable, CancellationToken.None);

    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    public Task<int> FillAsync(DataSet dataSet, int startRecord, int maxRecords, string srcTable, CancellationToken cancellationToken)
    {
        TaskCompletionSource<int> taskCompletionSource = new();
        if (cancellationToken == CancellationToken.None || !cancellationToken.IsCancellationRequested)
        {
            try
            {
                int result = Fill(dataSet, startRecord, maxRecords, srcTable);
                taskCompletionSource.SetResult(result);
            }
            catch (Exception exception)
            {
                taskCompletionSource.SetException(exception);
            }
        }
        else
        {
            taskCompletionSource.SetCanceled();
        }

        return taskCompletionSource.Task;
    }

    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    public Task<int> FillAsync(DataSet dataSet, string srcTable, IDataReader dataReader, int startRecord, int maxRecords) => FillAsync(dataSet, srcTable, dataReader, startRecord, maxRecords, CancellationToken.None);

    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    public Task<int> FillAsync(DataSet dataSet, string srcTable, IDataReader dataReader, int startRecord, int maxRecords, CancellationToken cancellationToken)
    {
        TaskCompletionSource<int> taskCompletionSource = new();
        if (cancellationToken == CancellationToken.None || !cancellationToken.IsCancellationRequested)
        {
            try
            {
                int result = Fill(dataSet, srcTable, dataReader, startRecord, maxRecords);
                taskCompletionSource.SetResult(result);
            }
            catch (Exception exception)
            {
                taskCompletionSource.SetException(exception);
            }
        }
        else
        {
            taskCompletionSource.SetCanceled();
        }

        return taskCompletionSource.Task;
    }

    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    public Task<int> FillAsync(DataTable[] dataTables, int startRecord, int maxRecords, IDbCommand command, CommandBehavior behavior)
        => FillAsync(dataTables, startRecord, maxRecords, command, behavior, CancellationToken.None);


    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    public Task<int> FillAsync(DataTable[] dataTables, int startRecord, int maxRecords, IDbCommand command, CommandBehavior behavior, CancellationToken cancellationToken)
    {
        TaskCompletionSource<int> taskCompletionSource = new();
        if (cancellationToken != CancellationToken.None && cancellationToken.IsCancellationRequested)
        {
            taskCompletionSource.SetCanceled();
            return taskCompletionSource.Task;
        }
        try
        {
            int result = Fill(dataTables, startRecord, maxRecords, command, behavior);
            taskCompletionSource.SetResult(result);
        }
        catch (Exception exception)
        {
            taskCompletionSource.SetException(exception);
        }

        return taskCompletionSource.Task;
    }

    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    public Task<int> FillAsync(DataSet dataSet, int startRecord, int maxRecords, string srcTable, IDbCommand command, CommandBehavior behavior)
        => FillAsync(dataSet, startRecord, maxRecords, srcTable, command, behavior, CancellationToken.None);

    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    public Task<int> FillAsync(DataSet dataSet, int startRecord, int maxRecords, string srcTable, IDbCommand command, CommandBehavior behavior, CancellationToken cancellationToken)
    {
        TaskCompletionSource<int> taskCompletionSource = new();
        if (cancellationToken != CancellationToken.None && cancellationToken.IsCancellationRequested)
        {
            taskCompletionSource.SetCanceled();
            return taskCompletionSource.Task;
        }

        try
        {
            int result = Fill(dataSet, startRecord, maxRecords, srcTable, command, behavior);
            taskCompletionSource.SetResult(result);
        }
        catch (Exception exception)
        {
            taskCompletionSource.SetException(exception);
        }

        return taskCompletionSource.Task;
    }

    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    public Task<DataTable[]> FillSchemaAsync(DataSet dataSet, SchemaType schemaType)
        => FillSchemaAsync(dataSet, schemaType, CancellationToken.None);

    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    public Task<DataTable[]> FillSchemaAsync(DataSet dataSet, SchemaType schemaType, CancellationToken cancellationToken)
    {
        TaskCompletionSource<DataTable[]> taskCompletionSource = new();
        if (cancellationToken != CancellationToken.None && cancellationToken.IsCancellationRequested)
        {
            taskCompletionSource.SetCanceled();
            return taskCompletionSource.Task;
        }

        try
        {
            DataTable[] result = FillSchema(dataSet, schemaType);
            taskCompletionSource.SetResult(result);
        }
        catch (Exception exception)
        {
            taskCompletionSource.SetException(exception);
        }

        return taskCompletionSource.Task;
    }

    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    public Task<DataTable[]> FillSchemaAsync(DataSet dataSet, SchemaType schemaType, string srcTable)
        => FillSchemaAsync(dataSet, schemaType, srcTable, CancellationToken.None);

    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    public Task<DataTable[]> FillSchemaAsync(DataSet dataSet, SchemaType schemaType, string srcTable, CancellationToken cancellationToken)
    {
        TaskCompletionSource<DataTable[]> taskCompletionSource = new();
        if (cancellationToken != CancellationToken.None
            && cancellationToken.IsCancellationRequested)
        {
            taskCompletionSource.SetCanceled();
            return taskCompletionSource.Task;
        }

        try
        {
            DataTable[] result = FillSchema(dataSet, schemaType, srcTable);
            taskCompletionSource.SetResult(result);
        }
        catch (Exception exception)
        {
            taskCompletionSource.SetException(exception);
        }

        return taskCompletionSource.Task;
    }

    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    public Task<DataTable[]> FillSchemaAsync(DataSet dataSet, SchemaType schemaType, string srcTable, IDataReader dataReader)
        => FillSchemaAsync(dataSet, schemaType, srcTable, dataReader, CancellationToken.None);

    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    public Task<DataTable[]> FillSchemaAsync(DataSet dataSet, SchemaType schemaType, string srcTable, IDataReader dataReader, CancellationToken cancellationToken)
    {
        TaskCompletionSource<DataTable[]> taskCompletionSource = new();
        if (cancellationToken != CancellationToken.None
            && cancellationToken.IsCancellationRequested)
        {
            taskCompletionSource.SetCanceled();
            return taskCompletionSource.Task;
        }

        try
        {
            DataTable[] result = FillSchema(dataSet, schemaType, srcTable, dataReader);
            taskCompletionSource.SetResult(result);
        }
        catch (Exception exception)
        {
            taskCompletionSource.SetException(exception);
        }

        return taskCompletionSource.Task;
    }

    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    public Task<DataTable[]> FillSchemaAsync(DataSet dataSet, SchemaType schemaType, IDbCommand command, string srcTable, CommandBehavior behavior)
        => FillSchemaAsync(dataSet, schemaType, command, srcTable, behavior, CancellationToken.None);

    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    public Task<DataTable[]> FillSchemaAsync(DataSet dataSet, SchemaType schemaType, IDbCommand command, string srcTable, CommandBehavior behavior, CancellationToken cancellationToken)
    {
        TaskCompletionSource<DataTable[]> taskCompletionSource = new();
        if (cancellationToken != CancellationToken.None && cancellationToken.IsCancellationRequested)
        {
            taskCompletionSource.SetCanceled();
            return taskCompletionSource.Task;
        }

        try
        {
            DataTable[] result = FillSchema(dataSet, schemaType, command, srcTable, behavior);
            taskCompletionSource.SetResult(result);
        }
        catch (Exception exception)
        {
            taskCompletionSource.SetException(exception);
        }

        return taskCompletionSource.Task;
    }

    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    public Task<DataTable> FillSchemaAsync(DataTable dataTable, SchemaType schemaType)
        => FillSchemaAsync(dataTable, schemaType, CancellationToken.None);

    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    public Task<DataTable> FillSchemaAsync(DataTable dataTable, SchemaType schemaType, CancellationToken cancellationToken)
    {
        TaskCompletionSource<DataTable> taskCompletionSource = new();
        if (cancellationToken != CancellationToken.None
            && cancellationToken.IsCancellationRequested)
        {
            taskCompletionSource.SetCanceled();
            return taskCompletionSource.Task;
        }

        try
        {
            DataTable result = FillSchema(dataTable, schemaType) ?? dataTable;
            taskCompletionSource.SetResult(result);
        }
        catch (Exception exception)
        {
            taskCompletionSource.SetException(exception);
        }

        return taskCompletionSource.Task;
    }

    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    public Task<DataTable> FillSchemaAsync(DataTable dataTable, SchemaType schemaType, IDataReader dataReader)
        => FillSchemaAsync(dataTable, schemaType, dataReader, CancellationToken.None);

    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    public Task<DataTable> FillSchemaAsync(DataTable dataTable, SchemaType schemaType, IDataReader dataReader, CancellationToken cancellationToken)
    {
        TaskCompletionSource<DataTable> taskCompletionSource = new();
        if (cancellationToken != CancellationToken.None
            && cancellationToken.IsCancellationRequested)
        {
            taskCompletionSource.SetCanceled();
            return taskCompletionSource.Task;
        }

        try
        {
            DataTable result = FillSchema(dataTable, schemaType, dataReader) ?? dataTable;
            taskCompletionSource.SetResult(result);
        }
        catch (Exception exception)
        {
            taskCompletionSource.SetException(exception);
        }


        return taskCompletionSource.Task;
    }

    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    public Task<DataTable> FillSchemaAsync(DataTable dataTable, SchemaType schemaType, IDbCommand command, CommandBehavior behavior)
        => FillSchemaAsync(dataTable, schemaType, command, behavior, CancellationToken.None);

    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    public Task<DataTable> FillSchemaAsync(DataTable dataTable, SchemaType schemaType, IDbCommand command, CommandBehavior behavior, CancellationToken cancellationToken)
    {
        TaskCompletionSource<DataTable> taskCompletionSource = new();
        if (cancellationToken != CancellationToken.None
            && cancellationToken.IsCancellationRequested)
        {
            taskCompletionSource.SetCanceled();
            return taskCompletionSource.Task;
        }

        try
        {
            DataTable result = FillSchema(dataTable, schemaType, command, behavior) ?? dataTable;
            taskCompletionSource.SetResult(result);
        }
        catch (Exception exception)
        {
            taskCompletionSource.SetException(exception);
        }

        return taskCompletionSource.Task;
    }

    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    public Task<int> UpdateAsync(DataRow[] dataRows) => UpdateAsync(dataRows, CancellationToken.None);

    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    public Task<int> UpdateAsync(DataRow[] dataRows, CancellationToken cancellationToken)
    {
        TaskCompletionSource<int> taskCompletionSource = new();
        if (cancellationToken != CancellationToken.None
            && cancellationToken.IsCancellationRequested)
        {
            taskCompletionSource.SetCanceled();
            return taskCompletionSource.Task;
        }

        try
        {
            int result = Update(dataRows);
            taskCompletionSource.SetResult(result);
        }
        catch (Exception exception)
        {
            taskCompletionSource.SetException(exception);
        }

        return taskCompletionSource.Task;
    }

    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    public Task<int> UpdateAsync(DataSet dataSet) => UpdateAsync(dataSet, CancellationToken.None);

    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    public Task<int> UpdateAsync(DataSet dataSet, CancellationToken cancellationToken)
    {
        TaskCompletionSource<int> taskCompletionSource = new();
        if (cancellationToken != CancellationToken.None
            && cancellationToken.IsCancellationRequested)
        {
            taskCompletionSource.SetCanceled();
            return taskCompletionSource.Task;
        }

        try
        {
            int result = Update(dataSet);
            taskCompletionSource.SetResult(result);
        }
        catch (Exception exception)
        {
            taskCompletionSource.SetException(exception);
        }

        return taskCompletionSource.Task;
    }

    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    public Task<int> UpdateAsync(DataTable dataTable)
        => UpdateAsync(dataTable, CancellationToken.None);

    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    public Task<int> UpdateAsync(DataTable dataTable, CancellationToken cancellationToken)
    {
        TaskCompletionSource<int> taskCompletionSource = new();
        if (cancellationToken != CancellationToken.None
            && cancellationToken.IsCancellationRequested)
        {
            taskCompletionSource.SetCanceled();
            return taskCompletionSource.Task;
        }

        try
        {
            int result = Update(dataTable);
            taskCompletionSource.SetResult(result);
        }
        catch (Exception exception)
        {
            taskCompletionSource.SetException(exception);
        }

        return taskCompletionSource.Task;
    }

    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    public Task<int> UpdateAsync(DataRow[] dataRows, DataTableMapping tableMapping)
        => UpdateAsync(dataRows, tableMapping, CancellationToken.None);

    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    public Task<int> UpdateAsync(DataRow[] dataRows, DataTableMapping tableMapping, CancellationToken cancellationToken)
    {
        TaskCompletionSource<int> taskCompletionSource = new();
        if (cancellationToken != CancellationToken.None
            && cancellationToken.IsCancellationRequested)
        {
            taskCompletionSource.SetCanceled();
            return taskCompletionSource.Task;
        }

        try
        {
            int result = base.Update(dataRows, tableMapping);
            taskCompletionSource.SetResult(result);
        }
        catch (Exception exception)
        {
            taskCompletionSource.SetException(exception);
        }

        return taskCompletionSource.Task;
    }

    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    public Task<int> UpdateAsync(DataSet dataSet, string srcTable)
        => UpdateAsync(dataSet, srcTable, CancellationToken.None);

    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    public Task<int> UpdateAsync(DataSet dataSet, string srcTable, CancellationToken cancellationToken)
    {
        TaskCompletionSource<int> taskCompletionSource = new();
        if (cancellationToken != CancellationToken.None
            && cancellationToken.IsCancellationRequested)
        {
            taskCompletionSource.SetCanceled();
            return taskCompletionSource.Task;
        }

        try
        {
            int result = Update(dataSet, srcTable);
            taskCompletionSource.SetResult(result);
        }
        catch (Exception exception)
        {
            taskCompletionSource.SetException(exception);
        }

        return taskCompletionSource.Task;
    }
}
