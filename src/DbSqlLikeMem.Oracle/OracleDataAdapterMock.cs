namespace DbSqlLikeMem.Oracle;

/// <summary>
/// EN: Summary for OracleDataAdapterMock.
/// PT: Resumo para OracleDataAdapterMock.
/// </summary>
public sealed class OracleDataAdapterMock : DbDataAdapter
{
    /// <summary>
    /// EN: Summary for DeleteCommand.
    /// PT: Resumo para DeleteCommand.
    /// </summary>
    public new OracleCommandMock? DeleteCommand
    {
        get => base.DeleteCommand as OracleCommandMock;
        set => base.DeleteCommand = value;
    }

    /// <summary>
    /// EN: Summary for InsertCommand.
    /// PT: Resumo para InsertCommand.
    /// </summary>
    public new OracleCommandMock? InsertCommand
    {
        get => base.InsertCommand as OracleCommandMock;
        set => base.InsertCommand = value;
    }

    /// <summary>
    /// EN: Summary for SelectCommand.
    /// PT: Resumo para SelectCommand.
    /// </summary>
    public new OracleCommandMock? SelectCommand
    {
        get => base.SelectCommand as OracleCommandMock;
        set => base.SelectCommand = value;
    }

    /// <summary>
    /// EN: Summary for UpdateCommand.
    /// PT: Resumo para UpdateCommand.
    /// </summary>
    public new OracleCommandMock? UpdateCommand
    {
        get => base.UpdateCommand as OracleCommandMock;
        set => base.UpdateCommand = value;
    }

    /// <summary>
    /// EN: Summary for OracleDataAdapterMock.
    /// PT: Resumo para OracleDataAdapterMock.
    /// </summary>
    public OracleDataAdapterMock()
    {
    }

    /// <summary>
    /// EN: Summary for OracleDataAdapterMock.
    /// PT: Resumo para OracleDataAdapterMock.
    /// </summary>
    public OracleDataAdapterMock(OracleCommandMock selectCommand) => SelectCommand = selectCommand;

    /// <summary>
    /// EN: Summary for OracleDataAdapterMock.
    /// PT: Resumo para OracleDataAdapterMock.
    /// </summary>
    public OracleDataAdapterMock(string selectCommandText, OracleConnectionMock connection)
        => SelectCommand = new OracleCommandMock(connection) { CommandText = selectCommandText };
}
