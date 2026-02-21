using System.Data.Common;

namespace DbSqlLikeMem.Db2;

/// <summary>
/// EN: Summary for Db2DataAdapterMock.
/// PT: Resumo para Db2DataAdapterMock.
/// </summary>
public sealed class Db2DataAdapterMock : DbDataAdapter
{
    /// <summary>
    /// EN: Summary for DeleteCommand.
    /// PT: Resumo para DeleteCommand.
    /// </summary>
    public new Db2CommandMock? DeleteCommand
    {
        get => base.DeleteCommand as Db2CommandMock;
        set => base.DeleteCommand = value;
    }

    /// <summary>
    /// EN: Summary for InsertCommand.
    /// PT: Resumo para InsertCommand.
    /// </summary>
    public new Db2CommandMock? InsertCommand
    {
        get => base.InsertCommand as Db2CommandMock;
        set => base.InsertCommand = value;
    }

    /// <summary>
    /// EN: Summary for SelectCommand.
    /// PT: Resumo para SelectCommand.
    /// </summary>
    public new Db2CommandMock? SelectCommand
    {
        get => base.SelectCommand as Db2CommandMock;
        set => base.SelectCommand = value;
    }

    /// <summary>
    /// EN: Summary for UpdateCommand.
    /// PT: Resumo para UpdateCommand.
    /// </summary>
    public new Db2CommandMock? UpdateCommand
    {
        get => base.UpdateCommand as Db2CommandMock;
        set => base.UpdateCommand = value;
    }

    /// <summary>
    /// EN: Summary for Db2DataAdapterMock.
    /// PT: Resumo para Db2DataAdapterMock.
    /// </summary>
    public Db2DataAdapterMock()
    {
    }

    /// <summary>
    /// EN: Summary for Db2DataAdapterMock.
    /// PT: Resumo para Db2DataAdapterMock.
    /// </summary>
    public Db2DataAdapterMock(Db2CommandMock selectCommand) => SelectCommand = selectCommand;

    /// <summary>
    /// EN: Summary for Db2DataAdapterMock.
    /// PT: Resumo para Db2DataAdapterMock.
    /// </summary>
    public Db2DataAdapterMock(string selectCommandText, Db2ConnectionMock connection)
        => SelectCommand = new Db2CommandMock(connection) { CommandText = selectCommandText };
}
