using System.Data.Common;

using DbDataAdapter = System.Data.Common.DbDataAdapter;
namespace DbSqlLikeMem.Npgsql;

/// <summary>
/// EN: Summary for NpgsqlDataAdapterMock.
/// PT: Resumo para NpgsqlDataAdapterMock.
/// </summary>
public sealed class NpgsqlDataAdapterMock : DbDataAdapter
{
    /// <summary>
    /// EN: Summary for DeleteCommand.
    /// PT: Resumo para DeleteCommand.
    /// </summary>
    public new NpgsqlCommandMock? DeleteCommand
    {
        get => base.DeleteCommand as NpgsqlCommandMock;
        set => base.DeleteCommand = value;
    }

    /// <summary>
    /// EN: Summary for InsertCommand.
    /// PT: Resumo para InsertCommand.
    /// </summary>
    public new NpgsqlCommandMock? InsertCommand
    {
        get => base.InsertCommand as NpgsqlCommandMock;
        set => base.InsertCommand = value;
    }

    /// <summary>
    /// EN: Summary for SelectCommand.
    /// PT: Resumo para SelectCommand.
    /// </summary>
    public new NpgsqlCommandMock? SelectCommand
    {
        get => base.SelectCommand as NpgsqlCommandMock;
        set => base.SelectCommand = value;
    }

    /// <summary>
    /// EN: Summary for UpdateCommand.
    /// PT: Resumo para UpdateCommand.
    /// </summary>
    public new NpgsqlCommandMock? UpdateCommand
    {
        get => base.UpdateCommand as NpgsqlCommandMock;
        set => base.UpdateCommand = value;
    }

    /// <summary>
    /// EN: Summary for NpgsqlDataAdapterMock.
    /// PT: Resumo para NpgsqlDataAdapterMock.
    /// </summary>
    public NpgsqlDataAdapterMock()
    {
    }

    /// <summary>
    /// EN: Summary for NpgsqlDataAdapterMock.
    /// PT: Resumo para NpgsqlDataAdapterMock.
    /// </summary>
    public NpgsqlDataAdapterMock(NpgsqlCommandMock selectCommand) => SelectCommand = selectCommand;

    /// <summary>
    /// EN: Summary for NpgsqlDataAdapterMock.
    /// PT: Resumo para NpgsqlDataAdapterMock.
    /// </summary>
    public NpgsqlDataAdapterMock(string selectCommandText, NpgsqlConnectionMock connection)
        => SelectCommand = new NpgsqlCommandMock(connection) { CommandText = selectCommandText };
}
