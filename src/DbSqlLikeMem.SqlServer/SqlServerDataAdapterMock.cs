using DbDataAdapter = System.Data.Common.DbDataAdapter;
namespace DbSqlLikeMem.SqlServer;

/// <summary>
/// EN: Summary for SqlServerDataAdapterMock.
/// PT: Resumo para SqlServerDataAdapterMock.
/// </summary>
public sealed class SqlServerDataAdapterMock : DbDataAdapter
{
    /// <summary>
    /// EN: Summary for DeleteCommand.
    /// PT: Resumo para DeleteCommand.
    /// </summary>
    public new SqlServerCommandMock? DeleteCommand
    {
        get => base.DeleteCommand as SqlServerCommandMock;
        set => base.DeleteCommand = value;
    }

    /// <summary>
    /// EN: Summary for InsertCommand.
    /// PT: Resumo para InsertCommand.
    /// </summary>
    public new SqlServerCommandMock? InsertCommand
    {
        get => base.InsertCommand as SqlServerCommandMock;
        set => base.InsertCommand = value;
    }

    /// <summary>
    /// EN: Summary for SelectCommand.
    /// PT: Resumo para SelectCommand.
    /// </summary>
    public new SqlServerCommandMock? SelectCommand
    {
        get => base.SelectCommand as SqlServerCommandMock;
        set => base.SelectCommand = value;
    }

    /// <summary>
    /// EN: Summary for UpdateCommand.
    /// PT: Resumo para UpdateCommand.
    /// </summary>
    public new SqlServerCommandMock? UpdateCommand
    {
        get => base.UpdateCommand as SqlServerCommandMock;
        set => base.UpdateCommand = value;
    }

    /// <summary>
    /// EN: Summary for SqlServerDataAdapterMock.
    /// PT: Resumo para SqlServerDataAdapterMock.
    /// </summary>
    public SqlServerDataAdapterMock()
    {
    }

    /// <summary>
    /// EN: Summary for SqlServerDataAdapterMock.
    /// PT: Resumo para SqlServerDataAdapterMock.
    /// </summary>
    public SqlServerDataAdapterMock(SqlServerCommandMock selectCommand) => SelectCommand = selectCommand;

    /// <summary>
    /// EN: Summary for SqlServerDataAdapterMock.
    /// PT: Resumo para SqlServerDataAdapterMock.
    /// </summary>
    public SqlServerDataAdapterMock(string selectCommandText, SqlServerConnectionMock connection)
        => SelectCommand = new SqlServerCommandMock(connection) { CommandText = selectCommandText };
}
