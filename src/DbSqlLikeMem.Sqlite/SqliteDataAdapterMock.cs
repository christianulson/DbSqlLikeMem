using DbDataAdapter = System.Data.Common.DbDataAdapter;
namespace DbSqlLikeMem.Sqlite;

/// <summary>
/// EN: Summary for SqliteDataAdapterMock.
/// PT: Resumo para SqliteDataAdapterMock.
/// </summary>
public sealed class SqliteDataAdapterMock : DbDataAdapter
{
    /// <summary>
    /// EN: Summary for DeleteCommand.
    /// PT: Resumo para DeleteCommand.
    /// </summary>
    public new SqliteCommandMock? DeleteCommand
    {
        get => base.DeleteCommand as SqliteCommandMock;
        set => base.DeleteCommand = value;
    }

    /// <summary>
    /// EN: Summary for InsertCommand.
    /// PT: Resumo para InsertCommand.
    /// </summary>
    public new SqliteCommandMock? InsertCommand
    {
        get => base.InsertCommand as SqliteCommandMock;
        set => base.InsertCommand = value;
    }

    /// <summary>
    /// EN: Summary for SelectCommand.
    /// PT: Resumo para SelectCommand.
    /// </summary>
    public new SqliteCommandMock? SelectCommand
    {
        get => base.SelectCommand as SqliteCommandMock;
        set => base.SelectCommand = value;
    }

    /// <summary>
    /// EN: Summary for UpdateCommand.
    /// PT: Resumo para UpdateCommand.
    /// </summary>
    public new SqliteCommandMock? UpdateCommand
    {
        get => base.UpdateCommand as SqliteCommandMock;
        set => base.UpdateCommand = value;
    }

    /// <summary>
    /// EN: Summary for SqliteDataAdapterMock.
    /// PT: Resumo para SqliteDataAdapterMock.
    /// </summary>
    public SqliteDataAdapterMock()
    {
    }

    /// <summary>
    /// EN: Summary for SqliteDataAdapterMock.
    /// PT: Resumo para SqliteDataAdapterMock.
    /// </summary>
    public SqliteDataAdapterMock(SqliteCommandMock selectCommand) => SelectCommand = selectCommand;

    /// <summary>
    /// EN: Summary for SqliteDataAdapterMock.
    /// PT: Resumo para SqliteDataAdapterMock.
    /// </summary>
    public SqliteDataAdapterMock(string selectCommandText, SqliteConnectionMock connection)
        => SelectCommand = new SqliteCommandMock(connection) { CommandText = selectCommandText };
}
