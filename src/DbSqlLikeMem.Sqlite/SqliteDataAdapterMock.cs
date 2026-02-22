using DbDataAdapter = System.Data.Common.DbDataAdapter;
namespace DbSqlLikeMem.Sqlite;

/// <summary>
/// EN: Represents the Sqlite Data Adapter Mock type used by provider mocks.
/// PT: Representa o tipo Sqlite adaptador de dados simulado usado pelos mocks do provedor.
/// </summary>
public sealed class SqliteDataAdapterMock : DbDataAdapter
{
    /// <summary>
    /// EN: Executes delete command.
    /// PT: Executa delete comando.
    /// </summary>
    public new SqliteCommandMock? DeleteCommand
    {
        get => base.DeleteCommand as SqliteCommandMock;
        set => base.DeleteCommand = value;
    }

    /// <summary>
    /// EN: Executes insert command.
    /// PT: Executa insert comando.
    /// </summary>
    public new SqliteCommandMock? InsertCommand
    {
        get => base.InsertCommand as SqliteCommandMock;
        set => base.InsertCommand = value;
    }

    /// <summary>
    /// EN: Executes select command.
    /// PT: Executa select comando.
    /// </summary>
    public new SqliteCommandMock? SelectCommand
    {
        get => base.SelectCommand as SqliteCommandMock;
        set => base.SelectCommand = value;
    }

    /// <summary>
    /// EN: Executes update command.
    /// PT: Executa update comando.
    /// </summary>
    public new SqliteCommandMock? UpdateCommand
    {
        get => base.UpdateCommand as SqliteCommandMock;
        set => base.UpdateCommand = value;
    }

    /// <summary>
    /// EN: Represents a provider-specific data adapter mock with typed command accessors.
    /// PT: Representa um simulado de adaptador de dados específico do provedor com acessores tipados de comando.
    /// </summary>
    public SqliteDataAdapterMock()
    {
    }

    /// <summary>
    /// EN: Represents a provider-specific data adapter mock with typed command accessors.
    /// PT: Representa um simulado de adaptador de dados específico do provedor com acessores tipados de comando.
    /// </summary>
    public SqliteDataAdapterMock(SqliteCommandMock selectCommand) => SelectCommand = selectCommand;

    /// <summary>
    /// EN: Represents a provider-specific data adapter mock with typed command accessors.
    /// PT: Representa um simulado de adaptador de dados específico do provedor com acessores tipados de comando.
    /// </summary>
    public SqliteDataAdapterMock(string selectCommandText, SqliteConnectionMock connection)
        => SelectCommand = new SqliteCommandMock(connection) { CommandText = selectCommandText };
}
