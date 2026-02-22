using DbDataAdapter = System.Data.Common.DbDataAdapter;
namespace DbSqlLikeMem.SqlServer;

/// <summary>
/// EN: Represents the Sql Server Data Adapter Mock type used by provider mocks.
/// PT: Representa o tipo Sql Server adaptador de dados simulado usado pelos mocks do provedor.
/// </summary>
public sealed class SqlServerDataAdapterMock : DbDataAdapter
{
    /// <summary>
    /// EN: Executes delete command.
    /// PT: Executa delete comando.
    /// </summary>
    public new SqlServerCommandMock? DeleteCommand
    {
        get => base.DeleteCommand as SqlServerCommandMock;
        set => base.DeleteCommand = value;
    }

    /// <summary>
    /// EN: Executes insert command.
    /// PT: Executa insert comando.
    /// </summary>
    public new SqlServerCommandMock? InsertCommand
    {
        get => base.InsertCommand as SqlServerCommandMock;
        set => base.InsertCommand = value;
    }

    /// <summary>
    /// EN: Executes select command.
    /// PT: Executa select comando.
    /// </summary>
    public new SqlServerCommandMock? SelectCommand
    {
        get => base.SelectCommand as SqlServerCommandMock;
        set => base.SelectCommand = value;
    }

    /// <summary>
    /// EN: Executes update command.
    /// PT: Executa update comando.
    /// </summary>
    public new SqlServerCommandMock? UpdateCommand
    {
        get => base.UpdateCommand as SqlServerCommandMock;
        set => base.UpdateCommand = value;
    }

    /// <summary>
    /// EN: Represents a provider-specific data adapter mock with typed command accessors.
    /// PT: Representa um simulado de adaptador de dados específico do provedor com acessores tipados de comando.
    /// </summary>
    public SqlServerDataAdapterMock()
    {
    }

    /// <summary>
    /// EN: Represents a provider-specific data adapter mock with typed command accessors.
    /// PT: Representa um simulado de adaptador de dados específico do provedor com acessores tipados de comando.
    /// </summary>
    public SqlServerDataAdapterMock(SqlServerCommandMock selectCommand) => SelectCommand = selectCommand;

    /// <summary>
    /// EN: Represents a provider-specific data adapter mock with typed command accessors.
    /// PT: Representa um simulado de adaptador de dados específico do provedor com acessores tipados de comando.
    /// </summary>
    public SqlServerDataAdapterMock(string selectCommandText, SqlServerConnectionMock connection)
        => SelectCommand = new SqlServerCommandMock(connection) { CommandText = selectCommandText };
}
