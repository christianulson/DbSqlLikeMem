using DbDataAdapter = System.Data.Common.DbDataAdapter;
namespace DbSqlLikeMem.SqlServer;

/// <summary>
/// EN: Represents the Sql Server Data Adapter Mock type used by provider mocks.
/// PT: Representa o adaptador de dados simulado do SQL Server usado pelos mocks do provedor.
/// </summary>
public sealed class SqlServerDataAdapterMock : DbDataAdapter
{
    /// <summary>
    /// EN: Gets or sets the command used to delete rows during data adapter updates.
    /// PT: Obtém ou define o comando usado para excluir linhas durante atualizações do adaptador.
    /// </summary>
    public new SqlServerCommandMock? DeleteCommand
    {
        get => base.DeleteCommand as SqlServerCommandMock;
        set => base.DeleteCommand = value;
    }

    /// <summary>
    /// EN: Gets or sets the command used to insert rows during data adapter updates.
    /// PT: Obtém ou define o comando usado para inserir linhas durante atualizações do adaptador.
    /// </summary>
    public new SqlServerCommandMock? InsertCommand
    {
        get => base.InsertCommand as SqlServerCommandMock;
        set => base.InsertCommand = value;
    }

    /// <summary>
    /// EN: Gets or sets the command used to retrieve rows for this data adapter.
    /// PT: Obtém ou define o comando usado para consultar linhas neste adaptador.
    /// </summary>
    public new SqlServerCommandMock? SelectCommand
    {
        get => base.SelectCommand as SqlServerCommandMock;
        set => base.SelectCommand = value;
    }

    /// <summary>
    /// EN: Gets or sets the command used to update rows during data adapter updates.
    /// PT: Obtém ou define o comando usado para atualizar linhas durante atualizações do adaptador.
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
