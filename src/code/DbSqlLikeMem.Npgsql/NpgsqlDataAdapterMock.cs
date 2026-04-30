using DbDataAdapter = System.Data.Common.DbDataAdapter;
namespace DbSqlLikeMem.Npgsql;

/// <summary>
/// EN: Represents the Npgsql Data Adapter Mock type used by provider mocks.
/// PT: Representa o adaptador de dados simulado do Npgsql usado pelos mocks do provedor.
/// </summary>
public sealed class NpgsqlDataAdapterMock : DbDataAdapter
{
    /// <summary>
    /// EN: Gets or sets the command used to delete rows during data adapter updates.
    /// PT: Obtém ou define o comando usado para excluir linhas durante atualizações do adaptador.
    /// </summary>
    public new NpgsqlCommandMock? DeleteCommand
    {
        get => base.DeleteCommand as NpgsqlCommandMock;
        set => base.DeleteCommand = value;
    }

    /// <summary>
    /// EN: Gets or sets the command used to insert rows during data adapter updates.
    /// PT: Obtém ou define o comando usado para inserir linhas durante atualizações do adaptador.
    /// </summary>
    public new NpgsqlCommandMock? InsertCommand
    {
        get => base.InsertCommand as NpgsqlCommandMock;
        set => base.InsertCommand = value;
    }

    /// <summary>
    /// EN: Gets or sets the command used to retrieve rows for this data adapter.
    /// PT: Obtém ou define o comando usado para consultar linhas neste adaptador.
    /// </summary>
    public new NpgsqlCommandMock? SelectCommand
    {
        get => base.SelectCommand as NpgsqlCommandMock;
        set => base.SelectCommand = value;
    }

    /// <summary>
    /// EN: Gets or sets the command used to update rows during data adapter updates.
    /// PT: Obtém ou define o comando usado para atualizar linhas durante atualizações do adaptador.
    /// </summary>
    public new NpgsqlCommandMock? UpdateCommand
    {
        get => base.UpdateCommand as NpgsqlCommandMock;
        set => base.UpdateCommand = value;
    }

    /// <summary>
    /// EN: Represents a provider-specific data adapter mock with typed command accessors.
    /// PT: Representa um simulado de adaptador de dados específico do provedor com acessores tipados de comando.
    /// </summary>
    public NpgsqlDataAdapterMock()
    {
    }

    /// <summary>
    /// EN: Represents a provider-specific data adapter mock with typed command accessors.
    /// PT: Representa um simulado de adaptador de dados específico do provedor com acessores tipados de comando.
    /// </summary>
    public NpgsqlDataAdapterMock(NpgsqlCommandMock selectCommand) => SelectCommand = selectCommand;

    /// <summary>
    /// EN: Represents a provider-specific data adapter mock with typed command accessors.
    /// PT: Representa um simulado de adaptador de dados específico do provedor com acessores tipados de comando.
    /// </summary>
    public NpgsqlDataAdapterMock(string selectCommandText, NpgsqlConnectionMock connection)
        => SelectCommand = new NpgsqlCommandMock(connection) { CommandText = selectCommandText };
}
