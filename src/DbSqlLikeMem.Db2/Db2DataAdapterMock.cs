namespace DbSqlLikeMem.Db2;

/// <summary>
/// EN: Represents the Db2 Data Adapter Mock type used by provider mocks.
/// PT: Representa o adaptador de dados simulado do Db2 usado pelos mocks do provedor.
/// </summary>
public sealed class Db2DataAdapterMock : DbDataAdapter
{
    /// <summary>
    /// EN: Gets or sets the command used to delete rows during data adapter updates.
    /// PT: Obtém ou define o comando usado para excluir linhas durante atualizações do adaptador.
    /// </summary>
    public new Db2CommandMock? DeleteCommand
    {
        get => base.DeleteCommand as Db2CommandMock;
        set => base.DeleteCommand = value;
    }

    /// <summary>
    /// EN: Gets or sets the command used to insert rows during data adapter updates.
    /// PT: Obtém ou define o comando usado para inserir linhas durante atualizações do adaptador.
    /// </summary>
    public new Db2CommandMock? InsertCommand
    {
        get => base.InsertCommand as Db2CommandMock;
        set => base.InsertCommand = value;
    }

    /// <summary>
    /// EN: Gets or sets the command used to retrieve rows for this data adapter.
    /// PT: Obtém ou define o comando usado para consultar linhas neste adaptador.
    /// </summary>
    public new Db2CommandMock? SelectCommand
    {
        get => base.SelectCommand as Db2CommandMock;
        set => base.SelectCommand = value;
    }

    /// <summary>
    /// EN: Gets or sets the command used to update rows during data adapter updates.
    /// PT: Obtém ou define o comando usado para atualizar linhas durante atualizações do adaptador.
    /// </summary>
    public new Db2CommandMock? UpdateCommand
    {
        get => base.UpdateCommand as Db2CommandMock;
        set => base.UpdateCommand = value;
    }

    /// <summary>
    /// EN: Represents a provider-specific data adapter mock with typed command accessors.
    /// PT: Representa um simulado de adaptador de dados específico do provedor com acessores tipados de comando.
    /// </summary>
    public Db2DataAdapterMock()
    {
    }

    /// <summary>
    /// EN: Represents a provider-specific data adapter mock with typed command accessors.
    /// PT: Representa um simulado de adaptador de dados específico do provedor com acessores tipados de comando.
    /// </summary>
    public Db2DataAdapterMock(Db2CommandMock selectCommand) => SelectCommand = selectCommand;

    /// <summary>
    /// EN: Represents a provider-specific data adapter mock with typed command accessors.
    /// PT: Representa um simulado de adaptador de dados específico do provedor com acessores tipados de comando.
    /// </summary>
    public Db2DataAdapterMock(string selectCommandText, Db2ConnectionMock connection)
        => SelectCommand = new Db2CommandMock(connection) { CommandText = selectCommandText };
}
