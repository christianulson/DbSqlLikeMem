namespace DbSqlLikeMem.Oracle;

/// <summary>
/// EN: Represents the Oracle Data Adapter Mock type used by provider mocks.
/// PT: Representa o adaptador de dados simulado do Oracle usado pelos mocks do provedor.
/// </summary>
public sealed class OracleDataAdapterMock : DbDataAdapter
{
    /// <summary>
    /// EN: Gets or sets the command used to delete rows during data adapter updates.
    /// PT: Obtém ou define o comando usado para excluir linhas durante atualizações do adaptador.
    /// </summary>
    public new OracleCommandMock? DeleteCommand
    {
        get => base.DeleteCommand as OracleCommandMock;
        set => base.DeleteCommand = value;
    }

    /// <summary>
    /// EN: Gets or sets the command used to insert rows during data adapter updates.
    /// PT: Obtém ou define o comando usado para inserir linhas durante atualizações do adaptador.
    /// </summary>
    public new OracleCommandMock? InsertCommand
    {
        get => base.InsertCommand as OracleCommandMock;
        set => base.InsertCommand = value;
    }

    /// <summary>
    /// EN: Gets or sets the command used to retrieve rows for this data adapter.
    /// PT: Obtém ou define o comando usado para consultar linhas neste adaptador.
    /// </summary>
    public new OracleCommandMock? SelectCommand
    {
        get => base.SelectCommand as OracleCommandMock;
        set => base.SelectCommand = value;
    }

    /// <summary>
    /// EN: Gets or sets the command used to update rows during data adapter updates.
    /// PT: Obtém ou define o comando usado para atualizar linhas durante atualizações do adaptador.
    /// </summary>
    public new OracleCommandMock? UpdateCommand
    {
        get => base.UpdateCommand as OracleCommandMock;
        set => base.UpdateCommand = value;
    }

    /// <summary>
    /// EN: Represents a provider-specific data adapter mock with typed command accessors.
    /// PT: Representa um simulado de adaptador de dados específico do provedor com acessores tipados de comando.
    /// </summary>
    public OracleDataAdapterMock()
    {
    }

    /// <summary>
    /// EN: Represents a provider-specific data adapter mock with typed command accessors.
    /// PT: Representa um simulado de adaptador de dados específico do provedor com acessores tipados de comando.
    /// </summary>
    public OracleDataAdapterMock(OracleCommandMock selectCommand) => SelectCommand = selectCommand;

    /// <summary>
    /// EN: Represents a provider-specific data adapter mock with typed command accessors.
    /// PT: Representa um simulado de adaptador de dados específico do provedor com acessores tipados de comando.
    /// </summary>
    public OracleDataAdapterMock(string selectCommandText, OracleConnectionMock connection)
        => SelectCommand = new OracleCommandMock(connection) { CommandText = selectCommandText };
}
