using DbDataAdapter = System.Data.Common.DbDataAdapter;

namespace DbSqlLikeMem.Firebird;

/// <summary>
/// EN: Represents the Firebird data adapter mock type used by provider mocks.
/// PT: Representa o adaptador de dados simulado do Firebird usado pelos mocks do provedor.
/// </summary>
public sealed class FirebirdDataAdapterMock : DbDataAdapter
{
    private bool loadingDefaults;

    private int updateBatchSize;

    /// <summary>
    /// EN: Gets or sets the command used to delete rows during data adapter updates.
    /// PT: Obtém ou define o comando usado para excluir linhas durante atualizações do adaptador.
    /// </summary>
    public new FirebirdCommandMock? DeleteCommand
    {
        get => base.DeleteCommand as FirebirdCommandMock;
        set => base.DeleteCommand = value;
    }

    /// <summary>
    /// EN: Gets or sets the command used to insert rows during data adapter updates.
    /// PT: Obtém ou define o comando usado para inserir linhas durante atualizações do adaptador.
    /// </summary>
    public new FirebirdCommandMock? InsertCommand
    {
        get => base.InsertCommand as FirebirdCommandMock;
        set => base.InsertCommand = value;
    }

    /// <summary>
    /// EN: Gets or sets the command used to retrieve rows for this data adapter.
    /// PT: Obtém ou define o comando usado para consultar linhas neste adaptador.
    /// </summary>
    public new FirebirdCommandMock? SelectCommand
    {
        get => base.SelectCommand as FirebirdCommandMock;
        set => base.SelectCommand = value;
    }

    /// <summary>
    /// EN: Gets or sets the command used to update rows during data adapter updates.
    /// PT: Obtém ou define o comando usado para atualizar linhas durante atualizações do adaptador.
    /// </summary>
    public new FirebirdCommandMock? UpdateCommand
    {
        get => base.UpdateCommand as FirebirdCommandMock;
        set => base.UpdateCommand = value;
    }

    /// <summary>
    /// EN: Gets or sets whether the adapter should use its default load settings.
    /// PT: Obtém ou define se o adaptador deve usar suas configurações padrão de carga.
    /// </summary>
    internal bool LoadDefaults
    {
        get => loadingDefaults;
        set => loadingDefaults = value;
    }

    /// <summary>
    /// EN: Gets or sets the number of rows processed per update batch.
    /// PT: Obtém ou define o número de linhas processadas por lote de atualização.
    /// </summary>
    public override int UpdateBatchSize
    {
        get => updateBatchSize;
        set => updateBatchSize = value;
    }

    /// <summary>
    /// EN: Represents a provider-specific data adapter mock with typed command accessors.
    /// PT: Representa um simulado de adaptador de dados específico do provedor com acessores tipados de comando.
    /// </summary>
    public FirebirdDataAdapterMock()
    {
        loadingDefaults = true;
        updateBatchSize = 1;
    }

    /// <summary>
    /// EN: Represents a provider-specific data adapter mock with typed command accessors.
    /// PT: Representa um simulado de adaptador de dados específico do provedor com acessores tipados de comando.
    /// </summary>
    public FirebirdDataAdapterMock(FirebirdCommandMock selectCommand) => SelectCommand = selectCommand;

    /// <summary>
    /// EN: Represents a provider-specific data adapter mock with typed command accessors.
    /// PT: Representa um simulado de adaptador de dados específico do provedor com acessores tipados de comando.
    /// </summary>
    public FirebirdDataAdapterMock(string selectCommandText, FirebirdConnectionMock connection)
        => SelectCommand = new FirebirdCommandMock(connection) { CommandText = selectCommandText };
}

