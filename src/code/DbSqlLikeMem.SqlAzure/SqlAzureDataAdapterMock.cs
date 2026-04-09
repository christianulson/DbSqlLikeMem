namespace DbSqlLikeMem.SqlAzure;

/// <summary>
/// EN: Represents the Sql Azure Data Adapter Mock type used by provider mocks.
/// PT: Representa o adaptador de dados simulado do SQL Azure usado pelos mocks do provedor.
/// </summary>
public sealed class SqlAzureDataAdapterMock : DbDataAdapter
{
    /// <summary>
    /// EN: Gets or sets the typed SQL Azure command used to delete rows.
    /// PT: Obtem ou define o comando tipado do SQL Azure usado para excluir linhas.
    /// </summary>
    public new SqlAzureCommandMock? DeleteCommand
    {
        get => base.DeleteCommand as SqlAzureCommandMock;
        set => base.DeleteCommand = value;
    }

    /// <summary>
    /// EN: Gets or sets the typed SQL Azure command used to insert rows.
    /// PT: Obtem ou define o comando tipado do SQL Azure usado para inserir linhas.
    /// </summary>
    public new SqlAzureCommandMock? InsertCommand
    {
        get => base.InsertCommand as SqlAzureCommandMock;
        set => base.InsertCommand = value;
    }

    /// <summary>
    /// EN: Gets or sets the typed SQL Azure command used to select rows.
    /// PT: Obtem ou define o comando tipado do SQL Azure usado para selecionar linhas.
    /// </summary>
    public new SqlAzureCommandMock? SelectCommand
    {
        get => base.SelectCommand as SqlAzureCommandMock;
        set => base.SelectCommand = value;
    }

    /// <summary>
    /// EN: Gets or sets the typed SQL Azure command used to update rows.
    /// PT: Obtem ou define o comando tipado do SQL Azure usado para atualizar linhas.
    /// </summary>
    public new SqlAzureCommandMock? UpdateCommand
    {
        get => base.UpdateCommand as SqlAzureCommandMock;
        set => base.UpdateCommand = value;
    }

    /// <summary>
    /// EN: Creates an empty SQL Azure data adapter mock.
    /// PT: Cria um adaptador de dados simulado do SQL Azure vazio.
    /// </summary>
    public SqlAzureDataAdapterMock()
    {
    }

    /// <summary>
    /// EN: Creates a SQL Azure data adapter mock using the provided select command.
    /// PT: Cria um adaptador de dados simulado do SQL Azure usando o comando de selecao informado.
    /// </summary>
    public SqlAzureDataAdapterMock(SqlAzureCommandMock selectCommand) => SelectCommand = selectCommand;

    /// <summary>
    /// EN: Creates a SQL Azure data adapter mock from select command text and connection.
    /// PT: Cria um adaptador de dados simulado do SQL Azure a partir do texto de selecao e da conexao.
    /// </summary>
    public SqlAzureDataAdapterMock(string selectCommandText, SqlAzureConnectionMock connection)
        => SelectCommand = new SqlAzureCommandMock(connection) { CommandText = selectCommandText };
}