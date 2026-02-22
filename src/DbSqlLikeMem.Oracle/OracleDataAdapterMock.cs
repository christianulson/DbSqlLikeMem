namespace DbSqlLikeMem.Oracle;

/// <summary>
/// EN: Represents the Oracle Data Adapter Mock type used by provider mocks.
/// PT: Representa o tipo Oracle adaptador de dados simulado usado pelos mocks do provedor.
/// </summary>
public sealed class OracleDataAdapterMock : DbDataAdapter
{
    /// <summary>
    /// EN: Executes delete command.
    /// PT: Executa delete comando.
    /// </summary>
    public new OracleCommandMock? DeleteCommand
    {
        get => base.DeleteCommand as OracleCommandMock;
        set => base.DeleteCommand = value;
    }

    /// <summary>
    /// EN: Executes insert command.
    /// PT: Executa insert comando.
    /// </summary>
    public new OracleCommandMock? InsertCommand
    {
        get => base.InsertCommand as OracleCommandMock;
        set => base.InsertCommand = value;
    }

    /// <summary>
    /// EN: Executes select command.
    /// PT: Executa select comando.
    /// </summary>
    public new OracleCommandMock? SelectCommand
    {
        get => base.SelectCommand as OracleCommandMock;
        set => base.SelectCommand = value;
    }

    /// <summary>
    /// EN: Executes update command.
    /// PT: Executa update comando.
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
