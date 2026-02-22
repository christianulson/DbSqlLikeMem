namespace DbSqlLikeMem.Db2;

/// <summary>
/// EN: Represents the Db2 Data Adapter Mock type used by provider mocks.
/// PT: Representa o tipo Db2 adaptador de dados simulado usado pelos mocks do provedor.
/// </summary>
public sealed class Db2DataAdapterMock : DbDataAdapter
{
    /// <summary>
    /// EN: Executes delete command.
    /// PT: Executa delete comando.
    /// </summary>
    public new Db2CommandMock? DeleteCommand
    {
        get => base.DeleteCommand as Db2CommandMock;
        set => base.DeleteCommand = value;
    }

    /// <summary>
    /// EN: Executes insert command.
    /// PT: Executa insert comando.
    /// </summary>
    public new Db2CommandMock? InsertCommand
    {
        get => base.InsertCommand as Db2CommandMock;
        set => base.InsertCommand = value;
    }

    /// <summary>
    /// EN: Executes select command.
    /// PT: Executa select comando.
    /// </summary>
    public new Db2CommandMock? SelectCommand
    {
        get => base.SelectCommand as Db2CommandMock;
        set => base.SelectCommand = value;
    }

    /// <summary>
    /// EN: Executes update command.
    /// PT: Executa update comando.
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
