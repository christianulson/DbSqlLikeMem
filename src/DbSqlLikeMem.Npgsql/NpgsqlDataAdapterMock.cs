using DbDataAdapter = System.Data.Common.DbDataAdapter;
namespace DbSqlLikeMem.Npgsql;

/// <summary>
/// EN: Represents the Npgsql Data Adapter Mock type used by provider mocks.
/// PT: Representa o tipo Npgsql adaptador de dados simulado usado pelos mocks do provedor.
/// </summary>
public sealed class NpgsqlDataAdapterMock : DbDataAdapter
{
    /// <summary>
    /// EN: Executes delete command.
    /// PT: Executa delete comando.
    /// </summary>
    public new NpgsqlCommandMock? DeleteCommand
    {
        get => base.DeleteCommand as NpgsqlCommandMock;
        set => base.DeleteCommand = value;
    }

    /// <summary>
    /// EN: Executes insert command.
    /// PT: Executa insert comando.
    /// </summary>
    public new NpgsqlCommandMock? InsertCommand
    {
        get => base.InsertCommand as NpgsqlCommandMock;
        set => base.InsertCommand = value;
    }

    /// <summary>
    /// EN: Executes select command.
    /// PT: Executa select comando.
    /// </summary>
    public new NpgsqlCommandMock? SelectCommand
    {
        get => base.SelectCommand as NpgsqlCommandMock;
        set => base.SelectCommand = value;
    }

    /// <summary>
    /// EN: Executes update command.
    /// PT: Executa update comando.
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
