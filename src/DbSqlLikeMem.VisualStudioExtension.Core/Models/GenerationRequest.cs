namespace DbSqlLikeMem.VisualStudioExtension.Core.Models;

/// <summary>
/// Represents an input request for code generation.
/// Representa uma solicitação de entrada para geração de código.
/// </summary>
public sealed record GenerationRequest
{
    /// <summary>
    /// Initializes a new generation request.
    /// Inicializa uma nova solicitação de geração.
    /// </summary>
    public GenerationRequest(ConnectionDefinition connection, IReadOnlyCollection<DatabaseObjectReference> selectedObjects)
    {
        Connection = connection;
        SelectedObjects = selectedObjects;
    }

    /// <summary>
    /// Gets the source database connection definition.
    /// Obtém a definição da conexão de banco de origem.
    /// </summary>
    public ConnectionDefinition Connection { get; }

    /// <summary>
    /// Gets selected database objects.
    /// Obtém os objetos de banco selecionados.
    /// </summary>
    public IReadOnlyCollection<DatabaseObjectReference> SelectedObjects { get; }
}
