namespace DbSqlLikeMem.VisualStudioExtension.Core.Models;

/// <summary>
/// Represents the consistency status of a generated object.
/// Representa o status de consistência de um objeto gerado.
/// </summary>
public sealed record ObjectHealthResult
{
    /// <summary>
    /// Initializes a new object health result.
    /// Inicializa um novo resultado de saúde do objeto.
    /// </summary>
    public ObjectHealthResult(DatabaseObjectReference databaseObject, string localFilePath, ObjectHealthStatus status, string? message = null)
    {
        DatabaseObject = databaseObject;
        LocalFilePath = localFilePath;
        Status = status;
        Message = message;
    }

    /// <summary>
    /// Gets the database object reference.
    /// Obtém a referência do objeto de banco.
    /// </summary>
    public DatabaseObjectReference DatabaseObject { get; }

    /// <summary>
    /// Gets the local generated file path.
    /// Obtém o caminho local do arquivo gerado.
    /// </summary>
    public string LocalFilePath { get; }

    /// <summary>
    /// Gets the health status.
    /// Obtém o status de saúde.
    /// </summary>
    public ObjectHealthStatus Status { get; }

    /// <summary>
    /// Gets an optional diagnostic message.
    /// Obtém uma mensagem de diagnóstico opcional.
    /// </summary>
    public string? Message { get; }
}
