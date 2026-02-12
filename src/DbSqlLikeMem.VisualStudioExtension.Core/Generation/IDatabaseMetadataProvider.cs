using DbSqlLikeMem.VisualStudioExtension.Core.Models;

namespace DbSqlLikeMem.VisualStudioExtension.Core.Generation;

/// <summary>
/// Provides database metadata for object listing and details.
/// Fornece metadados de banco para listagem e detalhes de objetos.
/// </summary>
public interface IDatabaseMetadataProvider
{
    /// <summary>
    /// Lists available database objects for a connection.
    /// Lista os objetos de banco disponíveis para uma conexão.
    /// </summary>
    Task<IReadOnlyCollection<DatabaseObjectReference>> ListObjectsAsync(ConnectionDefinition connection, CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets detailed metadata for a specific database object.
    /// Obtém metadados detalhados para um objeto de banco específico.
    /// </summary>
    Task<DatabaseObjectReference?> GetObjectAsync(ConnectionDefinition connection, DatabaseObjectReference reference, CancellationToken cancellationToken = default);
}
