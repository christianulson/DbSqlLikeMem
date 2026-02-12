namespace DbSqlLikeMem.VisualStudioExtension.Core.Models;

/// <summary>
/// Defines synchronization status between local and database objects.
/// Define o status de sincronização entre objetos locais e do banco.
/// </summary>
public enum ObjectHealthStatus
{
    /// <summary>
    /// The object was not found in the database.
    /// O objeto não foi encontrado no banco.
    /// </summary>
    MissingInDatabase,
    /// <summary>
    /// The local generated artifacts were not found.
    /// Os artefatos locais gerados não foram encontrados.
    /// </summary>
    MissingLocalArtifacts,
    /// <summary>
    /// The object exists but differs from the database metadata.
    /// O objeto existe, mas difere dos metadados do banco.
    /// </summary>
    DifferentFromDatabase,
    /// <summary>
    /// The object is synchronized with database metadata.
    /// O objeto está sincronizado com os metadados do banco.
    /// </summary>
    Synchronized
}
