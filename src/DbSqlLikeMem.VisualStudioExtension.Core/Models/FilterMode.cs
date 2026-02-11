namespace DbSqlLikeMem.VisualStudioExtension.Core.Models;

/// <summary>
/// Defines object filter matching behavior.
/// Define o comportamento de correspondência do filtro de objetos.
/// </summary>
public enum FilterMode
{
    /// <summary>
    /// Exact match.
    /// Correspondência exata.
    /// </summary>
    Equals,
    /// <summary>
    /// Contains/like match.
    /// Correspondência por contém/like.
    /// </summary>
    Like
}
