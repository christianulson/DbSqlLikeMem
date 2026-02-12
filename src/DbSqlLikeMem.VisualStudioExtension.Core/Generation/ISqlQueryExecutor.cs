using DbSqlLikeMem.VisualStudioExtension.Core.Models;

namespace DbSqlLikeMem.VisualStudioExtension.Core.Generation;

/// <summary>
/// Executes SQL metadata queries using a configured connection.
/// Executa consultas SQL de metadados usando uma conexão configurada.
/// </summary>
public interface ISqlQueryExecutor
{
    /// <summary>
    /// Executes a SQL query and returns rows as key/value dictionaries.
    /// Executa uma consulta SQL e retorna linhas como dicionários chave/valor.
    /// </summary>
    Task<IReadOnlyCollection<IReadOnlyDictionary<string, object?>>> QueryAsync(
        ConnectionDefinition connection,
        string sql,
        IReadOnlyDictionary<string, object?> parameters,
        CancellationToken cancellationToken = default);
}
