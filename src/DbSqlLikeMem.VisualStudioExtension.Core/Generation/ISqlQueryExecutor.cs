using DbSqlLikeMem.VisualStudioExtension.Core.Models;

namespace DbSqlLikeMem.VisualStudioExtension.Core.Generation;

public interface ISqlQueryExecutor
{
    Task<IReadOnlyCollection<IReadOnlyDictionary<string, object?>>> QueryAsync(
        ConnectionDefinition connection,
        string sql,
        IReadOnlyDictionary<string, object?> parameters,
        CancellationToken cancellationToken = default);
}
