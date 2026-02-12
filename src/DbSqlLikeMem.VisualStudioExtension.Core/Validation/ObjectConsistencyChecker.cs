using DbSqlLikeMem.VisualStudioExtension.Core.Generation;
using DbSqlLikeMem.VisualStudioExtension.Core.Models;

namespace DbSqlLikeMem.VisualStudioExtension.Core.Validation;

/// <summary>
/// Represents this public API type.
/// Representa este tipo público da API.
/// </summary>
public sealed class ObjectConsistencyChecker
{
    /// <summary>
    /// Executes this API operation.
    /// Executa esta operação da API.
    /// </summary>
    public async Task<ObjectHealthResult> CheckAsync(
        ConnectionDefinition connection,
        LocalObjectSnapshot snapshot,
        IDatabaseMetadataProvider provider,
        CancellationToken cancellationToken = default)
    {
        var databaseObject = await provider.GetObjectAsync(connection, snapshot.Reference, cancellationToken);

        if (databaseObject is null)
        {
            return new ObjectHealthResult(snapshot.Reference, snapshot.FilePath, ObjectHealthStatus.MissingInDatabase,
                "Objeto não existe mais na base.");
        }

        var localProperties = snapshot.Properties ?? new Dictionary<string, string>();
        var dbProperties = databaseObject.Properties ?? new Dictionary<string, string>();
        var isSame = HaveSameProperties(localProperties, dbProperties);

        return isSame
            ? new ObjectHealthResult(snapshot.Reference, snapshot.FilePath, ObjectHealthStatus.Synchronized)
            : new ObjectHealthResult(snapshot.Reference, snapshot.FilePath, ObjectHealthStatus.DifferentFromDatabase,
                "Arquivo local diferente das propriedades atuais no banco.");
    }

    private static bool HaveSameProperties(IReadOnlyDictionary<string, string> left, IReadOnlyDictionary<string, string> right)
    {
        if (left.Count != right.Count)
        {
            return false;
        }

        foreach (var pair in left)
        {
            if (!right.TryGetValue(pair.Key, out var value))
            {
                return false;
            }

            if (!string.Equals(pair.Value, value, StringComparison.Ordinal))
            {
                return false;
            }
        }

        return true;
    }
}
