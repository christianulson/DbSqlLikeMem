using DbSqlLikeMem.VisualStudioExtension.Core.Models;

namespace DbSqlLikeMem.VisualStudioExtension.Core.Validation;

/// <summary>
/// EN: Reads embedded snapshot metadata from generated class files.
/// PT-br: Le os metadados de snapshot embutidos em arquivos de classe gerados.
/// </summary>
public static class GeneratedClassSnapshotReader
{
    /// <summary>
    /// EN: Reads a generated file and returns the snapshot metadata embedded in it.
    /// PT-br: Le um arquivo gerado e retorna os metadados de snapshot embutidos nele.
    /// </summary>
    public static async Task<LocalObjectSnapshot> ReadAsync(
        string filePath,
        DatabaseObjectReference fallbackReference,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var lines = await Task.Run(() => File.ReadAllLines(filePath), cancellationToken);
        var metadata = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        foreach (var line in lines)
        {
            const string prefix = "// DBSqlLikeMem:";
            if (!line.StartsWith(prefix, StringComparison.Ordinal))
            {
                continue;
            }

            var payload = line.Substring(prefix.Length);
            var kv = payload.Split(['='], 2, StringSplitOptions.None);
            if (kv.Length == 2)
            {
                kv[0] = kv[0].Trim();
                kv[1] = kv[1].Trim();
            }
            if (kv.Length == 2)
            {
                metadata[kv[0]] = kv[1];
            }
        }

        var reference = new DatabaseObjectReference(
            metadata.TryGetValue("Schema", out var schema) ? schema : fallbackReference.Schema,
            metadata.TryGetValue("Object", out var name) ? name : fallbackReference.Name,
            metadata.TryGetValue("Type", out var typeRaw) && Enum.TryParse<DatabaseObjectType>(typeRaw, true, out var parsedType)
                ? parsedType
                : fallbackReference.Type,
            "public",
            BuildProperties(metadata, fallbackReference.Properties));

        return new LocalObjectSnapshot(reference, filePath, reference.Properties);
    }

    private static IReadOnlyDictionary<string, string>? BuildProperties(
        IReadOnlyDictionary<string, string> metadata,
        IReadOnlyDictionary<string, string>? fallback)
    {
        var props = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var key in new[]
        {
            "Columns",
            "PrimaryKey",
            "Indexes",
            "ForeignKeys",
            "Triggers",
            "RequiredIn",
            "OptionalIn",
            "OutParams",
            "ReturnParam",
            "Parameters",
            "ReturnTypeSql",
            "BodySql",
            "StartValue",
            "IncrementBy",
            "CurrentValue"
        })
        {
            if (metadata.TryGetValue(key, out var value))
            {
                props[key] = value;
            }
        }

        if (props.Count > 0)
        {
            return props;
        }

        return fallback;
    }
}
