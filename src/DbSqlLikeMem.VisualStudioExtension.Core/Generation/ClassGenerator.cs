using DbSqlLikeMem.VisualStudioExtension.Core.Models;
using System.Globalization;
using System.Text;

namespace DbSqlLikeMem.VisualStudioExtension.Core.Generation;

public sealed class ClassGenerator
{
    public async Task<IReadOnlyCollection<string>> GenerateAsync(
        GenerationRequest request,
        ConnectionMappingConfiguration configuration,
        Func<DatabaseObjectReference, string> classContentFactory,
        CancellationToken cancellationToken = default)
    {
        var writtenFiles = new List<string>();

        foreach (var dbObject in request.SelectedObjects)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (!configuration.Mappings.TryGetValue(dbObject.Type, out var mapping))
            {
                continue;
            }

            Directory.CreateDirectory(mapping.OutputDirectory);
            var fileName = ResolveFileName(mapping.FileNamePattern, request.Connection, dbObject);
            var fullPath = Path.Combine(mapping.OutputDirectory, fileName);
            var content = classContentFactory(dbObject);

            await File.WriteAllTextAsync(fullPath, content, cancellationToken);
            writtenFiles.Add(fullPath);
        }

        return writtenFiles;
    }

    private static string ResolveFileName(string fileNamePattern, ConnectionDefinition connection, DatabaseObjectReference dbObject)
    {
        var safePattern = string.IsNullOrWhiteSpace(fileNamePattern)
            ? "{NamePascal}{Type}Factory.cs"
            : fileNamePattern;

        var namePascal = ToPascalCase(dbObject.Name);
        var typeName = dbObject.Type.ToString();

        return safePattern
            .Replace("{NamePascal}", namePascal, StringComparison.OrdinalIgnoreCase)
            .Replace("{Name}", dbObject.Name, StringComparison.OrdinalIgnoreCase)
            .Replace("{Type}", typeName, StringComparison.OrdinalIgnoreCase)
            .Replace("{Schema}", dbObject.Schema, StringComparison.OrdinalIgnoreCase)
            .Replace("{DatabaseType}", connection.DatabaseType, StringComparison.OrdinalIgnoreCase)
            .Replace("{DatabaseName}", connection.DatabaseName, StringComparison.OrdinalIgnoreCase);
    }

    private static string ToPascalCase(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return "Object";
        }

        var parts = value
            .Split(['_', '-', '.', ' '], StringSplitOptions.RemoveEmptyEntries)
            .Select(Capitalize)
            .Where(static part => !string.IsNullOrWhiteSpace(part));

        var joined = string.Concat(parts);
        if (!string.IsNullOrWhiteSpace(joined))
        {
            return joined;
        }

        var filtered = new string(value.Where(char.IsLetterOrDigit).ToArray());
        return string.IsNullOrWhiteSpace(filtered) ? "Object" : Capitalize(filtered);
    }

    private static string Capitalize(string part)
    {
        if (string.IsNullOrWhiteSpace(part))
        {
            return string.Empty;
        }

        var normalized = new StringBuilder(part.Length);
        foreach (var ch in part)
        {
            if (char.IsLetterOrDigit(ch))
            {
                normalized.Append(ch);
            }
        }

        if (normalized.Length == 0)
        {
            return string.Empty;
        }

        var cleaned = normalized.ToString();
        if (cleaned.Length == 1)
        {
            return cleaned.ToUpperInvariant();
        }

        return string.Concat(
            char.ToUpper(cleaned[0], CultureInfo.InvariantCulture),
            cleaned[1..]);
    }
}
