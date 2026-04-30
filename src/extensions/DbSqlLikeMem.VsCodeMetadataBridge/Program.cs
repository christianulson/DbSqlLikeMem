using System.Text.Json;
using System.Text.Json.Serialization;
using DbSqlLikeMem.VisualStudioExtension.Core.Models;

namespace DbSqlLikeMem.VsCodeMetadataBridge;

internal static class Program
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
    };

    public static async Task<int> Main(string[] args)
    {
        var options = BridgeCommandOptions.Parse(args);
        if (string.IsNullOrWhiteSpace(options.Operation))
        {
            return WriteError("Missing --operation. Use list-objects or test-connection.");
        }

        if (string.IsNullOrWhiteSpace(options.DatabaseType) || string.IsNullOrWhiteSpace(options.ConnectionString))
        {
            return WriteError("Missing --database-type or --connection-string.");
        }

        var connection = new ConnectionDefinition(
            "bridge",
            options.DatabaseType,
            options.DatabaseName ?? string.Empty,
            options.ConnectionString,
            options.DisplayName);

        var executor = new BridgeSqlQueryExecutor();
        var provider = new BridgeMetadataProvider(executor);

        try
        {
            if (string.Equals(options.Operation, "test-connection", StringComparison.OrdinalIgnoreCase))
            {
                await provider.TestConnectionAsync(connection, options.CancellationToken);
                await WriteJsonAsync(new BridgeResponse(true));
                return 0;
            }

            if (string.Equals(options.Operation, "list-objects", StringComparison.OrdinalIgnoreCase))
            {
                var objects = await provider.GetObjectsAsync(connection, options.CancellationToken);
                await WriteJsonAsync(new BridgeResponse(true, Objects: objects));
                return 0;
            }

            return WriteError($"Unsupported operation: {options.Operation}");
        }
        catch (Exception ex)
        {
            await WriteJsonAsync(new BridgeResponse(false, ex.Message));
            Console.Error.WriteLine(ex);
            return 1;
        }
    }

    private static Task WriteJsonAsync(BridgeResponse response)
    {
        var json = JsonSerializer.Serialize(response, JsonOptions);
        return Console.Out.WriteLineAsync(json);
    }

    private static int WriteError(string message)
    {
        var response = new BridgeResponse(false, message);
        Console.Out.WriteLine(JsonSerializer.Serialize(response, JsonOptions));
        return 1;
    }
}

internal sealed record BridgeCommandOptions(
    string Operation,
    string DatabaseType,
    string DatabaseName,
    string ConnectionString,
    string? DisplayName,
    CancellationToken CancellationToken)
{
    public static BridgeCommandOptions Parse(string[] args)
    {
        string? operation = null;
        string? databaseType = null;
        string? databaseName = null;
        string? connectionString = null;
        string? displayName = null;

        for (var i = 0; i < args.Length; i++)
        {
            var arg = args[i];
            var key = arg;
            var value = string.Empty;

            var equalsIndex = arg.IndexOf('=');
            if (equalsIndex > 0)
            {
                key = arg[..equalsIndex];
                value = arg[(equalsIndex + 1)..];
            }
            else if (i + 1 < args.Length && !args[i + 1].StartsWith("-", StringComparison.Ordinal))
            {
                value = args[++i];
            }

            switch (key.TrimStart('-').ToLowerInvariant())
            {
                case "operation":
                    operation = value;
                    break;
                case "database-type":
                case "database_type":
                    databaseType = value;
                    break;
                case "database-name":
                case "database_name":
                    databaseName = value;
                    break;
                case "connection-string":
                case "connection_string":
                    connectionString = value;
                    break;
                case "display-name":
                case "display_name":
                    displayName = value;
                    break;
                case "help":
                case "h":
                case "?":
                    break;
            }
        }

        return new BridgeCommandOptions(operation ?? string.Empty, databaseType ?? string.Empty, databaseName ?? string.Empty, connectionString ?? string.Empty, displayName, CancellationToken.None);
    }
}

internal sealed record BridgeResponse(bool Success, string? Message = null, IReadOnlyList<BridgeDatabaseObjectReference>? Objects = null);

internal sealed record BridgeDatabaseObjectReference(
    string Schema,
    string Name,
    string ObjectType,
    IReadOnlyList<BridgeColumnReference>? Columns = null,
    IReadOnlyList<BridgeForeignKeyReference>? ForeignKeys = null,
    BridgeSequenceMetadataReference? SequenceMetadata = null,
    string? RequiredIn = null,
    string? OptionalIn = null,
    string? OutParams = null,
    string? ReturnParam = null,
    string? Parameters = null,
    string? ReturnTypeSql = null,
    string? BodySql = null);

internal sealed record BridgeColumnReference(
    string Name,
    string DataType,
    bool IsNullable,
    int OrdinalPosition);

internal sealed record BridgeForeignKeyReference(
    string Name,
    string ReferencedSchema,
    string ReferencedTable);

internal sealed record BridgeSequenceMetadataReference(
    string? StartValue,
    string? IncrementBy,
    string? CurrentValue);
