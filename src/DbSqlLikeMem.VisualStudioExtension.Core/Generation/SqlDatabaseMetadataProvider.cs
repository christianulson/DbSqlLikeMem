using DbSqlLikeMem.VisualStudioExtension.Core.Models;
using System.Data.Common;
using System.Globalization;

namespace DbSqlLikeMem.VisualStudioExtension.Core.Generation;

/// <summary>
/// Represents this public API type.
/// Representa este tipo público da API.
/// </summary>
public sealed class SqlDatabaseMetadataProvider : IDatabaseMetadataProvider
{
    private readonly ISqlQueryExecutor queryExecutor;

    /// <summary>
    /// Initializes a metadata provider backed by a SQL query executor.
    /// Inicializa um provedor de metadados baseado em um executor de consultas SQL.
    /// </summary>
    public SqlDatabaseMetadataProvider(ISqlQueryExecutor queryExecutor)
    {
        this.queryExecutor = queryExecutor;
    }
    /// <inheritdoc/>
    public async Task<IReadOnlyCollection<DatabaseObjectReference>> ListObjectsAsync(
        ConnectionDefinition connection,
        CancellationToken cancellationToken = default)
    {
        var sql = SqlMetadataQueryFactory.BuildListObjectsQuery(connection.DatabaseType);
        var rows = await queryExecutor.QueryAsync(
            connection,
            sql,
            new Dictionary<string, object?> { ["databaseName"] = ResolveDatabaseNameForMetadata(connection) },
            cancellationToken);

        return [.. rows.Select(MapObject).Where(x => x is not null).Cast<DatabaseObjectReference>()];
    }

    /// <inheritdoc/>
    public async Task<DatabaseObjectReference?> GetObjectAsync(
        ConnectionDefinition connection,
        DatabaseObjectReference reference,
        CancellationToken cancellationToken = default)
    {
        var listed = await ListObjectsAsync(connection, cancellationToken);
        var exists = listed.Any(o =>
            string.Equals(o.Schema, reference.Schema, StringComparison.OrdinalIgnoreCase) &&
            string.Equals(o.Name, reference.Name, StringComparison.OrdinalIgnoreCase) &&
            o.Type == reference.Type);

        if (!exists)
        {
            return null;
        }

        var args = new Dictionary<string, object?>
        {
            ["schemaName"] = reference.Schema,
            ["objectName"] = reference.Name
        };

        var columns = await queryExecutor.QueryAsync(connection, SqlMetadataQueryFactory.BuildObjectColumnsQuery(connection.DatabaseType), args, cancellationToken);
        var pks = await queryExecutor.QueryAsync(connection, SqlMetadataQueryFactory.BuildPrimaryKeyQuery(connection.DatabaseType), args, cancellationToken);
        var indexes = await queryExecutor.QueryAsync(connection, SqlMetadataQueryFactory.BuildIndexesQuery(connection.DatabaseType), args, cancellationToken);
        var fks = await queryExecutor.QueryAsync(connection, SqlMetadataQueryFactory.BuildForeignKeysQuery(connection.DatabaseType), args, cancellationToken);
        var triggers = await queryExecutor.QueryAsync(connection, SqlMetadataQueryFactory.BuildTriggersQuery(connection.DatabaseType), args, cancellationToken);

        var properties = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["Columns"] = SerializeColumns(columns),
            ["PrimaryKey"] = SerializePrimaryKey(pks),
            ["Indexes"] = SerializeIndexes(indexes),
            ["ForeignKeys"] = SerializeForeignKeys(fks),
            ["Triggers"] = SerializeTriggers(triggers)
        };

        return reference with { Properties = properties };
    }


    private static string ResolveDatabaseNameForMetadata(ConnectionDefinition connection)
    {
        if (!string.Equals(connection.DatabaseType, "MySql", StringComparison.OrdinalIgnoreCase)
            && !string.Equals(connection.DatabaseType, "SqlServer", StringComparison.OrdinalIgnoreCase)
            && !string.Equals(connection.DatabaseType, "PostgreSql", StringComparison.OrdinalIgnoreCase))
        {
            return connection.DatabaseName;
        }

        try
        {
            var builder = new DbConnectionStringBuilder { ConnectionString = connection.ConnectionString };
            if (TryReadDatabaseName(builder, out var parsedName))
            {
                return parsedName;
            }
        }
        catch
        {
            // Fallback to persisted value.
        }

        return connection.DatabaseName;
    }

    private static bool TryReadDatabaseName(DbConnectionStringBuilder builder, out string databaseName)
    {
        databaseName = string.Empty;
        foreach (var key in new[] { "Database", "Initial Catalog" })
        {
            if (builder.TryGetValue(key, out var value))
            {
                var candidate = Convert.ToString(value, CultureInfo.InvariantCulture)?.Trim();
                if (!string.IsNullOrWhiteSpace(candidate))
                {
                    databaseName = candidate!;
                    return true;
                }
            }
        }

        return false;
    }

    private static string SerializeColumns(IReadOnlyCollection<IReadOnlyDictionary<string, object?>> rows)
    {
        var cols = rows
            .Select(r => new
            {
                Name = ReadString(r, "ColumnName"),
                DataType = ReadString(r, "DataType"),
                Ordinal = ReadInt(r, "Ordinal"),
                IsNullable = ReadBoolFlexible(r, "IsNullable"),
                IsIdentity = ReadBoolFlexible(r, "IsIdentity") || ReadString(r, "Extra").Contains("identity", StringComparison.OrdinalIgnoreCase) || ReadString(r, "Extra").Contains("auto_increment", StringComparison.OrdinalIgnoreCase),
                DefaultValue = ReadString(r, "DefaultValue"),
                CharMaxLen = ReadNullableLong(r, "CharMaxLen"),
                NumPrecision = ReadNullableInt(r, "NumPrecision"),
                NumScale = ReadNullableInt(r, "NumScale"),
                ColumnType = ReadString(r, "ColumnType"),
                Generated = ReadString(r, "Generated")
            })
            .Where(c => !string.IsNullOrWhiteSpace(c.Name))
            .OrderBy(c => c.Ordinal)
            .Select(c => string.Join("|", [
                Escape(c.Name),
                Escape(c.DataType),
                c.Ordinal.ToString(CultureInfo.InvariantCulture),
                c.IsNullable ? "1" : "0",
                c.IsIdentity ? "1" : "0",
                Escape(c.DefaultValue),
                c.CharMaxLen?.ToString(CultureInfo.InvariantCulture) ?? string.Empty,
                c.NumScale?.ToString(CultureInfo.InvariantCulture) ?? string.Empty,
                Escape(c.ColumnType),
                Escape(c.Generated),
                c.NumPrecision?.ToString(CultureInfo.InvariantCulture) ?? string.Empty
            ]));

        return string.Join(";", cols);
    }

    private static string SerializePrimaryKey(IReadOnlyCollection<IReadOnlyDictionary<string, object?>> rows)
        => string.Join(",", rows.Select(r => ReadString(r, "ColumnName")).Where(s => !string.IsNullOrWhiteSpace(s)).Select(Escape));

    private static string SerializeIndexes(IReadOnlyCollection<IReadOnlyDictionary<string, object?>> rows)
    {
        var grouped = rows
            .Select(r => new
            {
                IndexName = ReadString(r, "IndexName"),
                IsUnique = ReadBoolFlexible(r, "IsUnique") || ReadString(r, "Uniqueness").Equals("UNIQUE", StringComparison.OrdinalIgnoreCase) || ReadString(r, "UniqueRule").Equals("U", StringComparison.OrdinalIgnoreCase) || ReadInt(r, "NonUnique") == 0,
                Column = ReadString(r, "ColumnName"),
                Seq = ReadInt(r, "Seq")
            })
            .Where(i => !string.IsNullOrWhiteSpace(i.IndexName) && !string.IsNullOrWhiteSpace(i.Column))
            .Where(i => !IsPrimaryIndexName(i.IndexName))
            .GroupBy(i => i.IndexName, StringComparer.OrdinalIgnoreCase)
            .Select(g =>
            {
                var cols = g.OrderBy(x => x.Seq).Select(x => Escape(x.Column));
                var unique = g.Any(x => x.IsUnique) ? "1" : "0";
                return $"{Escape(g.Key)}|{unique}|{string.Join(",", cols)}";
            });

        return string.Join(";", grouped);
    }

    private static bool IsPrimaryIndexName(string indexName)
        => indexName.Equals("PRIMARY", StringComparison.OrdinalIgnoreCase)
           || indexName.Equals("PK", StringComparison.OrdinalIgnoreCase)
           || indexName.StartsWith("PK_", StringComparison.OrdinalIgnoreCase);

    private static string SerializeTriggers(IReadOnlyCollection<IReadOnlyDictionary<string, object?>> rows)
        => string.Join(";", rows
            .Select(r => ReadString(r, "TriggerName"))
            .Where(t => !string.IsNullOrWhiteSpace(t))
            .Select(Escape));

    private static string SerializeForeignKeys(IReadOnlyCollection<IReadOnlyDictionary<string, object?>> rows)
    {
        var fks = rows
            .Select(r =>
            {
                var col = ReadString(r, "ColumnName");
                var refTable = ReadString(r, "RefTable");
                var refCol = ReadString(r, "RefColumn");
                return (col, refTable, refCol);
            })
            .Where(f => !string.IsNullOrWhiteSpace(f.col) && !string.IsNullOrWhiteSpace(f.refTable) && !string.IsNullOrWhiteSpace(f.refCol))
            .Select(f => $"{Escape(f.col)}|{Escape(f.refTable)}|{Escape(f.refCol)}");

        return string.Join(";", fks);
    }

    private static DatabaseObjectReference? MapObject(IReadOnlyDictionary<string, object?> row)
    {
        var schema = ReadString(row, "SchemaName");
        var name = ReadString(row, "ObjectName");
        var typeRaw = ReadString(row, "ObjectType");

        if (string.IsNullOrWhiteSpace(name) || string.IsNullOrWhiteSpace(typeRaw) || !Enum.TryParse<DatabaseObjectType>(typeRaw, true, out var type))
        {
            return null;
        }

        return new DatabaseObjectReference(schema, name, type);
    }

    private static bool ReadBoolFlexible(IReadOnlyDictionary<string, object?> row, string key)
    {
        var s = ReadString(row, key);
        if (string.IsNullOrWhiteSpace(s)) return false;
        return s.Equals("1", StringComparison.OrdinalIgnoreCase) || s.Equals("true", StringComparison.OrdinalIgnoreCase) || s.Equals("yes", StringComparison.OrdinalIgnoreCase) || s.Equals("y", StringComparison.OrdinalIgnoreCase);
    }

    private static long? ReadNullableLong(IReadOnlyDictionary<string, object?> row, string key)
    {
        var s = ReadString(row, key);
        if (string.IsNullOrWhiteSpace(s)) return null;
        return long.TryParse(s, NumberStyles.Integer, CultureInfo.InvariantCulture, out var n) ? n : null;
    }

    private static int? ReadNullableInt(IReadOnlyDictionary<string, object?> row, string key)
    {
        var s = ReadString(row, key);
        if (string.IsNullOrWhiteSpace(s)) return null;
        return int.TryParse(s, NumberStyles.Integer, CultureInfo.InvariantCulture, out var n) ? n : null;
    }

    private static int ReadInt(IReadOnlyDictionary<string, object?> row, string key)
        => ReadNullableInt(row, key) ?? 0;

    private static string ReadString(IReadOnlyDictionary<string, object?> row, string key)
    {
        foreach (var item in row)
        {
            if (string.Equals(item.Key, key, StringComparison.OrdinalIgnoreCase))
            {
                return Convert.ToString(item.Value, CultureInfo.InvariantCulture)?.Trim() ?? string.Empty;
            }
        }

        return string.Empty;
    }

    private static string Escape(string value)
        => value.Replace("\\", "\\\\").Replace("|", "\\|").Replace(";", "\\;").Replace(",", "\\,");
}
