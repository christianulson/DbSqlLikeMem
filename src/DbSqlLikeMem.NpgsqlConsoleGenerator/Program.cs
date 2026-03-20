using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Text;
using Dapper;
using DbSqlLikeMem.VisualStudioExtension.Core.Generation;
using DbSqlLikeMem.VisualStudioExtension.Core.Models;
using Microsoft.Extensions.Configuration;

namespace DbSqlLikeMem.NpgsqlConsoleGenerator;

static partial class Program
{
#pragma warning disable CA1303

#pragma warning disable  CA1812
    private sealed record DestinyInfo(
        string OutputPath,
        string Schema,
        string Namespace,
        string? ClassAccessibility = null,
        List<string>? Tables = null,
        List<string>? Sequences = null);

    private sealed record ConnectionInfo(
        string Name,
        string ProviderName,
        string Connection,
        bool? Enable,
        List<DestinyInfo> Destinies);

#pragma warning restore CA1812

    private const string DatabaseType = "postgresql";
    private const string DefaultProvider = "Npgsql";

    static void Main(string[] args)
    {
        var baseDirectory = Directory.GetCurrentDirectory()
            .Split("\\Tools")
            [0];
        Console.WriteLine($"BaseDirectory: {baseDirectory}");

        var configuration = new ConfigurationBuilder()
            .SetBasePath(Directory.GetCurrentDirectory())
            .AddJsonFile("appsettings.json")
            .Build();

        var connections = configuration
            .GetSection("ConnectionsString")
            .Get<List<ConnectionInfo>>()
            ?.Where(_ => !_.Enable.HasValue || _.Enable.Value)
            .ToList();

        if (connections == null || connections.Count == 0)
        {
            Console.WriteLine("No connection strings found in appsettings.json.");
            return;
        }

        bool runAll = args.Any(a => string.Equals(a, "--all", StringComparison.OrdinalIgnoreCase));

        int selectedConnectionIndex = 1;
        if (!runAll)
        {
            Console.WriteLine("Select a connection:");
            Console.WriteLine("0. Todas");
            for (int i = 0; i < connections.Count; i++)
                Console.WriteLine($"{i + 1}. {connections[i].Name}");

            Console.Write("Enter the number of the connection to use: ");
            if (!int.TryParse(Console.ReadLine(), out selectedConnectionIndex)
                || selectedConnectionIndex < 0 || selectedConnectionIndex > connections.Count)
            {
                Console.WriteLine("Invalid connection selection.");
                return;
            }

            if (selectedConnectionIndex == 0)
                runAll = true;
        }

        foreach (var connInfo in runAll
            ? connections
            : [connections[selectedConnectionIndex - 1]])
        {
            using var connection = CreateConnection(connInfo);
            connection.Open();

            foreach (var destiny in connInfo.Destinies)
            {
                var tables = (destiny.Tables != null && destiny.Tables.Count != 0)
                    ? destiny.Tables
                    : GetTablesInSchema(connection, destiny.Schema);
                var sequences = (destiny.Sequences != null && destiny.Sequences.Count != 0)
                    ? destiny.Sequences
                    : GetSequencesInSchema(connection, destiny.Schema);

                Console.WriteLine($"Schema: {destiny.Schema}");

                var outputPath = Path.Combine(baseDirectory, destiny.OutputPath);
                if ((tables.Count > 0 || sequences.Count > 0) && !Directory.Exists(outputPath))
                    Directory.CreateDirectory(outputPath);

                foreach (var tableName in tables.Distinct(StringComparer.OrdinalIgnoreCase))
                {
                    var clean = tableName.Trim();
                    if (string.IsNullOrEmpty(clean)) continue;

                    var meta = LoadTableMetadata(connection, destiny.Schema, clean);

                    GenerateTableFile(
                        destiny.Namespace,
                        destiny.ClassAccessibility,
                        tableName: clean,
                        columns: meta.Columns,
                        primaryKey: meta.PrimaryKey,
                        indexes: meta.Indexes,
                        foreignKeys: meta.ForeignKeys,
                        outputPath: outputPath);

                    Console.WriteLine($" - Tabela: {tableName} gerada.");
                }

                foreach (var sequenceName in sequences.Distinct(StringComparer.OrdinalIgnoreCase))
                {
                    var clean = sequenceName.Trim();
                    if (string.IsNullOrEmpty(clean)) continue;

                    var meta = LoadSequenceMetadata(connection, destiny.Schema, clean);
                    GenerateSequenceFile(destiny.Namespace, clean, destiny.Schema, meta, outputPath);
                    Console.WriteLine($" - Sequence: {sequenceName} gerada.");
                }
            }
        }

        Console.WriteLine("Table structure files have been generated.");
    }

    private static IDbConnection CreateConnection(ConnectionInfo connInfo)
    {
        var providerName = string.IsNullOrWhiteSpace(connInfo.ProviderName) ? DefaultProvider : connInfo.ProviderName;
        var factory = DbProviderFactories.GetFactory(providerName);
        var connection = factory.CreateConnection() ?? throw new InvalidOperationException($"Failed to create provider connection for '{providerName}'.");
        connection.ConnectionString = connInfo.Connection;
        return connection;
    }

    private static List<string> GetTablesInSchema(IDbConnection cn, string schema)
    {
        var qObjects = SqlMetadataQueryFactory.BuildListObjectsQuery(DatabaseType);
        var rows = cn.Query(qObjects, new { databaseName = schema })
            .Select(ToDictionary)
            .Where(r => ReadString(r, "ObjectType").Equals("Table", StringComparison.OrdinalIgnoreCase))
            .Where(r => string.IsNullOrWhiteSpace(schema) || ReadString(r, "SchemaName").Equals(schema, StringComparison.OrdinalIgnoreCase))
            .Select(r => ReadString(r, "ObjectName"))
            .Where(static name => !string.IsNullOrWhiteSpace(name))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(static name => name)
            .ToList();

        return rows;
    }

    private static List<string> GetSequencesInSchema(IDbConnection cn, string schema)
    {
        var qObjects = SqlMetadataQueryFactory.BuildListObjectsQuery(DatabaseType);
        var rows = cn.Query(qObjects, new { databaseName = schema })
            .Select(ToDictionary)
            .Where(r => ReadString(r, "ObjectType").Equals("Sequence", StringComparison.OrdinalIgnoreCase))
            .Where(r => string.IsNullOrWhiteSpace(schema) || ReadString(r, "SchemaName").Equals(schema, StringComparison.OrdinalIgnoreCase))
            .Select(r => ReadString(r, "ObjectName"))
            .Where(static name => !string.IsNullOrWhiteSpace(name))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(static name => name)
            .ToList();

        return rows;
    }

    private sealed record ColumnMeta(
        string ColumnName,
        string DataType,
        string ColumnType,
        bool IsNullable,
        bool IsIdentity,
        string? DefaultValue,
        int Ordinal,
        long? CharMaxLen,
        int? NumPrecision,
        int? NumScale,
        string? Generated);

    private sealed record TableMeta(
        List<ColumnMeta> Columns,
        List<string> PrimaryKey,
        Dictionary<string, (bool Unique, List<string> Cols)> Indexes,
        List<(string Col, string RefTable, string RefCol)> ForeignKeys);

    private sealed record SequenceMeta(
        long? StartValue,
        long? IncrementBy,
        long? CurrentValue);

    private static TableMeta LoadTableMetadata(IDbConnection cn, string schema, string table)
    {
        var args = new { schemaName = schema, objectName = table };

        var cols = cn.Query(SqlMetadataQueryFactory.BuildObjectColumnsQuery(DatabaseType), args)
            .Select(ToDictionary)
            .Select(row => new ColumnMeta(
                ColumnName: ReadString(row, "ColumnName"),
                DataType: ReadString(row, "DataType"),
                ColumnType: ReadString(row, "ColumnType"),
                IsNullable: ReadBoolFlexible(row, "IsNullable"),
                IsIdentity: ReadBoolFlexible(row, "IsIdentity") || ReadString(row, "Extra").Contains("identity", StringComparison.OrdinalIgnoreCase) || ReadString(row, "Extra").Contains("auto_increment", StringComparison.OrdinalIgnoreCase),
                DefaultValue: string.IsNullOrWhiteSpace(ReadString(row, "DefaultValue")) ? null : ReadString(row, "DefaultValue"),
                Ordinal: ReadInt(row, "Ordinal") - 1,
                CharMaxLen: ReadNullableLong(row, "CharMaxLen"),
                NumPrecision: ReadNullableInt(row, "NumPrecision"),
                NumScale: ReadNullableInt(row, "NumScale"),
                Generated: string.IsNullOrWhiteSpace(ReadString(row, "ColumnGenerated")) ? null : ReadString(row, "ColumnGenerated")))
            .Where(static c => !string.IsNullOrWhiteSpace(c.ColumnName))
            .OrderBy(static c => c.Ordinal)
            .ToList();

        var pk = cn.Query(SqlMetadataQueryFactory.BuildPrimaryKeyQuery(DatabaseType), args)
            .Select(ToDictionary)
            .Select(row => ReadString(row, "ColumnName"))
            .Where(static c => !string.IsNullOrWhiteSpace(c))
            .ToList();

        var idx = new Dictionary<string, (bool Unique, List<string> Cols)>(StringComparer.OrdinalIgnoreCase);
        foreach (var row in cn.Query(SqlMetadataQueryFactory.BuildIndexesQuery(DatabaseType), args).Select(ToDictionary))
        {
            var name = ReadString(row, "IndexName");
            var col = ReadString(row, "ColumnName");
            if (string.IsNullOrWhiteSpace(name) || string.IsNullOrWhiteSpace(col))
                continue;
            if (name.Equals(SqlConst.PRIMARY, StringComparison.OrdinalIgnoreCase)
                || name.Equals("PK", StringComparison.OrdinalIgnoreCase)
                || name.StartsWith("PK_", StringComparison.OrdinalIgnoreCase))
                continue;

            var unique = ReadBoolFlexible(row, "IsUnique")
                         || ReadString(row, "Uniqueness").Equals(SqlConst.UNIQUE, StringComparison.OrdinalIgnoreCase)
                         || ReadString(row, "UniqueRule").Equals("U", StringComparison.OrdinalIgnoreCase)
                         || ReadInt(row, "NonUnique") == 0;

            if (!idx.TryGetValue(name, out var tuple))
                tuple = (Unique: unique, Cols: []);

            tuple = (Unique: tuple.Unique || unique, tuple.Cols);
            tuple.Cols.Add(col);
            idx[name] = tuple;
        }

        var fks = cn.Query(SqlMetadataQueryFactory.BuildForeignKeysQuery(DatabaseType), args)
            .Select(ToDictionary)
            .Select(row => (
                Col: ReadString(row, "ColumnName"),
                RefTable: ReadString(row, "RefTable"),
                RefCol: ReadString(row, "RefColumn")))
            .Where(f => !string.IsNullOrWhiteSpace(f.Col) && !string.IsNullOrWhiteSpace(f.RefTable) && !string.IsNullOrWhiteSpace(f.RefCol))
            .ToList();

        return new TableMeta(cols, pk, idx, fks);
    }

    private static SequenceMeta LoadSequenceMetadata(IDbConnection cn, string schema, string sequence)
    {
        var args = new { schemaName = schema, objectName = sequence };
        var row = cn.Query(SqlMetadataQueryFactory.BuildSequenceMetadataQuery(DatabaseType), args)
            .Select(ToDictionary)
            .FirstOrDefault();

        return new SequenceMeta(
            StartValue: row is null ? null : ReadNullableLong(row, "StartValue"),
            IncrementBy: row is null ? null : ReadNullableLong(row, "IncrementBy"),
            CurrentValue: row is null ? null : ReadNullableLong(row, "CurrentValue"));
    }

    private static void GenerateTableFile(
        string ns,
        string? classAccessibility,
        string tableName,
        List<ColumnMeta> columns,
        List<string> primaryKey,
        Dictionary<string, (bool Unique, List<string> Cols)> indexes,
        List<(string Col, string RefTable, string RefCol)> foreignKeys,
        string outputPath)
    {
        var className = $"{GenerationRuleSet.ToPascalCase(tableName)}TableFactory";
        var methodName = $"CreateTable{GenerationRuleSet.ToPascalCase(tableName)}";
        var fileName = Path.Combine(outputPath, $"{className}.cs");

        using var w = new StreamWriter(fileName, false, new UTF8Encoding(encoderShouldEmitUTF8Identifier: false));

        w.WriteLine($"namespace {ns};");
        w.WriteLine();
        var normalizedAccessibility = string.Equals(classAccessibility, "public", StringComparison.OrdinalIgnoreCase)
            ? "public"
            : "internal";

        w.WriteLine($"{normalizedAccessibility} static class {className}");
        w.WriteLine("{");
        w.WriteLine($"    {normalizedAccessibility} static ITableMock {methodName}(this DbMock db)");
        w.WriteLine("    {");
        if (normalizedAccessibility == "public")
            w.WriteLine("        ArgumentNullException.ThrowIfNull(db);");

        w.WriteLine($"        var table = db.AddTable(\"{tableName}\");");

        foreach (var c in columns.OrderBy(c => c.Ordinal))
        {
            var dbType = GenerationRuleSet.MapDbType(c.DataType, c.CharMaxLen, c.NumPrecision, c.ColumnName, DatabaseType);
            var nullable = c.IsNullable ? "true" : "false";
            var ctor = $"DbType.{dbType}, {nullable}";

            if (c.IsIdentity) ctor += ", true";
            if (!string.IsNullOrEmpty(c.DefaultValue)
                && GenerationRuleSet.IsSimpleLiteralDefault(c.DefaultValue!))
                ctor += $", defaultValue: {GenerationRuleSet.FormatDefaultLiteral(c.DefaultValue!, dbType)}";
            if (c.CharMaxLen is > 0 and <= int.MaxValue)
                ctor += $", size: {(int)c.CharMaxLen}";
            if (c.NumScale is >= 0)
                ctor += $", decimalPlaces: {c.NumScale.Value}";

            var enums = GenerationRuleSet.TryParseEnumValues(c.ColumnType);
            if (enums.Length > 0)
                ctor += $", enumValues: [{string.Join(", ", enums.Select(GenerationRuleSet.Literal))}]";


            var col = $"        table.AddColumn(\"{c.ColumnName}\", {ctor})";
            if (!string.IsNullOrWhiteSpace(c.Generated))
            {
                if (!GenerationRuleSet.TryConvertIfIsNull(c.Generated, out var genCode))
                    throw new NotSupportedException($"Expressão não suportada: {c.Generated}");

                col += $"\n          .GetGenValue = {genCode}";
            }
            col += ";";
            w.WriteLine(col);
        }

        if (primaryKey.Count > 0)
        {
            w.WriteLine($"        table.AddPrimaryKeyIndexes({string.Join(",", primaryKey.Select(_ => $"\"{_}\""))});");
            var cols = string.Join(", ", primaryKey.Select(GenerationRuleSet.Literal));
            w.WriteLine($"        table.CreateIndex(\"PRIMARY\", [{cols}], unique: true));");
        }

        foreach (var (name, (Unique, Cols)) in indexes.OrderBy(p => p.Key))
        {
            var cols = string.Join(", ", Cols.Select(GenerationRuleSet.Literal));
            var uniq = Unique ? "true" : "false";
            w.WriteLine($"        table.CreateIndex({GenerationRuleSet.Literal(name)}, [{cols}], unique: {uniq});");
        }

        foreach (var (col, rtab, rcol) in foreignKeys)
        {
            w.WriteLine($"        table.CreateForeignKey({GenerationRuleSet.Literal($"FK_{tableName}_{col}_{rtab}_{rcol}")}, {GenerationRuleSet.Literal(rtab)}, [({GenerationRuleSet.Literal(col)}, {GenerationRuleSet.Literal(rcol)})]);");
        }

        w.WriteLine("        return table;");
        w.WriteLine("    }");
        w.WriteLine("}");
    }

    private static void GenerateSequenceFile(
        string ns,
        string sequenceName,
        string schema,
        SequenceMeta meta,
        string outputPath)
    {
        var dbObject = new DatabaseObjectReference(
            schema,
            sequenceName,
            DatabaseObjectType.Sequence,
            new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["StartValue"] = meta.StartValue?.ToString(CultureInfo.InvariantCulture) ?? string.Empty,
                ["IncrementBy"] = meta.IncrementBy?.ToString(CultureInfo.InvariantCulture) ?? string.Empty,
                ["CurrentValue"] = meta.CurrentValue?.ToString(CultureInfo.InvariantCulture) ?? string.Empty
            });

        var className = $"{GenerationRuleSet.ToPascalCase(sequenceName)}SequenceFactory";
        var fileName = Path.Combine(outputPath, $"{className}.cs");
        File.WriteAllText(fileName, StructuredClassContentFactory.Build(dbObject, ns), new UTF8Encoding(encoderShouldEmitUTF8Identifier: false));
    }

    private static IDictionary<string, object?> ToDictionary(object row)
    {
        if (row is IDictionary<string, object?> dict)
            return dict;

        return row
            .GetType()
            .GetProperties()
            .ToDictionary(p => p.Name, p => p.GetValue(row), StringComparer.OrdinalIgnoreCase);
    }

    private static bool ReadBoolFlexible(IDictionary<string, object?> row, string key)
    {
        var s = ReadString(row, key);
        if (string.IsNullOrWhiteSpace(s)) return false;
        return s.Equals("1", StringComparison.OrdinalIgnoreCase)
            || s.Equals("true", StringComparison.OrdinalIgnoreCase)
            || s.Equals("yes", StringComparison.OrdinalIgnoreCase)
            || s.Equals("y", StringComparison.OrdinalIgnoreCase);
    }

    private static long? ReadNullableLong(IDictionary<string, object?> row, string key)
    {
        var s = ReadString(row, key);
        if (string.IsNullOrWhiteSpace(s)) return null;
        return long.TryParse(s, NumberStyles.Integer, CultureInfo.InvariantCulture, out var n) ? n : null;
    }

    private static int? ReadNullableInt(IDictionary<string, object?> row, string key)
    {
        var s = ReadString(row, key);
        if (string.IsNullOrWhiteSpace(s)) return null;
        return int.TryParse(s, NumberStyles.Integer, CultureInfo.InvariantCulture, out var n) ? n : null;
    }

    private static int ReadInt(IDictionary<string, object?> row, string key)
        => ReadNullableInt(row, key) ?? 0;

    private static string ReadString(IDictionary<string, object?> row, string key)
    {
        foreach (var item in row)
        {
            if (string.Equals(item.Key, key, StringComparison.OrdinalIgnoreCase))
            {
                if (item.Value is null || item.Value is DBNull)
                    return string.Empty;
                return Convert.ToString(item.Value, CultureInfo.InvariantCulture)?.Trim() ?? string.Empty;
            }
        }

        return string.Empty;
    }

#pragma warning restore CA1303
}
