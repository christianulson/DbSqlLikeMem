using System.Data;
using System.Globalization;
using System.Text;
using DbSqlLikeMem.VisualStudioExtension.Core.Generation;
using Dapper;
using Microsoft.Extensions.Configuration;
using MySql.Data.MySqlClient;

namespace TableStructureGenerator;

static partial class Program
{
#pragma warning disable CA1303 // Do not pass literals as localized parameters

#pragma warning disable  CA1812
    private sealed record DestinyInfo(
        string OutputPath,
        string Schema,
        string Namespace,
        List<string>? Tables = null);

    private sealed record ConnectionInfo(
        string Name,
        string ProviderName,
        string Connection,
        bool? Enable,
        List<DestinyInfo> Destinies);

#pragma warning restore CA1812

    private const string DatabaseType = "mysql";

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

        // Se quiser rodar “tudo”, aceite --all nos args
        bool runAll = args.Any(a => string.Equals(a, "--all", StringComparison.OrdinalIgnoreCase));

        int selectedConnectionIndex = 1;
        if (!runAll)
        {
            Console.WriteLine("Select a connection:");
            Console.WriteLine($"0. Todas");
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
            using var connection = new MySqlConnection(connInfo.Connection);
            connection.Open();

            foreach (var destiny in connInfo.Destinies)
            {
                var tables = (destiny.Tables != null && destiny.Tables.Count != 0)
                    ? destiny.Tables
                    : GetTablesInSchema(connection, destiny.Schema);

                Console.WriteLine($"Schema: {destiny.Schema}");

                var outputPath = Path.Combine(baseDirectory, destiny.OutputPath);
                if (tables.Count > 0 && !Directory.Exists(outputPath))
                    Directory.CreateDirectory(outputPath);

                foreach (var tableName in tables.Distinct(StringComparer.OrdinalIgnoreCase))
                {
                    var clean = tableName.Trim();
                    if (string.IsNullOrEmpty(clean)) continue;

                    var meta = LoadTableMetadata(connection, destiny.Schema, clean);

                    GenerateTableFile(
                        destiny.Namespace,
                        tableName: clean,
                        columns: meta.Columns,
                        primaryKey: meta.PrimaryKey,
                        indexes: meta.Indexes,
                        foreignKeys: meta.ForeignKeys,
                        outputPath: outputPath);

                    Console.WriteLine($" - Tabela: {tableName} gerada.");
                }
            }
        }

        Console.WriteLine("Table structure files have been generated.");
    }

    // ---------- Metadados ----------
    private static List<string> GetTablesInSchema(
        MySqlConnection cn,
        string schema
        ) => [.. cn.Query<string>(@"
SELECT TABLE_NAME
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = @schema
  AND TABLE_TYPE = 'BASE TABLE'
ORDER BY TABLE_NAME;", new { schema })];

    private sealed record ColumnMeta(
        string ColumnName,
        string DataType,
        string ColumnType, // ex: enum('A','B') / varchar(50) / decimal(10,2)
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

    private static TableMeta LoadTableMetadata(
        MySqlConnection cn,
        string schema,
        string table)
    {
        var cols = new List<ColumnMeta>();

        // COLUMNS
        const string qCols = @"
SELECT COLUMN_NAME
     , DATA_TYPE
     , COLUMN_TYPE
     , IS_NULLABLE
     , COLUMN_DEFAULT
     , EXTRA
     , ORDINAL_POSITION
     , CHARACTER_MAXIMUM_LENGTH
     , NUMERIC_PRECISION
     , NUMERIC_SCALE
     , GENERATION_EXPRESSION
  FROM INFORMATION_SCHEMA.COLUMNS
 WHERE TABLE_SCHEMA = @schema 
   AND TABLE_NAME = @table
 ORDER BY ORDINAL_POSITION;";
        using (var cmd = new MySqlCommand(qCols, cn))
        {
            cmd.Parameters.AddWithValue("@schema", schema);
            cmd.Parameters.AddWithValue("@table", table);
            using var rd = cmd.ExecuteReader();
            while (rd.Read())
            {
                var col = new ColumnMeta(
                    ColumnName: rd.GetString("COLUMN_NAME"),
                    DataType: rd.GetString("DATA_TYPE"),
                    ColumnType: rd.GetString("COLUMN_TYPE"),
                    IsNullable: string.Equals(rd.GetString("IS_NULLABLE"), "YES", StringComparison.OrdinalIgnoreCase),
                    IsIdentity: rd["EXTRA"] is string e && e.Contains("auto_increment", StringComparison.OrdinalIgnoreCase),
                    DefaultValue: rd["COLUMN_DEFAULT"] is DBNull ? null : rd["COLUMN_DEFAULT"]?.ToString(),
                    Ordinal: Convert.ToInt32(rd["ORDINAL_POSITION"], CultureInfo.InvariantCulture) - 1,
                    CharMaxLen: rd["CHARACTER_MAXIMUM_LENGTH"] is DBNull ? null : Convert.ToInt64(rd["CHARACTER_MAXIMUM_LENGTH"], CultureInfo.InvariantCulture),
                    NumPrecision: rd["NUMERIC_PRECISION"] is DBNull ? null : Convert.ToInt32(rd["NUMERIC_PRECISION"], CultureInfo.InvariantCulture),
                    NumScale: rd["NUMERIC_SCALE"] is DBNull ? null : Convert.ToInt32(rd["NUMERIC_SCALE"], CultureInfo.InvariantCulture),
                    Generated: rd["GENERATION_EXPRESSION"] is DBNull ? null : rd.GetString("GENERATION_EXPRESSION")
                );
                cols.Add(col);
            }
        }

        // PK (via STATISTICS com INDEX_NAME='PRIMARY')
        var pk = new List<string>();
        const string qPk = @"
SELECT COLUMN_NAME
  FROM INFORMATION_SCHEMA.STATISTICS
 WHERE TABLE_SCHEMA=@schema 
   AND TABLE_NAME=@table 
   AND INDEX_NAME='PRIMARY'
 ORDER BY SEQ_IN_INDEX;";
        using (var cmd = new MySqlCommand(qPk, cn))
        {
            cmd.Parameters.AddWithValue("@schema", schema);
            cmd.Parameters.AddWithValue("@table", table);
            using var rd = cmd.ExecuteReader();
            while (rd.Read()) pk.Add(rd.GetString(0));
        }

        // Índices (inclui PRIMARY também; filtramos já lido)
        var idx = new Dictionary<string, (bool Unique, List<string> Cols)>(StringComparer.OrdinalIgnoreCase);
        const string qIdx = @"
SELECT INDEX_NAME
     , NON_UNIQUE
     , SEQ_IN_INDEX
     , COLUMN_NAME
  FROM INFORMATION_SCHEMA.STATISTICS
 WHERE TABLE_SCHEMA=@schema 
   AND TABLE_NAME=@table
 ORDER BY INDEX_NAME, SEQ_IN_INDEX;";
        using (var cmd = new MySqlCommand(qIdx, cn))
        {
            cmd.Parameters.AddWithValue("@schema", schema);
            cmd.Parameters.AddWithValue("@table", table);
            using var rd = cmd.ExecuteReader();
            while (rd.Read())
            {
                var name = rd.GetString("INDEX_NAME");
                var unique = rd.GetInt32("NON_UNIQUE") == 0;
                var col = rd.GetString("COLUMN_NAME");

                if (!idx.TryGetValue(name, out var tuple))
                    tuple = (Unique: unique, Cols: []);
                // manter Unique = true se algum registro marcar como único (deve ser consistente)
                tuple = (Unique: tuple.Unique || unique, tuple.Cols);
                tuple.Cols.Add(col);
                idx[name] = tuple;
            }
        }
        // Remove PRIMARY daqui; trataremos separado
        idx.Remove("PRIMARY");

        // FKs
        var fks = new List<(string Col, string RefTable, string RefCol)>();
        const string qFk = @"
SELECT KCU.COLUMN_NAME
     , KCU.REFERENCED_TABLE_NAME
     , KCU.REFERENCED_COLUMN_NAME
  FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE KCU
  JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS RC
    ON RC.CONSTRAINT_SCHEMA = KCU.CONSTRAINT_SCHEMA
   AND RC.CONSTRAINT_NAME = KCU.CONSTRAINT_NAME
 WHERE KCU.TABLE_SCHEMA=@schema AND KCU.TABLE_NAME=@table
   AND KCU.REFERENCED_TABLE_NAME IS NOT NULL
 ORDER BY KCU.CONSTRAINT_NAME, KCU.ORDINAL_POSITION;";
        using (var cmd = new MySqlCommand(qFk, cn))
        {
            cmd.Parameters.AddWithValue("@schema", schema);
            cmd.Parameters.AddWithValue("@table", table);
            using var rd = cmd.ExecuteReader();
            while (rd.Read())
            {
                fks.Add((rd.GetString(0), rd.GetString(1), rd.GetString(2)));
            }
        }

        return new TableMeta(cols, pk, idx, fks);
    }

    // ---------- Geração ----------
    private static void GenerateTableFile(
        string ns,
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
        w.WriteLine($"public static class {className}");
        w.WriteLine("{");
        w.WriteLine($"    public static ITableMock {methodName}(" +
                    $"        this DbMock db)");
        w.WriteLine("    {");
        w.WriteLine($"        var table = db.AddTable(\"{tableName}\");");

        // map: nome → ordinal (de fato já vem na meta)
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

        // PK
        if (primaryKey.Count > 0)
        {
            w.WriteLine($"        table.AddPrimaryKeyIndexes({string.Join(",", primaryKey.Select(_ => $"\"{_}\""))});");
            var cols = string.Join(", ", primaryKey.Select(GenerationRuleSet.Literal));
            w.WriteLine($"        table.CreateIndex(\"PRIMARY\", [{cols}], unique: true);");
        }

        // Índices (unique e não-unique)
        foreach (var (name, (Unique, Cols)) in indexes.OrderBy(p => p.Key))
        {
            var cols = string.Join(", ", Cols.Select(GenerationRuleSet.Literal));
            var uniq = Unique ? "true" : "false";
            w.WriteLine($"        table.CreateIndex({GenerationRuleSet.Literal(name)}, [{cols}], unique: {uniq});");
        }

        // FKs
        foreach (var (col, rtab, rcol) in foreignKeys)
        {
            w.WriteLine($"        table.CreateForeignKey({GenerationRuleSet.Literal($"FK_{tableName}_{col}_{rtab}_{rcol}")}, {GenerationRuleSet.Literal(rtab)}, [({GenerationRuleSet.Literal(col)}, {GenerationRuleSet.Literal(rcol)})]);");
        }

        w.WriteLine("        return table;");
        w.WriteLine("    }");
        w.WriteLine("}");

        // pronto.
    }

    // ---------- Helpers ----------
#pragma warning restore CA1303 // Do not pass literals as localized parameters
}
