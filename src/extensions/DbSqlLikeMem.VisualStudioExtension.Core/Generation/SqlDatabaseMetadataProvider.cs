using DbSqlLikeMem.VisualStudioExtension.Core.Models;
using System.Data.Common;
using System.Globalization;

namespace DbSqlLikeMem.VisualStudioExtension.Core.Generation;

/// <summary>
/// EN: Reads schema objects and routine metadata from a database connection.
/// PT: Le objetos de schema e metadados de rotinas a partir de uma conexao de banco.
/// </summary>
/// <remarks>
/// Initializes a metadata provider backed by a SQL query executor.
/// Inicializa um provedor de metadados baseado em um executor de consultas SQL.
/// </remarks>
public sealed class SqlDatabaseMetadataProvider(ISqlQueryExecutor queryExecutor) : IDatabaseMetadataProvider
{
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

        return await GetObjectDetailsAsync(connection, reference, cancellationToken);
    }

    /// <summary>
    /// EN: Gets detailed metadata for a database object without rechecking its presence in the object list.
    /// PT: Obtem metadados detalhados de um objeto de banco sem verificar novamente sua presenca na listagem.
    /// </summary>
    /// <param name="connection">EN: Connection definition used to query metadata. PT: Definicao de conexao usada para consultar metadados.</param>
    /// <param name="reference">EN: Database object reference to hydrate. PT: Referencia do objeto de banco a ser enriquecida.</param>
    /// <param name="cancellationToken">EN: Cancellation token for the operation. PT: Token de cancelamento para a operacao.</param>
    /// <returns>EN: The populated object reference, or null when the object cannot be read. PT: A referencia populada do objeto, ou null quando o objeto nao puder ser lido.</returns>
    public async Task<DatabaseObjectReference?> GetObjectDetailsAsync(
        ConnectionDefinition connection,
        DatabaseObjectReference reference,
        CancellationToken cancellationToken = default)
    {
        var args = new Dictionary<string, object?>
        {
            ["schemaName"] = reference.Schema,
            ["objectName"] = reference.Name
        };

        if (reference.Type == DatabaseObjectType.Sequence)
        {
            var sequenceRows = await queryExecutor.QueryAsync(connection, SqlMetadataQueryFactory.BuildSequenceMetadataQuery(connection.DatabaseType), args, cancellationToken);
            return reference with
            {
                Properties = SerializeSequence(sequenceRows)
            };
        }

        if (reference.Type is DatabaseObjectType.Procedure or DatabaseObjectType.Function)
        {
            var routineRows = await queryExecutor.QueryAsync(connection, SqlMetadataQueryFactory.BuildRoutineMetadataQuery(connection.DatabaseType), args, cancellationToken);
            var parameterRows = await queryExecutor.QueryAsync(connection, SqlMetadataQueryFactory.BuildRoutineParametersQuery(connection.DatabaseType), args, cancellationToken);
            var routineRow = routineRows.FirstOrDefault();

            var properties1 = reference.Type == DatabaseObjectType.Procedure
                ? SerializeProcedureRoutine(connection.DatabaseType, routineRow, parameterRows)
                : SerializeFunctionRoutine(connection.DatabaseType, routineRow, parameterRows);

            return reference with { Properties = properties1 };
        }

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

    private static readonly HashSet<string> DatabaseTypesRequiringNameParsing = new(StringComparer.Ordinal)
    {
        "mysql",
        "mariadb",
        "sqlserver",
        "sqlazure",
        "azuresql",
        "postgresql",
        "db2",
        "firebird"
    };

    private static string ResolveDatabaseNameForMetadata(ConnectionDefinition connection)
    {
        var databaseType = DatabaseTypeNormalizer.NormalizeKey(connection.DatabaseType);
        if (!DatabaseTypesRequiringNameParsing.Contains(databaseType))
            return connection.DatabaseName;

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
                Generated = ReadString(r, "ColumnGenerated")
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
                IsUnique = ReadBoolFlexible(r, "IsUnique")
                    || ReadString(r, "Uniqueness").Equals(Const.UNIQUE, StringComparison.OrdinalIgnoreCase)
                    || ReadString(r, "UniqueRule").Equals("U", StringComparison.OrdinalIgnoreCase)
                    || (HasValue(r, "NonUnique") && ReadInt(r, "NonUnique") == 0),
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

    private static bool HasValue(IReadOnlyDictionary<string, object?> row, string key)
        => !string.IsNullOrWhiteSpace(ReadString(row, key));

    private static bool IsPrimaryIndexName(string indexName)
        => indexName.Equals(Const.PRIMARY, StringComparison.OrdinalIgnoreCase)
           || indexName.Equals("PK", StringComparison.OrdinalIgnoreCase)
           || indexName.StartsWith("PK_", StringComparison.OrdinalIgnoreCase);

    private static string SerializeTriggers(IReadOnlyCollection<IReadOnlyDictionary<string, object?>> rows)
        => string.Join(";", rows
            .Select(r => ReadString(r, "TriggerName"))
            .Where(t => !string.IsNullOrWhiteSpace(t))
            .Select(Escape));

    private static IReadOnlyDictionary<string, string> SerializeProcedureRoutine(
        string databaseType,
        IReadOnlyDictionary<string, object?>? routineRow,
        IReadOnlyCollection<IReadOnlyDictionary<string, object?>> parameterRows)
    {
        var rows = parameterRows.OrderBy(row => ReadInt(row, "Ordinal")).ToArray();
        var requiredIn = rows
            .Where(row => IsProcedureInput(row) && !HasRoutineDefaultValue(row))
            .Select(row => SerializeProcedureParameter(databaseType, row))
            .ToArray();
        var optionalIn = rows
            .Where(row => IsProcedureInput(row) && HasRoutineDefaultValue(row))
            .Select(row => SerializeProcedureParameter(databaseType, row))
            .ToArray();
        var outParams = rows
            .Where(IsProcedureOutput)
            .Select(row => SerializeProcedureParameter(databaseType, row))
            .ToArray();

        var returnParam = rows
            .FirstOrDefault(IsProcedureReturn);
        var returnTypeSql = routineRow is null ? string.Empty : ReadString(routineRow, "ReturnTypeSql");
        if (returnParam is null && !string.IsNullOrWhiteSpace(returnTypeSql))
        {
            returnParam = BuildSyntheticProcedureReturnParameter(databaseType, returnTypeSql);
        }

        return new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["RequiredIn"] = string.Join(";", requiredIn),
            ["OptionalIn"] = string.Join(";", optionalIn),
            ["OutParams"] = string.Join(";", outParams),
            ["ReturnParam"] = returnParam is null ? string.Empty : SerializeProcedureParameter(databaseType, returnParam)
        };
    }

    private static IReadOnlyDictionary<string, string> SerializeFunctionRoutine(
        string databaseType,
        IReadOnlyDictionary<string, object?>? routineRow,
        IReadOnlyCollection<IReadOnlyDictionary<string, object?>> parameterRows)
    {
        var rows = parameterRows.OrderBy(row => ReadInt(row, "Ordinal")).ToArray();
        var parameters = rows
            .Where(row => !IsRoutineReturnParameter(row))
            .Select(row => SerializeFunctionParameter(databaseType, row))
            .ToArray();
        var returnRow = rows.FirstOrDefault(IsRoutineReturnParameter);

        return new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["Parameters"] = string.Join(";", parameters),
            ["ReturnTypeSql"] = returnRow is null
                ? routineRow is null ? string.Empty : ReadString(routineRow, "ReturnTypeSql")
                : ResolveFunctionSqlTypeText(databaseType, returnRow),
            ["BodySql"] = routineRow is null ? string.Empty : NormalizeFunctionBodySql(ReadString(routineRow, "BodySql")),
            ["RequiredIn"] = string.Empty,
            ["OptionalIn"] = string.Empty,
            ["OutParams"] = string.Empty,
            ["ReturnParam"] = string.Empty
        };
    }

    private static string SerializeProcedureParameter(string databaseType, IReadOnlyDictionary<string, object?> row)
    {
        var name = ReadString(row, "ParameterName");
        var dbType = ResolveProcedureDbTypeName(row, databaseType);
        var required = IsProcedureInput(row) && !HasRoutineDefaultValue(row);
        var value = ReadString(row, "DefaultValue");
        return string.Join("|", [
            Escape(name),
            Escape(dbType),
            required ? "1" : "0",
            Escape(value)
        ]);
    }

    private static string SerializeFunctionParameter(string databaseType, IReadOnlyDictionary<string, object?> row)
    {
        var name = ReadString(row, "ParameterName");
        var typeSql = ResolveFunctionSqlTypeText(databaseType, row);
        var required = !HasRoutineDefaultValue(row);
        var isVariadic = ReadBoolFlexible(row, "IsVariadic");
        var isOrderByClause = ReadBoolFlexible(row, "IsOrderByClause");
        var isFrameClause = ReadBoolFlexible(row, "IsFrameClause");
        var value = ReadString(row, "DefaultValue");

        return string.Join("|", [
            Escape(name),
            Escape(typeSql),
            required ? "1" : "0",
            isVariadic ? "1" : "0",
            isOrderByClause ? "1" : "0",
            isFrameClause ? "1" : "0",
            Escape(value)
        ]);
    }

    private static IReadOnlyDictionary<string, object?> BuildSyntheticProcedureReturnParameter(string databaseType, string returnTypeSql)
    {
        var row = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase)
        {
            ["ParameterName"] = "return",
            ["DataType"] = returnTypeSql,
            ["Ordinal"] = 0,
            ["DefaultValue"] = string.Empty,
            ["ParameterMode"] = "RETURN"
        };

        row["DbType"] = ResolveProcedureDbTypeName(row, databaseType);
        return row;
    }

    private static string ResolveProcedureDbTypeName(
        IReadOnlyDictionary<string, object?> row,
        string databaseType)
    {
        var dataType = ReadString(row, "DataType");
        var charMaxLen = ReadNullableLong(row, "CharMaxLen");
        var numPrecision = ReadNullableInt(row, "NumPrecision");
        var columnName = ReadString(row, "ParameterName");

        if (string.IsNullOrWhiteSpace(dataType))
        {
            return "String";
        }

        try
        {
            return GenerationRuleSet.MapDbType(dataType, charMaxLen, numPrecision, columnName, databaseType);
        }
        catch
        {
            return "String";
        }
    }

    private static bool HasRoutineDefaultValue(IReadOnlyDictionary<string, object?> row)
        => !string.IsNullOrWhiteSpace(ReadString(row, "DefaultValue"));

    private static string ResolveFunctionSqlTypeText(
        string databaseType,
        IReadOnlyDictionary<string, object?> row)
    {
        if (!DatabaseTypeNormalizer.NormalizeKey(databaseType).Equals("firebird", StringComparison.Ordinal))
        {
            return ReadString(row, "DataType");
        }

        var dataType = ReadString(row, "DataType");
        if (!int.TryParse(dataType, NumberStyles.Integer, CultureInfo.InvariantCulture, out var typeCode))
        {
            return dataType;
        }

        var charMaxLen = ReadNullableLong(row, "CharMaxLen");
        var numPrecision = ReadNullableInt(row, "NumPrecision");
        var numScale = ReadNullableInt(row, "NumScale");
        var scaleText = numScale is null ? null : Math.Abs(numScale.Value).ToString(CultureInfo.InvariantCulture);

        return typeCode switch
        {
            7 => FormatFirebirdNumericType("SMALLINT", numPrecision, scaleText),
            8 => FormatFirebirdNumericType("INTEGER", numPrecision, scaleText),
            10 => "FLOAT",
            12 => "DATE",
            13 => "TIME",
            14 => FormatSizedSqlType("CHAR", charMaxLen),
            16 => FormatFirebirdNumericType("BIGINT", numPrecision, scaleText),
            23 => "BOOLEAN",
            24 => "DECFLOAT(16)",
            25 => "DECFLOAT(34)",
            26 => FormatFirebirdNumericType("INT128", numPrecision, scaleText),
            27 => "DOUBLE PRECISION",
            28 => "TIME WITH TIME ZONE",
            29 => "TIMESTAMP WITH TIME ZONE",
            35 => "TIMESTAMP",
            37 => FormatSizedSqlType("VARCHAR", charMaxLen),
            40 => "CSTRING",
            261 => "BLOB",
            _ => dataType
        };
    }

    private static string NormalizeFunctionBodySql(string bodySql)
    {
        var text = bodySql.Trim();
        if (string.IsNullOrWhiteSpace(text))
        {
            return string.Empty;
        }

        if (TryExtractBlockFunctionBody(text, out var blockBody))
        {
            return blockBody;
        }

        if ((StartsWithWord(text, "CREATE") || StartsWithWord(text, "ALTER"))
            && TryExtractTrailingReturnExpression(text, out var ddlBody))
        {
            return ddlBody.Trim();
        }

        if (TryStripLeadingKeyword(text, "RETURN", out var returnBody))
        {
            return returnBody.Trim();
        }

        if (TryStripLeadingKeyword(text, "SELECT", out var selectBody))
        {
            return selectBody.Trim();
        }

        return text;
    }

    private static bool TryExtractBlockFunctionBody(string text, out string bodySql)
    {
        bodySql = string.Empty;

        var beginIndex = FindWordIndex(text, "BEGIN");
        if (beginIndex < 0)
        {
            return false;
        }

        var afterBegin = text.Substring(beginIndex + "BEGIN".Length).TrimStart();
        var returnIndex = FindWordIndex(afterBegin, "RETURN");
        if (returnIndex < 0)
        {
            return false;
        }

        bodySql = afterBegin.Substring(returnIndex + "RETURN".Length).Trim();
        while (true)
        {
            var trimmed = bodySql.TrimEnd();
            if (trimmed.EndsWith(";", StringComparison.Ordinal))
            {
                bodySql = trimmed.Substring(0, trimmed.Length - 1);
                continue;
            }

            if (EndsWithWord(trimmed, "END"))
            {
                bodySql = trimmed.Substring(0, LastWordIndex(trimmed, "END")).TrimEnd();
                continue;
            }

            bodySql = trimmed;
            break;
        }

        bodySql = bodySql.Trim();
        return !string.IsNullOrWhiteSpace(bodySql);
    }

    private static bool TryExtractTrailingReturnExpression(string text, out string bodySql)
    {
        bodySql = string.Empty;

        var returnIndex = LastWordIndex(text, "RETURN");
        if (returnIndex < 0)
        {
            return false;
        }

        bodySql = text.Substring(returnIndex + "RETURN".Length).Trim();
        if (TryStripLeadingKeyword(bodySql, "SELECT", out var selectBody))
        {
            bodySql = selectBody.Trim();
        }

        if (TryStripLeadingKeyword(bodySql, "RETURN", out var returnBody))
        {
            bodySql = returnBody.Trim();
        }

        if (bodySql.EndsWith(";", StringComparison.Ordinal))
        {
            bodySql = bodySql.Substring(0, bodySql.Length - 1).TrimEnd();
        }

        return !string.IsNullOrWhiteSpace(bodySql);
    }

    private static bool TryStripLeadingKeyword(string text, string keyword, out string value)
    {
        if (StartsWithWord(text, keyword))
        {
            value = text.Substring(keyword.Length).TrimStart();
            return true;
        }

        value = string.Empty;
        return false;
    }

    private static bool StartsWithWord(string text, string keyword)
    {
        if (text.Length < keyword.Length)
        {
            return false;
        }

        if (!text.StartsWith(keyword, StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        if (text.Length == keyword.Length)
        {
            return true;
        }

        return !IsWordChar(text[keyword.Length]);
    }

    private static bool EndsWithWord(string text, string keyword)
        => LastWordIndex(text, keyword) >= 0;

    private static int FindWordIndex(string text, string keyword)
    {
        var index = 0;
        while (index <= text.Length - keyword.Length)
        {
            var match = text.IndexOf(keyword, index, StringComparison.OrdinalIgnoreCase);
            if (match < 0)
            {
                return -1;
            }

            if (IsWholeWord(text, match, keyword.Length))
            {
                return match;
            }

            index = match + 1;
        }

        return -1;
    }

    private static int LastWordIndex(string text, string keyword)
    {
        var lastMatch = -1;
        var searchStart = 0;

        while (searchStart <= text.Length - keyword.Length)
        {
            var match = text.IndexOf(keyword, searchStart, StringComparison.OrdinalIgnoreCase);
            if (match < 0)
            {
                break;
            }

            if (IsWholeWord(text, match, keyword.Length))
            {
                lastMatch = match;
            }

            searchStart = match + 1;
        }

        return lastMatch;
    }

    private static bool IsWholeWord(string text, int index, int length)
    {
        var beforeOk = index == 0 || !IsWordChar(text[index - 1]);
        var afterIndex = index + length;
        var afterOk = afterIndex >= text.Length || !IsWordChar(text[afterIndex]);
        return beforeOk && afterOk;
    }

    private static bool IsWordChar(char ch)
        => char.IsLetterOrDigit(ch) || ch == '_';

    private static string FormatSizedSqlType(string typeName, long? length)
        => length is > 0
            ? $"{typeName}({length.Value.ToString(CultureInfo.InvariantCulture)})"
            : typeName;

    private static string FormatFirebirdNumericType(string baseType, int? precision, string? scaleText)
    {
        var typeName = baseType switch
        {
            "SMALLINT" => "NUMERIC",
            "INTEGER" => "NUMERIC",
            "BIGINT" => "NUMERIC",
            "INT128" => "NUMERIC",
            _ => baseType
        };

        if (precision is not > 0)
        {
            return baseType;
        }

        return scaleText is null
            ? $"{typeName}({precision.Value.ToString(CultureInfo.InvariantCulture)})"
            : $"{typeName}({precision.Value.ToString(CultureInfo.InvariantCulture)}, {scaleText})";
    }

    private static bool IsRoutineReturnParameter(IReadOnlyDictionary<string, object?> row)
    {
        var mode = ReadString(row, "ParameterMode");
        if (mode.Equals("RETURN", StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        if (ReadInt(row, "Ordinal") != 0)
        {
            return false;
        }

        return !mode.Equals("IN", StringComparison.OrdinalIgnoreCase)
            && !mode.Equals("OUT", StringComparison.OrdinalIgnoreCase)
            && !mode.Equals("INOUT", StringComparison.OrdinalIgnoreCase);
    }

    private static bool IsProcedureReturn(IReadOnlyDictionary<string, object?> row)
        => IsRoutineReturnParameter(row);

    private static bool IsProcedureOutput(IReadOnlyDictionary<string, object?> row)
    {
        var mode = ReadString(row, "ParameterMode");
        return !IsProcedureReturn(row)
            && mode.Contains("OUT", StringComparison.OrdinalIgnoreCase);
    }

    private static bool IsProcedureInput(IReadOnlyDictionary<string, object?> row)
        => !IsProcedureReturn(row) && !IsProcedureOutput(row);

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

    private static IReadOnlyDictionary<string, string> SerializeSequence(IReadOnlyCollection<IReadOnlyDictionary<string, object?>> rows)
    {
        var row = rows.FirstOrDefault();
        return new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["StartValue"] = row is null ? string.Empty : ReadString(row, "StartValue"),
            ["IncrementBy"] = row is null ? string.Empty : ReadString(row, "IncrementBy"),
            ["CurrentValue"] = row is null ? string.Empty : ReadString(row, "CurrentValue")
        };
    }

    private static DatabaseObjectReference? MapObject(IReadOnlyDictionary<string, object?> row)
    {
        var schema = ReadString(row, "SchemaName");
        var name = ReadString(row, "ObjectName");
        var typeRaw = ReadString(row, "ObjectType");

        if (string.IsNullOrWhiteSpace(name)
            || string.IsNullOrWhiteSpace(typeRaw)
            || !Enum.TryParse<DatabaseObjectType>(typeRaw, true, out var type))
        {
            return null;
        }

        return new DatabaseObjectReference(schema, name, type, "public");
    }

    private static bool ReadBoolFlexible(IReadOnlyDictionary<string, object?> row, string key)
    {
        var s = ReadString(row, key);
        if (string.IsNullOrWhiteSpace(s)) return false;
        return s.Equals("1", StringComparison.OrdinalIgnoreCase)
            || s.Equals("true", StringComparison.OrdinalIgnoreCase)
            || s.Equals("yes", StringComparison.OrdinalIgnoreCase)
            || s.Equals("y", StringComparison.OrdinalIgnoreCase);
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
