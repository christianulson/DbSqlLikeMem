namespace DbSqlLikeMem;

internal abstract partial class AstQueryExecutorBase
{
    private static int? TryResolveSqlServerSystemTypeId(string? typeName)
    {
        if (string.IsNullOrWhiteSpace(typeName))
            return null;

        return typeName!.Trim().ToUpperInvariant() switch
        {
            "BIGINT" => 127,
            "BIT" => 104,
            "DATE" => 40,
            "DATETIME" => 61,
            "DATETIME2" => 42,
            "DATETIMEOFFSET" => 43,
            "DECIMAL" or "NUMERIC" => 106,
            "FLOAT" => 62,
            "INT" => 56,
            "NCHAR" => 239,
            "NVARCHAR" => 231,
            "REAL" => 59,
            "SMALLINT" => 52,
            "TIME" => 41,
            "TINYINT" => 48,
            "UNIQUEIDENTIFIER" => 36,
            "VARCHAR" => 167,
            _ => null
        };
    }

    private static string? TryResolveSqlServerSystemTypeName(object? typeIdValue)
    {
        if (IsNullish(typeIdValue))
            return null;

        var typeId = Convert.ToInt32(typeIdValue, CultureInfo.InvariantCulture);
        return typeId switch
        {
            36 => "uniqueidentifier",
            40 => "date",
            41 => "time",
            42 => "datetime2",
            43 => "datetimeoffset",
            48 => "tinyint",
            52 => "smallint",
            56 => "int",
            59 => "real",
            61 => "datetime",
            62 => "float",
            104 => "bit",
            106 => "decimal",
            127 => "bigint",
            167 => "varchar",
            231 => "nvarchar",
            239 => "nchar",
            _ => null
        };
    }

    private static int? TryResolveSqlServerDatabasePrincipalId(string? principalName)
    {
        if (string.IsNullOrWhiteSpace(principalName))
            return null;

        return principalName!.Trim().ToUpperInvariant() switch
        {
            "DBO" => 1,
            "GUEST" => 2,
            "PUBLIC" => 0,
            _ => null
        };
    }

    private static object? TryResolveSqlServerTypeProperty(string? typeName, string? propertyName)
    {
        if (string.IsNullOrWhiteSpace(typeName) || string.IsNullOrWhiteSpace(propertyName))
            return null;

        var normalizedType = typeName!.Trim().ToUpperInvariant();
        return propertyName!.Trim().ToUpperInvariant() switch
        {
            "OWNERID" => TryResolveSqlServerSystemTypeId(normalizedType) is null ? null : 1,
            "PRECISION" => normalizedType switch
            {
                "BIGINT" => 19,
                "BIT" => 1,
                "DATE" => 10,
                "DATETIME" => 23,
                "DATETIME2" => 27,
                "DATETIMEOFFSET" => 34,
                "DECIMAL" or "NUMERIC" => 38,
                "FLOAT" => 53,
                "INT" => 10,
                "REAL" => 24,
                "SMALLINT" => 5,
                "TIME" => 16,
                "TINYINT" => 3,
                _ => null
            },
            _ => null
        };
    }

    private object? TryResolveSqlServerDatabaseProperty(string? databaseName, string? propertyName)
    {
        if (string.IsNullOrWhiteSpace(databaseName) || string.IsNullOrWhiteSpace(propertyName))
            return null;

        var normalizedDatabase = databaseName!.Trim().Trim('[', ']').NormalizeName();
        if (!string.Equals(normalizedDatabase, _cnn.Database.NormalizeName(), StringComparison.OrdinalIgnoreCase))
            return null;

        return propertyName!.Trim().ToUpperInvariant() switch
        {
            "STATUS" => "ONLINE",
            "UPDATEABILITY" => "READ_WRITE",
            "VERSION" => _cnn.Db.Version,
            _ => null
        };
    }

    private object? TryResolveSqlServerColumnProperty(
        object? objectIdValue,
        string? columnName,
        string? propertyName)
    {
        if (IsNullish(objectIdValue) || string.IsNullOrWhiteSpace(columnName) || string.IsNullOrWhiteSpace(propertyName))
            return null;

        ITableMock? table = objectIdValue switch
        {
            string objectName => TryResolveSqlServerTable(objectName, out var tableByName) ? tableByName : null,
            _ => TryResolveSqlServerTableByObjectId(Convert.ToInt32(objectIdValue, CultureInfo.InvariantCulture))
        };

        if (table is null)
            return null;

        var column = table.GetColumn(columnName!);
        return propertyName!.Trim().ToUpperInvariant() switch
        {
            "ALLOWSNULL" => column.Nullable ? 1 : 0,
            "COLUMNID" => column.Index + 1,
            "ISIDENTITY" => column.Identity ? 1 : 0,
            _ => null
        };
    }

    private int? TryResolveSqlServerColumnLength(string? objectName, string? columnName)
    {
        if (!TryResolveSqlServerTable(objectName, out var table) || table is null || string.IsNullOrWhiteSpace(columnName))
            return null;

        var column = table!.GetColumn(columnName!);
        return column.DbType switch
        {
            DbType.Boolean => 1,
            DbType.Byte or DbType.SByte => 1,
            DbType.Int16 or DbType.UInt16 => 2,
            DbType.Int32 or DbType.UInt32 => 4,
            DbType.Int64 or DbType.UInt64 => 8,
            DbType.Single => 4,
            DbType.Double => 8,
            DbType.Decimal or DbType.Currency or DbType.VarNumeric => 17,
            DbType.Guid => 16,
            DbType.Date => 3,
            DbType.Time => 5,
            DbType.DateTime => 8,
            DbType.DateTime2 => 8,
            DbType.DateTimeOffset => 10,
            DbType.Binary => column.Size,
            DbType.String or DbType.StringFixedLength => column.Size,
            DbType.AnsiString or DbType.AnsiStringFixedLength => column.Size,
            _ => column.Size
        };
    }

    private string? TryResolveSqlServerColumnName(object? objectIdValue, object? columnIdValue)
    {
        if (IsNullish(objectIdValue) || IsNullish(columnIdValue))
            return null;

        var table = TryResolveSqlServerTableByObjectId(Convert.ToInt32(objectIdValue, CultureInfo.InvariantCulture));
        if (table is null)
            return null;

        var columnId = Convert.ToInt32(columnIdValue, CultureInfo.InvariantCulture);
        if (columnId <= 0)
            return null;

        return table.Columns.Values
            .FirstOrDefault(col => col.Index == columnId - 1)
            ?.Name;
    }

    private bool TryResolveSqlServerTable(string? objectName, out ITableMock? table)
    {
        table = null;
        if (string.IsNullOrWhiteSpace(objectName))
            return false;

        var normalizedInput = objectName!.Trim().Trim('[', ']').NormalizeName();
        var objectEntry = EnumerateSqlServerObjects()
            .Where(item => item.FullName.Equals(normalizedInput, StringComparison.OrdinalIgnoreCase)
                || item.TableName.Equals(normalizedInput, StringComparison.OrdinalIgnoreCase))
            .Take(2)
            .ToList();

        if (objectEntry.Count != 1)
            return false;

        return _cnn.TryGetTable(objectEntry[0].TableName, out table, objectEntry[0].SchemaName);
    }

    private ITableMock? TryResolveSqlServerTableByObjectId(int objectId)
    {
        var objectEntry = EnumerateSqlServerObjects().FirstOrDefault(item => item.ObjectId == objectId);
        if (objectEntry == default)
            return null;

        return _cnn.TryGetTable(objectEntry.TableName, out var table, objectEntry.SchemaName)
            ? table
            : null;
    }

    private int? TryResolveSqlServerObjectId(string? objectName)
    {
        if (string.IsNullOrWhiteSpace(objectName))
            return null;

        var normalizedInput = objectName!.Trim().Trim('[', ']').NormalizeName();
        var matches = EnumerateSqlServerObjects()
            .Where(item => item.FullName.Equals(normalizedInput, StringComparison.OrdinalIgnoreCase)
                || item.TableName.Equals(normalizedInput, StringComparison.OrdinalIgnoreCase))
            .Take(2)
            .ToList();

        if (matches.Count != 1)
            return null;

        return matches[0].ObjectId;
    }

    private object? TryResolveSqlServerObjectProperty(object? objectIdValue, string? propertyName)
    {
        if (IsNullish(objectIdValue) || string.IsNullOrWhiteSpace(propertyName))
            return null;

        var objectId = Convert.ToInt32(objectIdValue, CultureInfo.InvariantCulture);
        var entry = EnumerateSqlServerObjects().FirstOrDefault(item => item.ObjectId == objectId);
        if (entry == default)
            return null;

        return propertyName!.Trim().ToUpperInvariant() switch
        {
            "ISTABLE" => entry.ObjectKind == SqlConst.TABLE ? 1 : 0,
            "ISPROCEDURE" => entry.ObjectKind == "PROCEDURE" ? 1 : 0,
            _ => null
        };
    }

    private string? TryResolveSqlServerObjectName(object? objectIdValue)
    {
        if (IsNullish(objectIdValue))
            return null;

        var objectId = Convert.ToInt32(objectIdValue, CultureInfo.InvariantCulture);
        var match = EnumerateSqlServerObjects()
            .FirstOrDefault(item => item.ObjectId == objectId);
        return match.ObjectId == 0 ? null : match.TableName;
    }

    private string? TryResolveSqlServerObjectSchemaName(object? objectIdValue)
    {
        if (IsNullish(objectIdValue))
            return null;

        var objectId = Convert.ToInt32(objectIdValue, CultureInfo.InvariantCulture);
        var match = EnumerateSqlServerObjects()
            .FirstOrDefault(item => item.ObjectId == objectId);
        return match.ObjectId == 0 ? null : match.SchemaName;
    }

    private List<(int ObjectId, string SchemaName, string TableName, string FullName, string ObjectKind)> EnumerateSqlServerObjects()
    {
        var objects = new List<(int ObjectId, string SchemaName, string TableName, string FullName, string ObjectKind)>();
        var nextId = 1;

        foreach (var schema in _cnn.Db.Values.OrderBy(static s => s.SchemaName, StringComparer.OrdinalIgnoreCase))
        {
            foreach (var table in schema.Tables.Values.OrderBy(static t => t.TableName, StringComparer.OrdinalIgnoreCase))
            {
                objects.Add((nextId++, schema.SchemaName, table.TableName, $"{schema.SchemaName}.{table.TableName}", SqlConst.TABLE));
            }

            foreach (var procedure in schema.Procedures.Keys.OrderBy(static p => p, StringComparer.OrdinalIgnoreCase))
            {
                objects.Add((nextId++, schema.SchemaName, procedure, $"{schema.SchemaName}.{procedure}", "PROCEDURE"));
            }
        }

        return objects;
    }

    private static int? TryResolveSqlServerRoleMembership(string? roleName)
    {
        if (string.IsNullOrWhiteSpace(roleName))
            return null;

        return roleName!.Trim().ToUpperInvariant() switch
        {
            "DB_OWNER" => 1,
            "PUBLIC" => 1,
            "DB_DATAREADER" => 0,
            "DB_DATAWRITER" => 0,
            _ => null
        };
    }

    private static int? TryResolveSqlServerServerRoleMembership(string? roleName)
    {
        if (string.IsNullOrWhiteSpace(roleName))
            return null;

        return roleName!.Trim().ToUpperInvariant() switch
        {
            "SYSADMIN" => 1,
            "SERVERADMIN" => 0,
            _ => null
        };
    }

    private static string ComputeSoundex(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return string.Empty;

        var firstLetter = char.ToUpperInvariant(value[0]);
        var codes = new StringBuilder();

        char? lastCode = null;
        foreach (var ch in value.Skip(1))
        {
            var code = GetSoundexCode(ch);
            if (code is null)
            {
                lastCode = null;
                continue;
            }

            if (lastCode.HasValue && lastCode.Value == code.Value)
                continue;

            codes.Append(code.Value);
            lastCode = code.Value;
        }

        var soundex = new StringBuilder(4);
        soundex.Append(firstLetter);
        soundex.Append(codes);
        while (soundex.Length < 4)
            soundex.Append('0');

        if (soundex.Length > 4)
            soundex.Length = 4;

        return soundex.ToString();
    }
}
