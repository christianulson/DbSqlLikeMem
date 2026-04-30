namespace DbSqlLikeMem;

internal static partial class DbSelectIntoAndInsertSelectStrategies
{
    private enum ExecuteBlockExceptionHandlerKind
    {
        Any,
        SqlCode,
        GdsCode,
        SqlState,
        Exception
    }

    private sealed record ExecuteBlockExceptionHandler(
        ExecuteBlockExceptionHandlerKind Kind,
        IReadOnlyList<string> Selectors,
        string BodySql);

    /// <summary>
    /// EN: Dispatches parsed AST commands to ExecuteNonQuery handlers.
    /// PT: Despacha comandos AST parseados para handlers de ExecuteNonQuery.
    /// </summary>
    public static DmlExecutionResult ExecuteParsedNonQuery(
        this DbConnectionMockBase connection,
        SqlQueryBase query,
        QueryExecutionContext context,
        bool allowMerge,
        bool unionUsesSelectMessage)
    {
        using var _ = connection.Metrics.BeginAmbientScope();
        return ExecuteParsedNonQueryCore(connection, query, context, allowMerge, unionUsesSelectMessage);
    }

    private static DmlExecutionResult ExecuteParsedNonQueryCore(
        DbConnectionMockBase connection,
        SqlQueryBase query,
        QueryExecutionContext context,
        bool allowMerge,
        bool unionUsesSelectMessage)
    {
        return query switch
        {
            SqlInsertQuery insertQ => connection.ExecuteInsert(insertQ, context),
            SqlUpdateQuery updateQ => connection.ExecuteUpdateSmart(updateQ, context),
            SqlDeleteQuery deleteQ => connection.ExecuteDeleteSmart(deleteQ, context),
            SqlCreateTemporaryTableQuery tempQ => connection.ExecuteCreateTemporaryTableAsSelect(tempQ, context),
            SqlCreateViewQuery viewQ => connection.ExecuteCreateView(viewQ, context.DbParameters, context.Dialect),
            SqlAlterTableAddColumnQuery alterAddColumnQ => connection.ExecuteAlterTableAddColumn(alterAddColumnQ, context.DbParameters, context.Dialect),
            SqlCreateIndexQuery createIndexQ => connection.ExecuteCreateIndex(createIndexQ, context.DbParameters, context.Dialect),
            SqlCreateSequenceQuery createSequenceQ => connection.ExecuteCreateSequence(createSequenceQ, context.DbParameters, context.Dialect),
            SqlAlterSequenceQuery alterSequenceQ => connection.ExecuteAlterSequence(alterSequenceQ, context.DbParameters, context.Dialect),
            SqlCreateFunctionQuery createFunctionQ => connection.ExecuteCreateFunction(createFunctionQ, context.DbParameters, context.Dialect),
            SqlCreateProcedureQuery createProcedureQ => connection.ExecuteCreateProcedure(createProcedureQ, context.DbParameters, context.Dialect),
            SqlCreateTriggerQuery createTriggerQ => connection.ExecuteCreateTrigger(createTriggerQ),
            SqlDropViewQuery dropViewQ => connection.ExecuteDropView(dropViewQ, context.DbParameters, context.Dialect),
            SqlDropTableQuery dropTableQ => connection.ExecuteDropTable(dropTableQ, context.DbParameters, context.Dialect),
            SqlDropIndexQuery dropIndexQ => connection.ExecuteDropIndex(dropIndexQ, context.DbParameters, context.Dialect),
            SqlDropSequenceQuery dropSequenceQ => connection.ExecuteDropSequence(dropSequenceQ, context.DbParameters, context.Dialect),
            SqlDropFunctionQuery dropFunctionQ => connection.ExecuteDropFunction(dropFunctionQ, context.DbParameters, context.Dialect),
            SqlDropProcedureQuery dropProcedureQ => connection.ExecuteDropProcedure(dropProcedureQ, context.DbParameters, context.Dialect),
            SqlDropTriggerQuery dropTriggerQ => connection.ExecuteDropTrigger(dropTriggerQ, context.DbParameters, context.Dialect),
            SqlExecuteBlockQuery executeBlockQ => connection.ExecuteExecuteBlock(executeBlockQ, context.DbParameters, context.Dialect),
            SqlMergeQuery mergeQ when allowMerge => connection.ExecuteMerge(mergeQ, context),
            SqlSelectQuery _ => throw new InvalidOperationException(SqlExceptionMessages.UseExecuteReaderForSelect()),
            SqlUnionQuery _ when unionUsesSelectMessage => throw new InvalidOperationException(SqlExceptionMessages.UseExecuteReaderForSelectUnion()),
            _ => throw SqlUnsupported.NotSupportedCommandType(context.Dialect, "ExecuteNonQuery", query.GetType())
        };
    }

    /// <summary>
    /// EN: Implements ExecuteCreateSchema.
    /// PT: Implementa ExecuteCreateSchema.
    /// </summary>
    public static DmlExecutionResult ExecuteCreateSchema(
        this DbConnectionMockBase connection,
        SqlCreateSchemaQuery query)
    {
        var schemaName = query.Table?.Name;
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(schemaName, nameof(schemaName));
        connection.Db.CreateSchema(schemaName!);
        return new DmlExecutionResult();
    }

    /// <summary>
    /// EN: Implements ExecuteDropTable.
    /// PT: Implementa ExecuteDropTable.
    /// </summary>
    public static DmlExecutionResult ExecuteDropTable(
        this DbConnectionMockBase connection,
        SqlDropTableQuery query,
        DbParameterCollection pars,
        ISqlDialect dialect)
    {
        _ = pars;
        DmlExecutionResult affected;
        affected = connection.Db.ExecuteWithLock(() => ExecuteDropTableImpl(connection, query, dialect));

        connection.SetLastFoundRows(affected.AffectedRows);
        return affected;
    }

    private static DmlExecutionResult ExecuteDropTableImpl(
        DbConnectionMockBase connection,
        SqlDropTableQuery query,
        ISqlDialect dialect)
    {
        var tableName = query.Table?.Name;
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(tableName, nameof(tableName));
        var scope = GetEffectiveTemporaryTableScope(query.Scope, dialect);
        connection.DropTable(
            tableName!,
            query.IfExists,
            query.Temporary,
            scope,
            query.Table?.DbName);
        return new DmlExecutionResult();
    }

    /// <summary>
    /// EN: Implements ExecuteAlterTableAddColumn.
    /// PT: Implementa ExecuteAlterTableAddColumn.
    /// </summary>
    public static DmlExecutionResult ExecuteAlterTableAddColumn(
        this DbConnectionMockBase connection,
        SqlAlterTableAddColumnQuery query,
        DbParameterCollection pars,
        ISqlDialect dialect)
    {
        _ = pars;
        _ = dialect;
        DmlExecutionResult affected;
        affected = connection.Db.ExecuteWithLock(() => ExecuteAlterTableAddColumnImpl(connection, query));

        connection.SetLastFoundRows(affected.AffectedRows);
        return affected;
    }

    private static DmlExecutionResult ExecuteAlterTableAddColumnImpl(
        DbConnectionMockBase connection,
        SqlAlterTableAddColumnQuery query)
    {
        var tableName = query.Table?.Name;
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(tableName, nameof(tableName));

        var defaultValue = query.DefaultValueRaw is null
            ? null
            : query.ColumnType.Parse(query.DefaultValueRaw);

        connection.AlterTableAddColumn(
            tableName!,
            query.ColumnName,
            query.ColumnType,
            query.Nullable,
            query.Size,
            query.DecimalPlaces,
            defaultValue,
            query.ComputedExpression,
            query.Table?.DbName);

        return new DmlExecutionResult();
    }

    private static DmlExecutionResult ExecuteDropFunctionImpl(
        DbConnectionMockBase connection,
        SqlDropFunctionQuery query)
    {
        var functionName = query.Table?.Name;
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(functionName, nameof(functionName));
        connection.DropFunction(functionName!, query.IfExists, query.Table?.DbName);
        return new DmlExecutionResult();
    }

    /// <summary>
    /// EN: Implements ExecuteCreateTableAsSelect.
    /// PT: Implementa ExecuteCreateTableAsSelect.
    /// </summary>
    public static DmlExecutionResult ExecuteCreateTableAsSelect(
        this DbConnectionMockBase connection,
        string sql,
        QueryExecutionContext context)
    {
        var affected = connection.Db.ExecuteWithLock(() => ExecuteCreateTableAsSelectImpl(connection, sql, context));

        connection.SetLastFoundRows(affected.AffectedRows);
        return affected;
    }

    private static DmlExecutionResult ExecuteCreateTableAsSelectImpl(
        this DbConnectionMockBase connection,
        string sql,
        QueryExecutionContext context)
    {
        // CREATE TABLE name AS SELECT ...
        var m = Regex.Match(sql, @"^CREATE\s+TABLE\s+`?(?<name>[A-Za-z0-9_]+)`?\s+AS\s+(?<select>(SELECT|WITH)\s+.*)$",
            RegexOptions.IgnoreCase | RegexOptions.Singleline);
        if (m.Success)
        {
            var tableName = m.Groups["name"].Value.NormalizeName();
            var selectSql = m.Groups["select"].Value;

            var executor = context.CreateExecutor();
            var q = SqlQueryParser.Parse(
                selectSql,
                connection.Db,
                connection.ExecutionDialect,
                null,
                SqlCustomFunctionResolverFactory.Create(context));
            var res = executor.ExecuteSelect((SqlSelectQuery)q);

            var newTable = connection.AddTable(tableName);
            // map columns
            for (int i = 0; i < res.Columns.Count; i++)
            {
                var colName = res.Columns[i].ColumnName;
                var dbType = InferDbType(res, i);
                AddInferredColumn(newTable, colName, dbType);
            }

            foreach (var row in res)
            {
                var d = new Dictionary<int, object?>();
                for (int i = 0; i < res.Columns.Count; i++)
                    d[i] = row.TryGetValue(i, out var v) ? v : null;
                newTable.Add(d);
            }

            return DmlExecutionResult.ForCount(res.Count);
        }

        // CREATE TABLE name (id INT, name VARCHAR(100), ...) [PARTITION BY ...]
        var createTableMatch = Regex.Match(
            sql,
            @"^CREATE\s+TABLE\s+`?(?<name>[A-Za-z0-9_]+)`?\s*(?<rest>.+)$",
            RegexOptions.IgnoreCase | RegexOptions.Singleline);
        if (!createTableMatch.Success)
            throw new InvalidOperationException(SqlExceptionMessages.InvalidCreateTableStatement());

        var table = connection.AddTable(createTableMatch.Groups["name"].Value.NormalizeName());
        var rest = createTableMatch.Groups["rest"].Value.Trim();
        if (!TryExtractSingleParenthesizedBlock(rest, out var columnsSql, out var trailingSql))
            throw new InvalidOperationException(SqlExceptionMessages.InvalidCreateTableStatement());

        var primaryKeyColumns = new List<string>();
        var checkConstraints = new List<SchemaSnapshotCheckConstraint>();
        var checkConstraintOrdinal = 1;
        foreach (var columnSql in SplitColumnDefinitions(columnsSql))
        {
            var tableCheckConstraint = ParseTableCheckConstraint(columnSql);
            if (tableCheckConstraint is not null)
            {
                if (string.IsNullOrWhiteSpace(tableCheckConstraint.Name))
                    tableCheckConstraint = tableCheckConstraint with { Name = $"CHECK_{checkConstraintOrdinal}" };
                checkConstraints.Add(tableCheckConstraint);
                checkConstraintOrdinal++;
                continue;
            }

            var tablePrimaryKeyColumns = ParsePrimaryKeyConstraint(columnSql);
            if (tablePrimaryKeyColumns.Count > 0)
            {
                primaryKeyColumns.AddRange(tablePrimaryKeyColumns);
                continue;
            }

            var col = ParseColumnDefinition(columnSql);
            if (col is null)
                continue;
            table.AddColumn(
                col.Value.Name,
                col.Value.Type,
                nullable: col.Value.Nullable,
                size: col.Value.Size,
                decimalPlaces: col.Value.DecimalPlaces,
                identity: col.Value.Identity,
                defaultValue: col.Value.DefaultValue,
                computedExpression: col.Value.ComputedExpression);
            if (col.Value.PrimaryKey)
                primaryKeyColumns.Add(col.Value.Name);
        }

        if (checkConstraints.Count > 0 && table is TableMock tableMock)
            tableMock.CheckConstraintsMutable.AddRange(checkConstraints);

        if (primaryKeyColumns.Count > 0)
            table.AddPrimaryKeyIndexes([.. primaryKeyColumns.Distinct(StringComparer.OrdinalIgnoreCase)]);

        var partitionClause = trailingSql.Trim();
        if (!string.IsNullOrWhiteSpace(partitionClause))
        {
            if (!partitionClause.StartsWith("PARTITION", StringComparison.OrdinalIgnoreCase))
                throw new InvalidOperationException("CREATE TABLE only supports trailing PARTITION BY metadata in the mock.");

            table.PartitionClauseSql = partitionClause.TrimEnd(';').Trim();
        }

        return new DmlExecutionResult();
    }

    private static bool TryExtractSingleParenthesizedBlock(string sql, out string inner, out string trailingSql)
    {
        inner = string.Empty;
        trailingSql = string.Empty;

        if (string.IsNullOrWhiteSpace(sql) || sql[0] != '(')
            return false;

        var depth = 0;
        var inSingleQuote = false;
        for (var i = 0; i < sql.Length; i++)
        {
            var ch = sql[i];
            if (inSingleQuote)
            {
                if (ch == '\'')
                {
                    if (i + 1 < sql.Length && sql[i + 1] == '\'')
                    {
                        i++;
                        continue;
                    }

                    inSingleQuote = false;
                }

                continue;
            }

            if (ch == '\'')
            {
                inSingleQuote = true;
                continue;
            }

            if (ch == '(')
            {
                depth++;
                continue;
            }

            if (ch == ')')
            {
                depth--;
                if (depth == 0)
                {
                    inner = sql[1..i];
                    trailingSql = i + 1 < sql.Length ? sql[(i + 1)..].Trim() : string.Empty;
                    return true;
                }
            }
        }

        return false;
    }

    private static IEnumerable<string> SplitColumnDefinitions(string columnsSql)
    {
        var start = 0;
        var depth = 0;
        for (var i = 0; i < columnsSql.Length; i++)
        {
            var c = columnsSql[i];
            if (c == '(')
                depth++;
            else if (c == ')')
                depth--;
            else if (c == ',' && depth == 0)
            {
                var slice = columnsSql[start..i].Trim();
                if (!string.IsNullOrWhiteSpace(slice))
                    yield return slice;
                start = i + 1;
            }
        }

        var last = columnsSql[start..].Trim();
        if (!string.IsNullOrWhiteSpace(last))
            yield return last;
    }

    private static (string Name, DbType Type, bool Nullable, bool PrimaryKey, bool Identity, int? Size, int? DecimalPlaces, object? DefaultValue, string? ComputedExpression)? ParseColumnDefinition(string columnSql)
    {
        var m = Regex.Match(
            columnSql,
            @"^`?(?<name>[A-Za-z0-9_]+)`?\s+(?<type>[A-Za-z0-9_]+)(\s*\((?<args>[^)]*)\))?(?<rest>.*)$",
            RegexOptions.IgnoreCase | RegexOptions.Singleline);
        if (!m.Success)
            return null;

        var rest = m.Groups["rest"].Value;
        if (Regex.IsMatch(rest, @"\b(CONSTRAINT|UNIQUE\s*\(|FOREIGN\s+KEY|CHECK)\b", RegexOptions.IgnoreCase))
            return null;

        var name = m.Groups["name"].Value;
        var type = ParseDbTypeFromSqlType(m.Groups["type"].Value);
        var (size, decimalPlaces) = ParseTypeArgs(m.Groups["args"].Value, type);
        var nullable = !Regex.IsMatch(rest, @"\bNOT\s+NULL\b", RegexOptions.IgnoreCase);
        var primaryKey = Regex.IsMatch(rest, @"\bPRIMARY\s+KEY\b", RegexOptions.IgnoreCase);
        var identity = Regex.IsMatch(rest, @"\bIDENTITY\s*(\(\s*\d+\s*,\s*\d+\s*\))?", RegexOptions.IgnoreCase);
        var defaultValue = ParseColumnDefaultValue(rest);
        var computedExpression = ParseComputedExpression(rest);
        return (name, type, nullable, primaryKey, identity, size, decimalPlaces, defaultValue, computedExpression);
    }

    private static SchemaSnapshotCheckConstraint? ParseTableCheckConstraint(string columnSql)
    {
        var m = Regex.Match(
            columnSql,
            @"^(CONSTRAINT\s+`?(?<name>[A-Za-z0-9_]+)`?\s+)?CHECK\s*\((?<expr>.+)\)\s*$",
            RegexOptions.IgnoreCase | RegexOptions.Singleline);
        if (!m.Success)
            return null;

        return new SchemaSnapshotCheckConstraint
        {
            Name = m.Groups["name"].Value.NormalizeName(),
            Expression = m.Groups["expr"].Value.Trim()
        };
    }

    private static string? ParseComputedExpression(string rest)
    {
        var m = Regex.Match(
            rest,
            @"\b(?:GENERATED\s+ALWAYS\s+)?AS\s*\((?<expr>.+)\)",
            RegexOptions.IgnoreCase | RegexOptions.Singleline);
        if (!m.Success)
            return null;

        var expr = m.Groups["expr"].Value.Trim();
        return string.IsNullOrWhiteSpace(expr) ? null : expr;
    }

    private static object? ParseColumnDefaultValue(string rest)
    {
        var m = Regex.Match(
            rest,
            @"\bDEFAULT\b\s+(?<value>.+?)(?=\bNOT\s+NULL\b|\bNULL\b|\bPRIMARY\s+KEY\b|\bCONSTRAINT\b|\bUNIQUE\b|\bCHECK\b|$)",
            RegexOptions.IgnoreCase | RegexOptions.Singleline);
        if (!m.Success)
            return null;

        var value = m.Groups["value"].Value.Trim().TrimEnd(',');
        if (string.IsNullOrWhiteSpace(value))
            return null;

        if (string.Equals(value, SqlConst.NULL, StringComparison.OrdinalIgnoreCase))
            return null;

        if (string.Equals(value, SqlConst.TRUE, StringComparison.OrdinalIgnoreCase))
            return true;

        if (string.Equals(value, SqlConst.FALSE, StringComparison.OrdinalIgnoreCase))
            return false;

        if (string.Equals(value, "CURRENT_TIMESTAMP", StringComparison.OrdinalIgnoreCase)
            || string.Equals(value, "CURRENT TIMESTAMP", StringComparison.OrdinalIgnoreCase))
        {
            return DateTime.Now;
        }

        if (string.Equals(value, "SYSUTCDATETIME", StringComparison.OrdinalIgnoreCase)
            || string.Equals(value, "SYSUTCDATETIME()", StringComparison.OrdinalIgnoreCase)
            || string.Equals(value, "GETUTCDATE", StringComparison.OrdinalIgnoreCase)
            || string.Equals(value, "GETUTCDATE()", StringComparison.OrdinalIgnoreCase))
        {
            return DateTime.UtcNow;
        }

        if (string.Equals(value, "GETDATE", StringComparison.OrdinalIgnoreCase)
            || string.Equals(value, "GETDATE()", StringComparison.OrdinalIgnoreCase)
            || string.Equals(value, "SYSDATETIME", StringComparison.OrdinalIgnoreCase)
            || string.Equals(value, "SYSDATETIME()", StringComparison.OrdinalIgnoreCase))
        {
            return DateTime.Now;
        }

        if (value.Length >= 2 && value[0] == '\'' && value[^1] == '\'')
            return value[1..^1].Replace("''", "'");

        if (long.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsedLong))
            return parsedLong;

        if (decimal.TryParse(value, NumberStyles.Number, CultureInfo.InvariantCulture, out var parsedDecimal))
            return parsedDecimal;

        return value;
    }


    private static (int? Size, int? DecimalPlaces) ParseTypeArgs(string rawArgs, DbType dbType)
    {
        if (string.IsNullOrWhiteSpace(rawArgs))
        {
            if (dbType == DbType.String)
                return (255, null);
            if (dbType == DbType.Decimal || dbType == DbType.Double || dbType == DbType.Currency)
                return (null, 2);
            return (null, null);
        }

        var args = rawArgs.Split(',').Select(x => x.Trim()).Where(x => x.Length > 0).ToArray();
        if (dbType == DbType.String)
        {
            if (args.Length > 0 && int.TryParse(args[0], out var parsedSize) && parsedSize > 0)
                return (parsedSize, null);
            return (255, null);
        }

        if (dbType == DbType.Decimal || dbType == DbType.Double || dbType == DbType.Currency)
        {
            if (args.Length > 1 && int.TryParse(args[1], out var parsedScale) && parsedScale >= 0)
                return (null, parsedScale);
            return (null, 2);
        }

        return (null, null);
    }

    private static IReadOnlyList<string> ParsePrimaryKeyConstraint(string columnSql)
    {
        var m = Regex.Match(
            columnSql,
            @"^(CONSTRAINT\s+`?[A-Za-z0-9_]+`?\s+)?PRIMARY\s+KEY\s*\((?<cols>[^)]*)\)\s*$",
            RegexOptions.IgnoreCase | RegexOptions.Singleline);

        if (!m.Success)
            return [];

        var cols = m.Groups["cols"].Value
            .Split(',')
            .Select(x => x.Trim())
            .Where(x => !string.IsNullOrWhiteSpace(x))
            .Select(static name => name.NormalizeName())
            .Where(static name => !string.IsNullOrWhiteSpace(name))
            .ToList();

        return cols;
    }

    private static DbType ParseDbTypeFromSqlType(string sqlType)
    {
        var normalizedType = sqlType.Trim().NormalizeName();
        if (normalizedType.StartsWith("LONG RAW", StringComparison.OrdinalIgnoreCase)
            || normalizedType.StartsWith("RAW", StringComparison.OrdinalIgnoreCase))
            return DbType.Binary;

        var typeNameEnd = normalizedType.IndexOf('(');
        var spaceIndex = normalizedType.IndexOf(' ');
        if (spaceIndex >= 0 && (typeNameEnd < 0 || spaceIndex < typeNameEnd))
            typeNameEnd = spaceIndex;

        var typeName = typeNameEnd >= 0
            ? normalizedType[..typeNameEnd]
            : normalizedType;

        return typeName switch
        {
            "INT" or "INTEGER" or "SMALLINT" => DbType.Int32,
            "BIGINT" => DbType.Int64,
            "DECIMAL" or "NUMERIC" => DbType.Decimal,
            "NUMBER" => DbType.Decimal,
            "BINARY_DOUBLE" => DbType.Double,
            "BINARY_FLOAT" => DbType.Single,
            "FLOAT" or "REAL" or "DOUBLE" => DbType.Double,
            "BIT" => DbType.Boolean,
            "BOOLEAN" or "BOOL" => DbType.Boolean,
            "DATE" => DbType.Date,
            "TIMESTAMP" or "DATETIME" => DbType.DateTime,
            "GUID" or "UUID" => DbType.Guid,
            "BLOB" or "BINARY" or "VARBINARY" or "RAW" => DbType.Binary,
            _ => DbType.String,
        };
    }

    /// <summary>
    /// EN: Implements ExecuteCreateTemporaryTableAsSelect.
    /// PT: Implementa ExecuteCreateTemporaryTableAsSelect.
    /// </summary>
    public static DmlExecutionResult ExecuteCreateTemporaryTableAsSelect(
        this DbConnectionMockBase connection,
        SqlCreateTemporaryTableQuery query,
        DbParameterCollection pars,
        ISqlDialect dialect)
        => connection.ExecuteCreateTemporaryTableAsSelect(query, new QueryExecutionContext(connection, dialect, pars));

    /// <summary>
    /// EN: Implements ExecuteCreateTemporaryTableAsSelect using a pre-built execution context.
    /// PT: Implementa ExecuteCreateTemporaryTableAsSelect usando um contexto de execucao pre-construido.
    /// </summary>
    public static DmlExecutionResult ExecuteCreateTemporaryTableAsSelect(
        this DbConnectionMockBase connection,
        SqlCreateTemporaryTableQuery query,
        QueryExecutionContext context)
    {
        DmlExecutionResult affected;
        affected = connection.Db.ExecuteWithLock(() => ExecuteCreateTemporaryTableAsSelectImpl(connection, query, context));

        connection.SetLastFoundRows(affected.AffectedRows);
        return affected;
    }

    private static DmlExecutionResult ExecuteCreateTemporaryTableAsSelectImpl(
        this DbConnectionMockBase connection,
        SqlCreateTemporaryTableQuery query,
        QueryExecutionContext context)
    {
        var tableName = query.Table?.Name?.NormalizeName();
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(tableName, nameof(tableName));

        var schemaName = query.Table?.DbName;
        var tempScope = GetEffectiveTemporaryTableScope(query.Scope, context.Dialect);
        ITableMock newTable = null!;

        if (!query.Temporary)
        {
            if (connection.TryGetTable(tableName!, out _, schemaName))
            {
                if (query.IfNotExists)
                    return new DmlExecutionResult();

                throw new InvalidOperationException(SqlExceptionMessages.TableAlreadyExists(tableName!));
            }

            newTable = connection.AddTable(tableName!, schemaName: schemaName);
        }
        else if (tempScope == TemporaryTableScope.Global)
        {
            if (connection.TryGetGlobalTemporaryTable(tableName!, out _, schemaName))
            {
                if (query.IfNotExists) return new DmlExecutionResult();
                throw new InvalidOperationException(SqlExceptionMessages.TableAlreadyExists(tableName!));
            }
        }
        else if (connection.TryGetTemporaryTable(tableName!, out _, schemaName))
        {
            if (query.IfNotExists) return new DmlExecutionResult();
            throw new InvalidOperationException(SqlExceptionMessages.TableAlreadyExists(tableName!));
        }

        var executor = context.CreateExecutor();
        if (query.Temporary)
        {
            if (tempScope == TemporaryTableScope.Global)
                newTable = connection.AddGlobalTemporaryTable(tableName!, schemaName: schemaName);
            else
                newTable = connection.AddTemporaryTable(tableName!, schemaName: schemaName);
        }

        if (query.AsSelect is null)
        {
            if (query.ColumnDefinitions.Count > 0)
            {
                foreach (var column in query.ColumnDefinitions)
                    AddTemporaryTableColumn(newTable, column);
            }

            ApplyTableCheckConstraints(newTable, query.CheckConstraints);

            if (query.PrimaryKeyColumns.Count > 0)
                newTable.AddPrimaryKeyIndexes([.. query.PrimaryKeyColumns]);

            if (!string.IsNullOrWhiteSpace(query.PartitionClauseSql)
                && !query.Temporary
                && newTable is TableMock newTableMock2)
            {
                newTableMock2.PartitionClauseSql = query.PartitionClauseSql;
            }

            return new DmlExecutionResult();
        }

        var res = executor.ExecuteSelect(query.AsSelect);

        if (query.ColumnDefinitions.Count > 0)
        {
            foreach (var column in query.ColumnDefinitions)
                AddTemporaryTableColumn(newTable, column);
        }
        else
        {
            // column names: prefer explicit list if provided; else use select result columns
            var names = (query.ColumnNames is { Count: > 0 })
                ? query.ColumnNames
                : [.. res.Columns.Select(c => c.ColumnName)];

            for (var i = 0; i < names.Count; i++)
            {
                var colName = names[i];
                var dbType = (i < res.Columns.Count) ? InferDbType(res, i) : DbType.String;
                AddInferredColumn(newTable, colName, dbType);
            }
        }

        ApplyTableCheckConstraints(newTable, query.CheckConstraints);

        if (query.PrimaryKeyColumns.Count > 0)
            newTable.AddPrimaryKeyIndexes([.. query.PrimaryKeyColumns]);

        if (!string.IsNullOrWhiteSpace(query.PartitionClauseSql)
            && !query.Temporary
            && newTable is TableMock newTableMock)
        {
            newTableMock.PartitionClauseSql = query.PartitionClauseSql;
        }

        foreach (var row in res)
        {
            var d = new Dictionary<int, object?>();
            for (var i = 0; i < newTable.Columns.Count; i++)
                d[i] = row.TryGetValue(i, out var v) ? v : null;
            newTable.Add(d);
        }

        return new DmlExecutionResult();
    }

    private static TemporaryTableScope GetEffectiveTemporaryTableScope(
        TemporaryTableScope scope,
        ISqlDialect dialect)
        => scope;

    /// <summary>
    /// EN: Implements ExecuteInsertSmart.
    /// PT: Implementa ExecuteInsertSmart.
    /// </summary>
    public static DmlExecutionResult ExecuteInsertSmart(
            this DbConnectionMockBase connection,
            SqlInsertQuery query,
            QueryExecutionContext context)
    {
        // Preserve existing behavior for VALUES inserts.
        // If it is INSERT ... SELECT ..., execute select and insert rows.
        if (query.InsertSelect != null)
            return connection.ExecuteInsertSelect(query, context);

        // fall back to original extension in MySqlInsertStrategy
        return connection.ExecuteInsert(query, context);
    }

    /// <summary>
    /// EN: Implements ExecuteInsertSelect.
    /// PT: Implementa ExecuteInsertSelect.
    /// </summary>
    public static DmlExecutionResult ExecuteInsertSelect(
        this DbConnectionMockBase connection,
        SqlInsertQuery query,
        QueryExecutionContext context)
    {
        return connection.Db.ExecuteWithLock(() => ExecuteInsertSelectImpl(connection, query, context));
    }

    private static DmlExecutionResult ExecuteInsertSelectImpl(
        DbConnectionMockBase connection,
        SqlInsertQuery query,
        QueryExecutionContext context)
    {
        var plan = BuildInsertSelectPlan(connection, query);
        var executor = context.CreateExecutor();
        var q = SqlQueryParser.Parse(
            plan.SelectSql,
            context.Connection.Db,
            context.Dialect,
            null,
            SqlCustomFunctionResolverFactory.Create(context));
        var res = executor.ExecuteSelect((SqlSelectQuery)q);

        if (plan.Columns.Count != res.Columns.Count)
            throw new InvalidOperationException(SqlExceptionMessages.ColumnCountDoesNotMatchSelectList());

        if (plan.Target is TableMock targetTableMock && !targetTableMock.TriggerManager.HasRegisteredTriggers())
        {
            var rows = new List<Dictionary<int, object?>>(res.Count);
            foreach (var row in res)
            {
                var newRow = CreateInsertSelectRow(plan.Target, plan.Columns, row);
                FillMissingInsertSelectValues(plan.Target, newRow);
                rows.Add(newRow);
            }

            targetTableMock.AddBatch(rows);
            if (connection.Metrics.Enabled)
                connection.Metrics.Inserts += rows.Count;
            return DmlExecutionResult.ForCount(rows.Count);
        }

        var inserted = new DmlExecutionResult();
        foreach (var row in res)
        {
            var newRow = CreateInsertSelectRow(plan.Target, plan.Columns, row);
            FillMissingInsertSelectValues(plan.Target, newRow);
            plan.Target.Add(newRow);
            plan.Target.UpdateIndexesWithRow(plan.Target.Count - 1);
            if (connection.Metrics.Enabled)
                connection.Metrics.Inserts++;
            inserted.IncreseAffected();
        }
        return inserted;
    }
    private static InsertSelectPlan BuildInsertSelectPlan(
        DbConnectionMockBase connection,
        SqlInsertQuery query)
    {
        var match = MatchInsertSelectStatement(query.RawSql);
        var tableName = match.Groups["table"].Value.NormalizeName();
        var target = GetInsertSelectTarget(connection, tableName);
        var columns = ParseInsertSelectColumns(match);
        var selectSql = match.Groups["select"].Value;
        return new InsertSelectPlan(target, columns, selectSql);
    }

    private static Match MatchInsertSelectStatement(string rawSql)
    {
        var match = Regex.Match(
            rawSql,
            @"^INSERT\s+INTO\s+`?(?<table>[A-Za-z0-9_]+)`?\s*\((?<cols>[^)]*)\)\s*(?<select>(SELECT|WITH)\s+.*)$",
            RegexOptions.IgnoreCase | RegexOptions.Singleline);
        if (!match.Success)
            throw new InvalidOperationException(SqlExceptionMessages.InvalidInsertSelectStatement());

        return match;
    }

    private static ITableMock GetInsertSelectTarget(DbConnectionMockBase connection, string tableName)
    {
        if (connection.TryGetTable(tableName, out var target)
            && target != null)
        {
            return target;
        }

        throw SqlUnsupported.ForTableDoesNotExist(tableName);
    }

    private static List<string> ParseInsertSelectColumns(Match match)
        => [.. match.Groups["cols"].Value.Split(',').Select(c => c.Replace("`", string.Empty).Trim())];

    private static Dictionary<int, object?> CreateInsertSelectRow(
        ITableMock target,
        IReadOnlyList<string> columns,
        IReadOnlyDictionary<int, object?> row)
    {
        var newRow = new Dictionary<int, object?>(columns.Count);
        for (var i = 0; i < columns.Count; i++)
        {
            var colName = columns[i];
            var info = target.GetColumn(colName);
            if (info.GetGenValue is not null)
                continue;

            var value = row.TryGetValue(i, out var rawValue) ? rawValue : null;
            if (value is DBNull)
                value = null;
            if (value == null && !info.Nullable)
                throw target.ColumnCannotBeNull(colName);

            newRow[info.Index] = value;
        }

        return newRow;
    }

    private static void FillMissingInsertSelectValues(
        ITableMock target,
        Dictionary<int, object?> newRow)
    {
        foreach (var columnEntry in target.Columns)
        {
            var column = columnEntry.Value;
            if (newRow.ContainsKey(column.Index))
                continue;

            if (TryGetGeneratedInsertSelectValue(target, column, out var value))
            {
                newRow[column.Index] = value;
                continue;
            }

            if (!column.Nullable)
                throw target.ColumnCannotBeNull(columnEntry.Key);
        }
    }

    private static bool TryGetGeneratedInsertSelectValue(
        ITableMock target,
        ColumnDef column,
        out object? value)
    {
        if (column.Identity)
        {
            value = target.NextIdentity++;
            return true;
        }

        if (column.DefaultValue is SequenceDef sequenceDefault)
        {
            var targetSchema = sequenceDefault.OwnedBySchema ?? target.Schema.SchemaName;
            if (!target.Schema.Db.TryGetSequence(sequenceDefault.Name, out var sequence, targetSchema) || sequence is null)
                sequence = target.Schema.Db.AddSequence(
                    sequenceDefault.Name,
                    sequenceDefault.StartValue,
                    sequenceDefault.IncrementBy,
                    sequenceDefault.CurrentValue,
                    schemaName: targetSchema);

            value = sequence.NextValue();
            return true;
        }

        value = column.DefaultValue;
        return value is not null;
    }

    private sealed record InsertSelectPlan(
        ITableMock Target,
        IReadOnlyList<string> Columns,
        string SelectSql);

    private static DbType InferDbType(TableResultMock res, int colIndex)
    {
        foreach (var row in res)
        {
            if (!row.TryGetValue(colIndex, out var v)
                || v is null
                || v is DBNull) continue;
            return v switch
            {
                int or long or short or byte => DbType.Int32,
                decimal => DbType.Decimal,
                double or float => DbType.Double,
                bool => DbType.Boolean,
                Guid => DbType.Guid,
                DateTime => DbType.DateTime,
                byte[] => DbType.Binary,
                string => DbType.String,
                _ => DbType.Object
            };
        }
        return DbType.Object;
    }

    private static void AddInferredColumn(ITableMock table, string columnName, DbType dbType)
    {
        if (dbType == DbType.String)
        {
            table.AddColumn(columnName, dbType, nullable: true, size: 255);
            return;
        }

        if (dbType == DbType.Decimal || dbType == DbType.Double || dbType == DbType.Currency)
        {
            table.AddColumn(columnName, dbType, nullable: true, decimalPlaces: 2);
            return;
        }

        table.AddColumn(columnName, dbType, nullable: true);
    }

    private static void AddTemporaryTableColumn(ITableMock table, Col column)
        => table.AddColumn(
            column.name,
            column.dbType,
            column.nullable,
            column.size,
            column.decimalPlaces,
            column.identity,
            column.defaultValue,
            column.enumValues,
            column.computedExpression);

    private static void ApplyTableCheckConstraints(
        ITableMock table,
        IReadOnlyList<SchemaSnapshotCheckConstraint> checkConstraints)
    {
        if (checkConstraints.Count == 0)
            return;

        if (table is not TableMock tableMock)
            return;

        tableMock.CheckConstraintsMutable.AddRange(checkConstraints);
    }
}
