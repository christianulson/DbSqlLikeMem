using DbSqlLikeMem.VisualStudioExtension.Core.Models;

namespace DbSqlLikeMem.VisualStudioExtension.Core.Generation;

/// <summary>
/// EN: Builds database-specific SQL used to read schema objects and routine metadata.
/// PT: Monta SQL especifico de banco usado para ler objetos de schema e metadados de rotinas.
/// </summary>
public static class SqlMetadataQueryFactory
{
    private static readonly IReadOnlyDictionary<string, ISqlMetadataQueryStrategy> Strategies =
        new Dictionary<string, ISqlMetadataQueryStrategy>(StringComparer.Ordinal)
        {
            ["mysql"] = new MySqlMetadataQueryStrategy(),
            ["mariadb"] = new MariaDbMetadataQueryStrategy(),
            ["sqlserver"] = new SqlServerMetadataQueryStrategy(),
            ["sqlazure"] = new SqlServerMetadataQueryStrategy(),
            ["azuresql"] = new SqlServerMetadataQueryStrategy(),
            ["postgresql"] = new PostgreSqlMetadataQueryStrategy(),
            ["oracle"] = new OracleMetadataQueryStrategy(),
            ["sqlite"] = new SqliteMetadataQueryStrategy(),
            ["db2"] = new Db2MetadataQueryStrategy(),
            ["firebird"] = new FirebirdMetadataQueryStrategy(),
        };

    /// <summary>
    /// Builds the metadata query that lists schema objects for the specified database type.
    /// </summary>
    /// <param name="databaseType">The target database type.</param>
    /// <param name="filter">Filter object by Name.</param>
    /// <returns>The SQL query text.</returns>
    public static string BuildListObjectsQuery(string databaseType, string? filter = null)
        => ResolveStrategy(databaseType).BuildListObjectsQuery(filter);

    /// <summary>
    /// Builds the metadata query that lists columns for a specific object for the specified database type.
    /// </summary>
    /// <param name="databaseType">The target database type.</param>
    /// <returns>The SQL query text.</returns>
    public static string BuildObjectColumnsQuery(string databaseType)
        => ResolveStrategy(databaseType).BuildObjectColumnsQuery();

    /// <summary>
    /// Builds the metadata query that returns primary-key columns for the specified database type.
    /// </summary>
    /// <param name="databaseType">The target database type.</param>
    /// <returns>The SQL query text.</returns>
    public static string BuildPrimaryKeyQuery(string databaseType)
        => ResolveStrategy(databaseType).BuildPrimaryKeyQuery();

    /// <summary>
    /// Builds the metadata query that lists indexes for the specified database type.
    /// </summary>
    /// <param name="databaseType">The target database type.</param>
    /// <returns>The SQL query text.</returns>
    public static string BuildIndexesQuery(string databaseType)
        => ResolveStrategy(databaseType).BuildIndexesQuery();

    /// <summary>
    /// Builds the metadata query that lists foreign keys for the specified database type.
    /// </summary>
    /// <param name="databaseType">The target database type.</param>
    /// <returns>The SQL query text.</returns>
    public static string BuildForeignKeysQuery(string databaseType)
        => ResolveStrategy(databaseType).BuildForeignKeysQuery();

    /// <summary>
    /// Builds the metadata query that lists triggers for the specified database type.
    /// </summary>
    /// <param name="databaseType">The target database type.</param>
    /// <returns>The SQL query text.</returns>
    public static string BuildTriggersQuery(string databaseType)
        => ResolveStrategy(databaseType).BuildTriggersQuery();

    /// <summary>
    /// Builds the metadata query that returns sequence settings for the specified database type.
    /// </summary>
    /// <param name="databaseType">The target database type.</param>
    /// <returns>The SQL query text.</returns>
    public static string BuildSequenceMetadataQuery(string databaseType)
        => ResolveStrategy(databaseType).BuildSequenceMetadataQuery();

    /// <summary>
    /// Builds the metadata query that returns routine-level details for the specified database type.
    /// </summary>
    /// <param name="databaseType">The target database type.</param>
    /// <returns>The SQL query text.</returns>
    public static string BuildRoutineMetadataQuery(string databaseType)
        => ResolveStrategy(databaseType).BuildRoutineMetadataQuery();

    /// <summary>
    /// Builds the metadata query that returns routine parameters for the specified database type.
    /// </summary>
    /// <param name="databaseType">The target database type.</param>
    /// <returns>The SQL query text.</returns>
    public static string BuildRoutineParametersQuery(string databaseType)
        => ResolveStrategy(databaseType).BuildRoutineParametersQuery();

    private static ISqlMetadataQueryStrategy ResolveStrategy(string databaseType)
    {
        var normalizedType = DatabaseTypeNormalizer.NormalizeKey(databaseType);
        if (Strategies.TryGetValue(normalizedType, out var strategy))
        {
            return strategy;
        }

        throw new NotSupportedException($"Database type not supported for metadata queries: {databaseType}");
    }

    private static string AppendFilter(string? filter, string columnName)
        => string.IsNullOrWhiteSpace(filter) ? string.Empty : $" AND {columnName} {filter}";

    private static string EmptyRoutineMetadataQuery()
        => """
SELECT '' AS SchemaName
     , '' AS ObjectName
     , '' AS RoutineType
     , '' AS ReturnTypeSql
     , '' AS BodySql
 WHERE 1=0;
""";

    private static string EmptyRoutineParametersQuery()
        => """
SELECT '' AS SchemaName
     , '' AS ObjectName
     , '' AS ParameterName
     , '' AS ParameterMode
     , '' AS DataType
     , CAST(0 AS INT) AS Ordinal
     , '' AS DefaultValue
     , '' AS IsNullable
     , CAST(NULL AS BIGINT) AS CharMaxLen
     , CAST(NULL AS INT) AS NumPrecision
     , CAST(NULL AS INT) AS NumScale
     , '0' AS IsVariadic
     , '0' AS IsOrderByClause
     , '0' AS IsFrameClause
 WHERE 1=0;
""";

    private interface ISqlMetadataQueryStrategy
    {
        string BuildListObjectsQuery(string? filter);
        string BuildObjectColumnsQuery();
        string BuildPrimaryKeyQuery();
        string BuildIndexesQuery();
        string BuildForeignKeysQuery();
        string BuildTriggersQuery();
        string BuildSequenceMetadataQuery();
        string BuildRoutineMetadataQuery();
        string BuildRoutineParametersQuery();
    }

    private class MySqlMetadataQueryStrategy : ISqlMetadataQueryStrategy
    {
        public virtual string BuildListObjectsQuery(string? filter)
            => $"""
  SELECT TABLE_SCHEMA AS `SchemaName`
       , TABLE_NAME AS ObjectName
       , CASE TABLE_TYPE
         WHEN 'BASE TABLE' THEN 'Table'
         WHEN 'VIEW' THEN 'View'
         WHEN 'SEQUENCE' THEN 'Sequence'
         ELSE TABLE_TYPE
         END AS `ObjectType`
    FROM INFORMATION_SCHEMA.TABLES
   WHERE TABLE_SCHEMA = @databaseName{AppendFilter(filter, "TABLE_NAME")}
   UNION ALL
  SELECT ROUTINE_SCHEMA AS `SchemaName`
       , ROUTINE_NAME AS `ObjectName`
       , CASE ROUTINE_TYPE
         WHEN 'FUNCTION' THEN 'Function'
         ELSE 'Procedure'
         END AS `ObjectType`
    FROM INFORMATION_SCHEMA.ROUTINES
   WHERE ROUTINE_SCHEMA = @databaseName{AppendFilter(filter, "ROUTINE_NAME")}
ORDER BY SchemaName, ObjectType, ObjectName;
""";

        public string BuildObjectColumnsQuery()
            => """
  SELECT COLUMN_NAME AS `ColumnName`
       , DATA_TYPE AS `DataType`
       , ORDINAL_POSITION AS `Ordinal`
       , IS_NULLABLE AS `IsNullable`
       , EXTRA AS `Extra`
       , COLUMN_DEFAULT AS `DefaultValue`
       , CHARACTER_MAXIMUM_LENGTH AS `CharMaxLen`
       , NUMERIC_PRECISION AS `NumPrecision`
       , NUMERIC_SCALE AS 'NumScale'
       , COLUMN_TYPE AS `ColumnType`
       , GENERATION_EXPRESSION AS `ColumnGenerated`
    FROM INFORMATION_SCHEMA.COLUMNS
   WHERE TABLE_SCHEMA = @schemaName
     AND TABLE_NAME = @objectName
ORDER BY ORDINAL_POSITION;
""";

        public string BuildPrimaryKeyQuery()
            => @"
  SELECT COLUMN_NAME AS `ColumnName` 
    FROM INFORMATION_SCHEMA.STATISTICS 
   WHERE TABLE_SCHEMA=@schemaName 
     AND TABLE_NAME=@objectName 
     AND INDEX_NAME='PRIMARY' 
ORDER BY SEQ_IN_INDEX;";

        public string BuildIndexesQuery()
            => @"
  SELECT INDEX_NAME AS `IndexName`
       , NON_UNIQUE AS `NonUnique`
       , COLUMN_NAME AS `ColumnName`
       , SEQ_IN_INDEX AS `Seq` 
    FROM INFORMATION_SCHEMA.STATISTICS 
   WHERE TABLE_SCHEMA=@schemaName 
     AND TABLE_NAME=@objectName 
ORDER BY INDEX_NAME, SEQ_IN_INDEX;
";

        public string BuildForeignKeysQuery()
            => """
SELECT KCU.COLUMN_NAME AS `ColumnName`
     , KCU.REFERENCED_TABLE_NAME AS `RefTable`
     , KCU.REFERENCED_COLUMN_NAME AS `RefColumn`
  FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE KCU
 WHERE KCU.TABLE_SCHEMA=@schemaName
   AND KCU.TABLE_NAME=@objectName
   AND KCU.REFERENCED_TABLE_NAME IS NOT NULL
 ORDER BY KCU.CONSTRAINT_NAME
        , KCU.ORDINAL_POSITION;
""";

        public string BuildTriggersQuery()
            => """
SELECT TRIGGER_NAME AS `TriggerName`
  FROM INFORMATION_SCHEMA.TRIGGERS
 WHERE TRIGGER_SCHEMA=@schemaName
   AND EVENT_OBJECT_TABLE=@objectName
 ORDER BY TRIGGER_NAME;
""";

        public virtual string BuildSequenceMetadataQuery()
            => "SELECT '' AS StartValue, '' AS IncrementBy, '' AS CurrentValue WHERE 1=0;";

        public string BuildRoutineMetadataQuery()
            => """
SELECT ROUTINE_SCHEMA AS SchemaName
     , ROUTINE_NAME AS ObjectName
     , ROUTINE_TYPE AS RoutineType
     , DATA_TYPE AS ReturnTypeSql
     , ROUTINE_DEFINITION AS BodySql
  FROM INFORMATION_SCHEMA.ROUTINES
 WHERE ROUTINE_SCHEMA=@schemaName
   AND ROUTINE_NAME=@objectName;
""";

        public string BuildRoutineParametersQuery()
            => """
SELECT SPECIFIC_SCHEMA AS SchemaName
     , SPECIFIC_NAME AS ObjectName
     , COALESCE(NULLIF(PARAMETER_NAME, ''), CONCAT(SPECIFIC_NAME, '_return')) AS ParameterName
     , CASE
         WHEN ORDINAL_POSITION = 0 THEN 'RETURN'
         ELSE COALESCE(PARAMETER_MODE, 'IN')
       END AS ParameterMode
     , DATA_TYPE AS DataType
     , ORDINAL_POSITION AS Ordinal
     , '' AS DefaultValue
     , CASE WHEN ORDINAL_POSITION = 0 THEN 'NO' ELSE 'YES' END AS IsNullable
     , CHARACTER_MAXIMUM_LENGTH AS CharMaxLen
     , NUMERIC_PRECISION AS NumPrecision
     , NUMERIC_SCALE AS NumScale
     , CASE WHEN COALESCE(PARAMETER_MODE, '') LIKE '%VARIADIC%' THEN '1' ELSE '0' END AS IsVariadic
     , '0' AS IsOrderByClause
     , '0' AS IsFrameClause
  FROM INFORMATION_SCHEMA.PARAMETERS
 WHERE SPECIFIC_SCHEMA=@schemaName
   AND SPECIFIC_NAME=@objectName
 ORDER BY ORDINAL_POSITION;
""";
    }

    private sealed class MariaDbMetadataQueryStrategy : MySqlMetadataQueryStrategy
    {
        public override string BuildListObjectsQuery(string? filter)
            => $"""
  SELECT TABLE_SCHEMA AS `SchemaName`
       , TABLE_NAME AS ObjectName
       , CASE TABLE_TYPE
         WHEN 'BASE TABLE' THEN 'Table'
         WHEN 'VIEW' THEN 'View'
         ELSE TABLE_TYPE
         END AS `ObjectType`
    FROM INFORMATION_SCHEMA.TABLES
   WHERE TABLE_SCHEMA = @databaseName{AppendFilter(filter, "TABLE_NAME")}
   UNION ALL
  SELECT ROUTINE_SCHEMA AS `SchemaName`
       , ROUTINE_NAME AS `ObjectName`
       , CASE ROUTINE_TYPE
         WHEN 'FUNCTION' THEN 'Function'
         ELSE 'Procedure'
         END AS `ObjectType`
    FROM INFORMATION_SCHEMA.ROUTINES
   WHERE ROUTINE_SCHEMA = @databaseName{AppendFilter(filter, "ROUTINE_NAME")}
   UNION ALL
  SELECT TABLE_SCHEMA AS `SchemaName`
       , TABLE_NAME AS `ObjectName`
       , 'Sequence' AS `ObjectType`
    FROM INFORMATION_SCHEMA.TABLES
   WHERE TABLE_SCHEMA = @databaseName
     AND TABLE_TYPE = 'SEQUENCE'
     {AppendFilter(filter, "TABLE_NAME")}
ORDER BY SchemaName, ObjectType, ObjectName;
""";

        public override string BuildSequenceMetadataQuery()
            => """
SELECT SEQUENCE_SCHEMA AS SchemaName
     , SEQUENCE_NAME AS ObjectName
     , CAST(START_VALUE AS CHAR) AS StartValue
     , CAST(INCREMENT AS CHAR) AS IncrementBy
     , CAST(NULL AS CHAR) AS CurrentValue
  FROM INFORMATION_SCHEMA.SEQUENCES
 WHERE SEQUENCE_SCHEMA = @schemaName
   AND SEQUENCE_NAME = @objectName;
""";
    }

    private sealed class SqlServerMetadataQueryStrategy : ISqlMetadataQueryStrategy
    {
        public string BuildListObjectsQuery(string? filter)
            => $"""
  SELECT s.name AS [SchemaName]
       , o.name AS [ObjectName]
       , CASE o.type
         WHEN 'U' THEN 'Table'
         WHEN 'V' THEN 'View'
         WHEN 'P' THEN 'Procedure'
         WHEN 'SO' THEN 'Sequence'
         WHEN 'FN' THEN 'Function'
         WHEN 'IF' THEN 'Function'
         WHEN 'TF' THEN 'Function'
         WHEN 'FS' THEN 'Function'
         WHEN 'FT' THEN 'Function'
         ELSE o.type
         END AS [ObjectType]
    FROM sys.objects o
    JOIN sys.schemas s ON s.schema_id = o.schema_id
   WHERE o.type IN ('U', 'V', 'P', 'SO', 'FN', 'IF', 'TF', 'FS', 'FT'){AppendFilter(filter, "o.name")}
ORDER BY s.name
       , ObjectType
       , o.name;
""";

        public string BuildObjectColumnsQuery()
            => """
  SELECT c.name AS [ColumnName]
       , t.name AS [DataType]
       , c.column_id AS [Ordinal]
       , c.is_nullable AS [IsNullable]
       , c.is_identity AS [IsIdentity]
       , OBJECT_DEFINITION(c.default_object_id) AS [DefaultValue]
       , c.max_length AS [CharMaxLen]
       , c.precision AS [NumPrecision]
       , c.scale AS [NumScale]
       , '' AS [ColumnType]
       , '' AS [ColumnGenerated]
    FROM sys.columns c
    JOIN sys.types t ON t.user_type_id = c.user_type_id
    JOIN sys.objects o ON o.object_id = c.object_id
    JOIN sys.schemas s ON s.schema_id = o.schema_id
   WHERE s.name = @schemaName AND o.name = @objectName
ORDER BY c.column_id;
""";

        public string BuildPrimaryKeyQuery()
            => """
SELECT c.name AS [ColumnName]
  FROM sys.indexes i
  JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
  JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
  JOIN sys.objects o ON o.object_id = i.object_id
  JOIN sys.schemas s ON s.schema_id = o.schema_id
 WHERE i.is_primary_key = 1
   AND s.name=@schemaName
   AND o.name=@objectName
 ORDER BY ic.key_ordinal;
""";

        public string BuildIndexesQuery()
            => """
  SELECT i.name AS [IndexName]
       , i.is_unique AS [IsUnique]
       , c.name AS [ColumnName]
       , ic.key_ordinal AS [Seq]
    FROM sys.indexes i
    JOIN sys.index_columns ic ON ic.object_id=i.object_id AND ic.index_id=i.index_id
    JOIN sys.columns c ON c.object_id=ic.object_id AND c.column_id=ic.column_id
    JOIN sys.objects o ON o.object_id=i.object_id
    JOIN sys.schemas s ON s.schema_id=o.schema_id
   WHERE i.is_hypothetical=0 
     AND i.name IS NOT NULL 
     AND s.name=@schemaName 
     AND o.name=@objectName
ORDER BY i.name, ic.key_ordinal;
""";

        public string BuildForeignKeysQuery()
            => """
  SELECT pc.name AS [ColumnName]
       , rt.name AS [RefTable]
       , rc.name AS [RefColumn]
    FROM sys.foreign_key_columns fkc
    JOIN sys.objects o ON o.object_id=fkc.parent_object_id
    JOIN sys.schemas s ON s.schema_id=o.schema_id
    JOIN sys.columns pc ON pc.object_id=fkc.parent_object_id AND pc.column_id=fkc.parent_column_id
    JOIN sys.objects rt ON rt.object_id=fkc.referenced_object_id
    JOIN sys.columns rc ON rc.object_id=fkc.referenced_object_id 
                       AND rc.column_id=fkc.referenced_column_id
   WHERE s.name=@schemaName AND o.name=@objectName
ORDER BY fkc.constraint_column_id;
""";

        public string BuildTriggersQuery()
            => """
  SELECT tr.name AS [TriggerName]
    FROM sys.triggers tr
    JOIN sys.objects o ON o.object_id=tr.parent_id
    JOIN sys.schemas s ON s.schema_id=o.schema_id
   WHERE s.name=@schemaName AND o.name=@objectName
ORDER BY tr.name;
""";

        public string BuildSequenceMetadataQuery()
            => """
SELECT CAST(seq.start_value AS bigint) AS [StartValue]
     , CAST(seq.increment AS bigint) AS [IncrementBy]
     , CAST(seq.current_value AS bigint) AS [CurrentValue]
  FROM sys.sequences seq
  JOIN sys.schemas s ON s.schema_id=seq.schema_id
 WHERE s.name=@schemaName
   AND seq.name=@objectName;
""";

        public string BuildRoutineMetadataQuery()
            => """
SELECT s.name AS [SchemaName]
     , o.name AS [ObjectName]
     , CASE WHEN o.type IN ('FN', 'IF', 'TF', 'FS', 'FT') THEN 'Function' ELSE 'Procedure' END AS [RoutineType]
     , COALESCE(t.name, '') AS [ReturnTypeSql]
     , COALESCE(m.definition, OBJECT_DEFINITION(o.object_id), '') AS [BodySql]
  FROM sys.objects o
  JOIN sys.schemas s ON s.schema_id = o.schema_id
  LEFT JOIN sys.sql_modules m ON m.object_id = o.object_id
  LEFT JOIN sys.parameters p0 ON p0.object_id = o.object_id AND p0.parameter_id = 0
  LEFT JOIN sys.types t ON t.user_type_id = p0.user_type_id
 WHERE s.name = @schemaName
   AND o.name = @objectName
   AND o.type IN ('P', 'FN', 'IF', 'TF', 'FS', 'FT');
""";

        public string BuildRoutineParametersQuery()
            => """
SELECT s.name AS [SchemaName]
     , o.name AS [ObjectName]
     , CASE
         WHEN p.parameter_id = 0 THEN 'RETURN'
         WHEN p.is_output = 1 THEN 'OUT'
         ELSE 'IN'
       END AS [ParameterMode]
     , CASE WHEN p.parameter_id = 0 THEN 'return' ELSE REPLACE(REPLACE(p.name, '@', ''), ' ', '') END AS [ParameterName]
     , COALESCE(t.name, '') AS [DataType]
     , p.parameter_id AS [Ordinal]
     , CASE WHEN p.has_default_value = 1 THEN COALESCE(CONVERT(nvarchar(4000), p.default_value), '') ELSE '' END AS [DefaultValue]
     , CASE WHEN p.is_nullable = 1 THEN 'YES' ELSE 'NO' END AS [IsNullable]
     , p.max_length AS [CharMaxLen]
     , p.precision AS [NumPrecision]
     , p.scale AS [NumScale]
     , '0' AS [IsVariadic]
     , '0' AS [IsOrderByClause]
     , '0' AS [IsFrameClause]
  FROM sys.parameters p
  JOIN sys.objects o ON o.object_id = p.object_id
  JOIN sys.schemas s ON s.schema_id = o.schema_id
  LEFT JOIN sys.types t ON t.user_type_id = p.user_type_id
 WHERE s.name = @schemaName
   AND o.name = @objectName
   AND o.type IN ('P', 'FN', 'IF', 'TF', 'FS', 'FT')
 ORDER BY p.parameter_id;
""";
    }

    private sealed class PostgreSqlMetadataQueryStrategy : ISqlMetadataQueryStrategy
    {
        public string BuildListObjectsQuery(string? filter)
            => $"""
SELECT n.nspname AS SchemaName, c.relname AS ObjectName,
       CASE c.relkind WHEN 'r' THEN 'Table' WHEN 'v' THEN 'View' ELSE c.relkind::text END AS ObjectType
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind IN ('r','v') AND n.nspname NOT IN ('pg_catalog', 'information_schema')
{AppendFilter(filter, "c.relname")}
UNION ALL
SELECT routine_schema AS SchemaName, routine_name AS ObjectName,
       CASE routine_type WHEN 'FUNCTION' THEN 'Function' ELSE 'Procedure' END AS ObjectType
FROM information_schema.routines
WHERE routine_schema NOT IN ('pg_catalog', 'information_schema')
{AppendFilter(filter, "routine_name")}
UNION ALL
SELECT sequence_schema AS SchemaName, sequence_name AS ObjectName, 'Sequence' AS ObjectType
FROM information_schema.sequences
WHERE sequence_schema NOT IN ('pg_catalog', 'information_schema')
{AppendFilter(filter, "sequence_name")}
ORDER BY SchemaName, ObjectType, ObjectName;
""";

        public string BuildObjectColumnsQuery()
            => """
SELECT column_name AS ColumnName, data_type AS DataType, ordinal_position AS Ordinal,
       is_nullable AS IsNullable,
       CASE WHEN column_default LIKE 'nextval(%' THEN 'identity' ELSE '' END AS Extra,
       column_default AS DefaultValue,
       character_maximum_length AS CharMaxLen,
       numeric_precision AS NumPrecision,
       numeric_scale AS NumScale,
       udt_name AS ColumnType,
       '' AS ColumnGenerated
FROM information_schema.columns
WHERE table_schema = @schemaName AND table_name = @objectName
ORDER BY ordinal_position;
""";

        public string BuildPrimaryKeyQuery()
            => """
SELECT a.attname AS ColumnName
FROM pg_constraint con
JOIN pg_class rel ON rel.oid = con.conrelid
JOIN pg_namespace nsp ON nsp.oid = rel.relnamespace
JOIN unnest(con.conkey) WITH ORDINALITY AS cols(attnum, ord) ON true
JOIN pg_attribute a ON a.attrelid = rel.oid AND a.attnum = cols.attnum
WHERE con.contype='p' AND nsp.nspname=@schemaName AND rel.relname=@objectName
ORDER BY cols.ord;
""";

        public string BuildIndexesQuery()
            => """
SELECT i.relname AS IndexName, ix.indisunique AS IsUnique, a.attname AS ColumnName, ord.n AS Seq
FROM pg_class t
JOIN pg_namespace n ON n.oid=t.relnamespace
JOIN pg_index ix ON ix.indrelid=t.oid
JOIN pg_class i ON i.oid=ix.indexrelid
JOIN LATERAL unnest(ix.indkey) WITH ORDINALITY AS ord(attnum,n) ON true
JOIN pg_attribute a ON a.attrelid=t.oid AND a.attnum=ord.attnum
WHERE n.nspname=@schemaName AND t.relname=@objectName
ORDER BY i.relname, ord.n;
""";

        public string BuildForeignKeysQuery()
            => """
SELECT a.attname AS ColumnName, rel_ref.relname AS RefTable, a_ref.attname AS RefColumn
FROM pg_constraint con
JOIN pg_class rel ON rel.oid=con.conrelid
JOIN pg_namespace nsp ON nsp.oid=rel.relnamespace
JOIN pg_class rel_ref ON rel_ref.oid=con.confrelid
JOIN unnest(con.conkey, con.confkey) WITH ORDINALITY AS keys(attnum, refattnum, ord) ON true
JOIN pg_attribute a ON a.attrelid=rel.oid AND a.attnum=keys.attnum
JOIN pg_attribute a_ref ON a_ref.attrelid=rel_ref.oid AND a_ref.attnum=keys.refattnum
WHERE con.contype='f' AND nsp.nspname=@schemaName AND rel.relname=@objectName
ORDER BY keys.ord;
""";

        public string BuildTriggersQuery()
            => """
SELECT t.tgname AS TriggerName
FROM pg_trigger t
JOIN pg_class c ON c.oid=t.tgrelid
JOIN pg_namespace n ON n.oid=c.relnamespace
WHERE NOT t.tgisinternal AND n.nspname=@schemaName AND c.relname=@objectName
ORDER BY t.tgname;
""";

        public string BuildSequenceMetadataQuery()
            => """
SELECT start_value::bigint AS StartValue
     , increment_by::bigint AS IncrementBy
     , last_value::bigint AS CurrentValue
  FROM pg_sequences
 WHERE schemaname=@schemaName
   AND sequencename=@objectName;
""";

        public string BuildRoutineMetadataQuery()
            => """
SELECT routine_schema AS SchemaName
     , routine_name AS ObjectName
     , routine_type AS RoutineType
     , data_type AS ReturnTypeSql
     , routine_definition AS BodySql
  FROM information_schema.routines
 WHERE routine_schema = @schemaName
   AND routine_name = @objectName;
""";

        public string BuildRoutineParametersQuery()
            => """
SELECT r.routine_schema AS SchemaName
     , r.routine_name AS ObjectName
     , CASE
         WHEN ordinal_position = 0 THEN 'RETURN'
         ELSE COALESCE(parameter_mode, 'IN')
       END AS ParameterMode
     , COALESCE(p.parameter_name, 'return') AS ParameterName
     , p.data_type AS DataType
     , p.ordinal_position AS Ordinal
     , COALESCE(p.parameter_default, '') AS DefaultValue
     , CASE WHEN p.ordinal_position = 0 THEN 'NO' ELSE 'YES' END AS IsNullable
     , p.character_maximum_length AS CharMaxLen
     , p.numeric_precision AS NumPrecision
     , p.numeric_scale AS NumScale
     , CASE WHEN COALESCE(p.parameter_mode, '') LIKE '%VARIADIC%' THEN '1' ELSE '0' END AS IsVariadic
     , '0' AS IsOrderByClause
     , '0' AS IsFrameClause
  FROM information_schema.routines r
  JOIN information_schema.parameters p
    ON p.specific_schema = r.specific_schema
   AND p.specific_name = r.specific_name
 WHERE r.routine_schema = @schemaName
   AND r.routine_name = @objectName
 ORDER BY p.ordinal_position;
""";
    }

    private sealed class OracleMetadataQueryStrategy : ISqlMetadataQueryStrategy
    {
        public string BuildListObjectsQuery(string? filter)
            => $"""
SELECT OWNER AS SchemaName
     , OBJECT_NAME AS ObjectName
     , CASE OBJECT_TYPE 
       WHEN 'TABLE' THEN 'Table' 
       WHEN 'VIEW' THEN 'View' 
       WHEN 'PROCEDURE' THEN 'Procedure' 
       WHEN 'FUNCTION' THEN 'Function'
       WHEN 'SEQUENCE' THEN 'Sequence'
       ELSE OBJECT_TYPE 
       END AS ObjectType
FROM ALL_OBJECTS
WHERE OBJECT_TYPE IN ('TABLE', 'VIEW', 'PROCEDURE', 'FUNCTION', 'SEQUENCE')
  {AppendFilter(filter, "OBJECT_NAME")}
ORDER BY OWNER, ObjectType, OBJECT_NAME
""";

        public string BuildObjectColumnsQuery()
            => """
SELECT COLUMN_NAME AS ColumnName, DATA_TYPE AS DataType, COLUMN_ID AS Ordinal,
       NULLABLE AS IsNullable,
       '' AS Extra,
       DATA_DEFAULT AS DefaultValue,
       CHAR_LENGTH AS CharMaxLen,
       DATA_PRECISION AS NumPrecision,
       DATA_SCALE AS NumScale,
       DATA_TYPE AS ColumnType,
       '' AS ColumnGenerated
FROM ALL_TAB_COLUMNS
WHERE OWNER = :schemaName AND TABLE_NAME = :objectName
ORDER BY COLUMN_ID
""";

        public string BuildPrimaryKeyQuery()
            => """
SELECT cols.COLUMN_NAME AS ColumnName
FROM ALL_CONSTRAINTS cons
JOIN ALL_CONS_COLUMNS cols ON cols.OWNER = cons.OWNER AND cols.CONSTRAINT_NAME = cons.CONSTRAINT_NAME
WHERE cons.CONSTRAINT_TYPE='P' AND cons.OWNER=:schemaName AND cons.TABLE_NAME=:objectName
ORDER BY cols.POSITION
""";

        public string BuildIndexesQuery()
            => """
SELECT i.INDEX_NAME AS IndexName, i.UNIQUENESS AS Uniqueness, c.COLUMN_NAME AS ColumnName, c.COLUMN_POSITION AS Seq
FROM ALL_INDEXES i
JOIN ALL_IND_COLUMNS c ON c.INDEX_OWNER=i.OWNER AND c.INDEX_NAME=i.INDEX_NAME
WHERE i.TABLE_OWNER=:schemaName AND i.TABLE_NAME=:objectName
ORDER BY i.INDEX_NAME, c.COLUMN_POSITION
""";

        public string BuildForeignKeysQuery()
            => """
SELECT cc.COLUMN_NAME AS ColumnName, rcc.TABLE_NAME AS RefTable, rcc.COLUMN_NAME AS RefColumn
FROM ALL_CONSTRAINTS c
JOIN ALL_CONS_COLUMNS cc ON cc.OWNER=c.OWNER AND cc.CONSTRAINT_NAME=c.CONSTRAINT_NAME
JOIN ALL_CONSTRAINTS rc ON rc.OWNER=c.R_OWNER AND rc.CONSTRAINT_NAME=c.R_CONSTRAINT_NAME
JOIN ALL_CONS_COLUMNS rcc ON rcc.OWNER=rc.OWNER AND rcc.CONSTRAINT_NAME=rc.CONSTRAINT_NAME AND rcc.POSITION=cc.POSITION
WHERE c.CONSTRAINT_TYPE='R' AND c.OWNER=:schemaName AND c.TABLE_NAME=:objectName
ORDER BY cc.POSITION
""";

        public string BuildTriggersQuery()
            => """
SELECT TRIGGER_NAME AS TriggerName
FROM ALL_TRIGGERS
WHERE OWNER=:schemaName AND TABLE_NAME=:objectName
ORDER BY TRIGGER_NAME
""";

        public string BuildSequenceMetadataQuery()
            => """
SELECT LAST_NUMBER AS StartValue
     , INCREMENT_BY AS IncrementBy
     , LAST_NUMBER AS CurrentValue
FROM ALL_SEQUENCES
WHERE SEQUENCE_OWNER=:schemaName AND SEQUENCE_NAME=:objectName
""";

        public string BuildRoutineMetadataQuery()
            => """
SELECT p.OWNER AS SchemaName
     , p.OBJECT_NAME AS ObjectName
     , CASE p.OBJECT_TYPE WHEN 'FUNCTION' THEN 'Function' ELSE 'Procedure' END AS RoutineType
     , COALESCE((SELECT MAX(a.DATA_TYPE)
                   FROM ALL_ARGUMENTS a
                  WHERE a.OWNER = p.OWNER
                    AND a.OBJECT_NAME = p.OBJECT_NAME
                    AND a.OBJECT_ID = p.OBJECT_ID
                    AND a.POSITION = 0
                    AND a.DATA_LEVEL = 0), '') AS ReturnTypeSql
     , (SELECT LISTAGG(TRIM(s.TEXT), CHR(10)) WITHIN GROUP (ORDER BY s.LINE)
          FROM ALL_SOURCE s
         WHERE s.OWNER = p.OWNER
           AND s.NAME = p.OBJECT_NAME
           AND s.TYPE = CASE p.OBJECT_TYPE WHEN 'FUNCTION' THEN 'FUNCTION' ELSE 'PROCEDURE' END) AS BodySql
  FROM ALL_PROCEDURES p
 WHERE p.OWNER = :schemaName
   AND p.OBJECT_NAME = :objectName;
""";

        public string BuildRoutineParametersQuery()
            => """
SELECT a.OWNER AS SchemaName
     , a.OBJECT_NAME AS ObjectName
     , CASE
         WHEN a.POSITION = 0 THEN 'RETURN'
         WHEN a.IN_OUT = 'OUT' THEN 'OUT'
         ELSE 'IN'
       END AS ParameterMode
     , COALESCE(a.ARGUMENT_NAME, 'return') AS ParameterName
     , COALESCE(a.DATA_TYPE, '') AS DataType
     , a.POSITION AS Ordinal
     , CASE WHEN a.DEFAULTED = 'Y' THEN 'Y' ELSE '' END AS DefaultValue
     , CASE WHEN a.IN_OUT = 'OUT' THEN 'NO' ELSE 'YES' END AS IsNullable
     , a.CHAR_LENGTH AS CharMaxLen
     , a.DATA_PRECISION AS NumPrecision
     , a.DATA_SCALE AS NumScale
     , CASE WHEN a.IN_OUT = 'IN/OUT' THEN '1' ELSE '0' END AS IsVariadic
     , '0' AS IsOrderByClause
     , '0' AS IsFrameClause
  FROM ALL_ARGUMENTS a
 WHERE a.OWNER = :schemaName
   AND a.OBJECT_NAME = :objectName
 ORDER BY a.POSITION;
""";
    }

    private sealed class SqliteMetadataQueryStrategy : ISqlMetadataQueryStrategy
    {
        public string BuildListObjectsQuery(string? filter)
            => $"""
SELECT '' AS SchemaName
     , name AS ObjectName
     , CASE type WHEN 'table' THEN 'Table' WHEN 'view' THEN 'View' ELSE type END AS ObjectType
FROM sqlite_master
WHERE type IN ('table','view') AND name NOT LIKE 'sqlite_%'
{AppendFilter(filter, "name")}
ORDER BY ObjectType, ObjectName;
""";

        public string BuildObjectColumnsQuery()
            => """
SELECT name AS ColumnName, type AS DataType, cid AS Ordinal,
       CASE WHEN "notnull" = 1 THEN 'NO' ELSE 'YES' END AS IsNullable,
       CASE WHEN pk = 1 THEN 'pk' ELSE '' END AS Extra,
       dflt_value AS DefaultValue,
       NULL AS CharMaxLen,
       NULL AS NumPrecision,
       NULL AS NumScale,
       type AS ColumnType,
       '' AS ColumnGenerated
FROM pragma_table_info(@objectName)
ORDER BY cid;
""";

        public string BuildPrimaryKeyQuery()
            => "SELECT name AS ColumnName FROM pragma_table_info(@objectName) WHERE pk > 0 ORDER BY pk;";

        public string BuildIndexesQuery()
            => "SELECT '' AS IndexName, 0 AS IsUnique, '' AS ColumnName, 0 AS Seq WHERE 1=0;";

        public string BuildForeignKeysQuery()
            => "SELECT \"\" AS ColumnName, \"\" AS RefTable, \"\" AS RefColumn WHERE 1=0;";

        public string BuildTriggersQuery()
            => "SELECT \"\" AS TriggerName WHERE 1=0;";

        public string BuildSequenceMetadataQuery()
            => "SELECT '' AS StartValue, '' AS IncrementBy, '' AS CurrentValue WHERE 1=0;";

        public string BuildRoutineMetadataQuery()
            => EmptyRoutineMetadataQuery();

        public string BuildRoutineParametersQuery()
            => EmptyRoutineParametersQuery();
    }

    private sealed class Db2MetadataQueryStrategy : ISqlMetadataQueryStrategy
    {
        public string BuildListObjectsQuery(string? filter)
            => $"""
SELECT RTRIM(TABSCHEMA) AS SchemaName
     , RTRIM(TABNAME) AS ObjectName,
       CASE TYPE WHEN 'T' THEN 'Table' WHEN 'V' THEN 'View' ELSE TYPE END AS ObjectType
FROM SYSCAT.TABLES
WHERE TYPE IN ('T','V')
  {AppendFilter(filter, "RTRIM(TABNAME)")}
UNION ALL
SELECT RTRIM(ROUTINESCHEMA) AS SchemaName
     , RTRIM(ROUTINENAME) AS ObjectName
     , CASE ROUTINETYPE WHEN 'F' THEN 'Function' ELSE 'Procedure' END AS ObjectType
FROM SYSCAT.ROUTINES
WHERE ROUTINETYPE IN ('P', 'F')
  {AppendFilter(filter, "RTRIM(ROUTINENAME)")}
UNION ALL
SELECT RTRIM(SEQSCHEMA) AS SchemaName
     , RTRIM(SEQNAME) AS ObjectName
     , 'Sequence' AS ObjectType
FROM SYSCAT.SEQUENCES
WHERE 1=1
  {AppendFilter(filter, "RTRIM(SEQNAME)")}
ORDER BY SchemaName, ObjectType, ObjectName;
""";

        public string BuildObjectColumnsQuery()
            => """
SELECT RTRIM(COLNAME) AS ColumnName, RTRIM(TYPENAME) AS DataType, COLNO AS Ordinal,
       CASE NULLS WHEN 'Y' THEN 'YES' ELSE 'NO' END AS IsNullable,
       CASE IDENTITY WHEN 'Y' THEN 'identity' ELSE '' END AS Extra,
       DEFAULT AS DefaultValue,
       LENGTH AS CharMaxLen,
       SCALE AS NumPrecision,
       SCALE AS NumScale,
       RTRIM(TYPENAME) AS ColumnType,
       '' AS ColumnGenerated
FROM SYSCAT.COLUMNS
WHERE TABSCHEMA = @schemaName AND TABNAME = @objectName
ORDER BY COLNO;
""";

        public string BuildPrimaryKeyQuery()
            => """
SELECT RTRIM(c.COLNAME) AS ColumnName
FROM SYSCAT.TABCONST t
JOIN SYSCAT.KEYCOLUSE c ON c.CONSTNAME = t.CONSTNAME AND c.TABSCHEMA = t.TABSCHEMA
WHERE t.TYPE='P' AND t.TABSCHEMA=@schemaName AND t.TABNAME=@objectName
ORDER BY c.COLSEQ;
""";

        public string BuildIndexesQuery()
            => """
SELECT RTRIM(i.INDNAME) AS IndexName, i.UNIQUERULE AS UniqueRule, RTRIM(c.COLNAME) AS ColumnName, c.COLSEQ AS Seq
FROM SYSCAT.INDEXES i
JOIN SYSCAT.INDEXCOLUSE c ON c.INDSCHEMA=i.INDSCHEMA AND c.INDNAME=i.INDNAME
WHERE i.TABSCHEMA=@schemaName AND i.TABNAME=@objectName
ORDER BY i.INDNAME, c.COLSEQ;
""";

        public string BuildForeignKeysQuery()
            => """
SELECT RTRIM(k.COLNAME) AS ColumnName, RTRIM(r.REFTABNAME) AS RefTable, RTRIM(rk.COLNAME) AS RefColumn
FROM SYSCAT.REFERENCES r
JOIN SYSCAT.KEYCOLUSE k ON k.CONSTNAME=r.CONSTNAME AND k.TABSCHEMA=r.TABSCHEMA
JOIN SYSCAT.KEYCOLUSE rk ON rk.CONSTNAME=r.REFKEYNAME AND rk.TABSCHEMA=r.REFTABSCHEMA AND rk.COLSEQ=k.COLSEQ
WHERE r.TABSCHEMA=@schemaName AND r.TABNAME=@objectName
ORDER BY k.COLSEQ;
""";

        public string BuildTriggersQuery()
            => """
SELECT RTRIM(TRIGNAME) AS TriggerName
FROM SYSCAT.TRIGGERS
WHERE TABSCHEMA=@schemaName AND TABNAME=@objectName
ORDER BY TRIGNAME;
""";

        public string BuildSequenceMetadataQuery()
            => """
SELECT BIGINT(START) AS StartValue
     , BIGINT(INCREMENT) AS IncrementBy
     , CAST(NULL AS BIGINT) AS CurrentValue
FROM SYSCAT.SEQUENCES
WHERE RTRIM(SEQSCHEMA)=@schemaName AND RTRIM(SEQNAME)=@objectName;
""";

        public string BuildRoutineMetadataQuery()
            => """
SELECT RTRIM(ROUTINESCHEMA) AS SchemaName
     , RTRIM(ROUTINENAME) AS ObjectName
     , CASE ROUTINETYPE WHEN 'F' THEN 'Function' ELSE 'Procedure' END AS RoutineType
     , COALESCE(TEXT, '') AS BodySql
     , COALESCE(RETURNS, '') AS ReturnTypeSql
  FROM SYSCAT.ROUTINES
 WHERE RTRIM(ROUTINESCHEMA)=@schemaName
   AND RTRIM(ROUTINENAME)=@objectName;
""";

        public string BuildRoutineParametersQuery()
            => """
SELECT RTRIM(ROUTINESCHEMA) AS SchemaName
     , RTRIM(ROUTINENAME) AS ObjectName
     , CASE
         WHEN ORDINAL = 0 THEN 'RETURN'
         WHEN COALESCE(PARM_MODE, 'IN') IN ('O', 'OUT') THEN 'OUT'
         ELSE 'IN'
       END AS ParameterMode
     , COALESCE(RTRIM(PARMNAME), 'return') AS ParameterName
     , COALESCE(RTRIM(TYPENAME), '') AS DataType
     , ORDINAL AS Ordinal
     , COALESCE(DEFAULT, '') AS DefaultValue
     , CASE WHEN NULLS = 'Y' THEN 'YES' ELSE 'NO' END AS IsNullable
     , LENGTH AS CharMaxLen
     , SCALE AS NumPrecision
     , SCALE AS NumScale
     , CASE WHEN COALESCE(PARM_MODE, '') = 'V' THEN '1' ELSE '0' END AS IsVariadic
     , '0' AS IsOrderByClause
     , '0' AS IsFrameClause
  FROM SYSCAT.ROUTINEPARMS
 WHERE RTRIM(ROUTINESCHEMA)=@schemaName
   AND RTRIM(ROUTINENAME)=@objectName
 ORDER BY ORDINAL;
""";
    }

    private sealed class FirebirdMetadataQueryStrategy : ISqlMetadataQueryStrategy
    {
        public string BuildListObjectsQuery(string? filter)
            => $"""
SELECT src.SchemaName
     , src.ObjectName
     , src.ObjectType
  FROM (
SELECT '' AS SchemaName
     , TRIM(r.RDB$RELATION_NAME) AS ObjectName
     , 'Table' AS ObjectType
  FROM RDB$RELATIONS r
 WHERE COALESCE(r.RDB$SYSTEM_FLAG, 0) = 0
   AND r.RDB$VIEW_BLR IS NULL
   {AppendFilter(filter, "TRIM(r.RDB$RELATION_NAME)")}
UNION ALL
SELECT '' AS SchemaName
     , TRIM(r.RDB$RELATION_NAME) AS ObjectName
     , 'View' AS ObjectType
  FROM RDB$RELATIONS r
 WHERE COALESCE(r.RDB$SYSTEM_FLAG, 0) = 0
   AND r.RDB$VIEW_BLR IS NOT NULL
   {AppendFilter(filter, "TRIM(r.RDB$RELATION_NAME)")}
UNION ALL
SELECT '' AS SchemaName
     , TRIM(p.RDB$PROCEDURE_NAME) AS ObjectName
     , 'Procedure' AS ObjectType
  FROM RDB$PROCEDURES p
 WHERE 1=1
   {AppendFilter(filter, "TRIM(p.RDB$PROCEDURE_NAME)")}
UNION ALL
SELECT '' AS SchemaName
     , TRIM(f.RDB$FUNCTION_NAME) AS ObjectName
     , 'Function' AS ObjectType
  FROM RDB$FUNCTIONS f
 WHERE 1=1
       {AppendFilter(filter, "TRIM(f.RDB$FUNCTION_NAME)")}
  ) src
 ORDER BY src.SchemaName, src.ObjectType, src.ObjectName;
""";

        public string BuildObjectColumnsQuery()
            => """
SELECT TRIM(rf.RDB$FIELD_NAME) AS ColumnName
     , '' AS DataType
     , rf.RDB$FIELD_POSITION AS Ordinal
     , CASE WHEN COALESCE(rf.RDB$NULL_FLAG, 0) = 0 THEN 'YES' ELSE 'NO' END AS IsNullable
     , '' AS Extra
     , '' AS DefaultValue
     , CAST(NULL AS BIGINT) AS CharMaxLen
     , CAST(NULL AS INTEGER) AS NumPrecision
     , CAST(NULL AS INTEGER) AS NumScale
     , '' AS ColumnType
     , '' AS ColumnGenerated
  FROM RDB$RELATION_FIELDS rf
 WHERE TRIM(rf.RDB$RELATION_NAME) = @objectName
 ORDER BY rf.RDB$FIELD_POSITION;
""";

        public string BuildPrimaryKeyQuery()
            => """
SELECT TRIM(seg.RDB$FIELD_NAME) AS ColumnName
  FROM RDB$RELATION_CONSTRAINTS rc
  JOIN RDB$INDICES i ON i.RDB$INDEX_NAME = rc.RDB$INDEX_NAME
  JOIN RDB$INDEX_SEGMENTS seg ON seg.RDB$INDEX_NAME = i.RDB$INDEX_NAME
 WHERE COALESCE(i.RDB$SYSTEM_FLAG, 0) = 0
   AND rc.RDB$CONSTRAINT_TYPE = 'PRIMARY KEY'
   AND TRIM(rc.RDB$RELATION_NAME) = @objectName
 ORDER BY seg.RDB$FIELD_POSITION;
""";

        public string BuildIndexesQuery()
            => """
SELECT TRIM(i.RDB$INDEX_NAME) AS IndexName
     , CASE WHEN COALESCE(i.RDB$UNIQUE_FLAG, 0) = 1 THEN '1' ELSE '0' END AS IsUnique
     , TRIM(seg.RDB$FIELD_NAME) AS ColumnName
     , seg.RDB$FIELD_POSITION AS Seq
  FROM RDB$INDICES i
  JOIN RDB$INDEX_SEGMENTS seg ON seg.RDB$INDEX_NAME = i.RDB$INDEX_NAME
  LEFT JOIN RDB$RELATION_CONSTRAINTS rc ON rc.RDB$INDEX_NAME = i.RDB$INDEX_NAME
 WHERE COALESCE(i.RDB$SYSTEM_FLAG, 0) = 0
   AND rc.RDB$INDEX_NAME IS NULL
   AND TRIM(i.RDB$RELATION_NAME) = @objectName
 ORDER BY i.RDB$INDEX_NAME, seg.RDB$FIELD_POSITION;
""";

        public string BuildForeignKeysQuery()
            => "SELECT '' AS ColumnName, '' AS RefTable, '' AS RefColumn FROM RDB$DATABASE WHERE 1=0;";

        public string BuildTriggersQuery()
            => "SELECT '' AS TriggerName FROM RDB$DATABASE WHERE 1=0;";

        public string BuildSequenceMetadataQuery()
            => """
SELECT TRIM(g.RDB$GENERATOR_NAME) AS StartValue
     , '1' AS IncrementBy
     , '1' AS CurrentValue
  FROM RDB$GENERATORS g
 WHERE COALESCE(g.RDB$SYSTEM_FLAG, 0) = 0
   AND TRIM(g.RDB$GENERATOR_NAME) = @objectName;
""";

        public string BuildRoutineMetadataQuery()
            => """
SELECT '' AS SchemaName
     , TRIM(p.RDB$PROCEDURE_NAME) AS ObjectName
     , 'Procedure' AS RoutineType
     , '' AS BodySql
     , '' AS ReturnTypeSql
  FROM RDB$PROCEDURES p
 WHERE TRIM(p.RDB$PROCEDURE_NAME) = @objectName
UNION ALL
SELECT '' AS SchemaName
     , TRIM(f.RDB$FUNCTION_NAME) AS ObjectName
     , 'Function' AS RoutineType
     , '' AS BodySql
     , COALESCE(CAST(fr.RDB$FIELD_TYPE AS VARCHAR(20)), CAST(fa.RDB$FIELD_TYPE AS VARCHAR(20)), '') AS ReturnTypeSql
  FROM RDB$FUNCTIONS f
  LEFT JOIN RDB$FUNCTION_ARGUMENTS fa
    ON fa.RDB$FUNCTION_NAME = f.RDB$FUNCTION_NAME
   AND fa.RDB$ARGUMENT_POSITION = f.RDB$RETURN_ARGUMENT
  LEFT JOIN RDB$FIELDS fr
    ON fr.RDB$FIELD_NAME = fa.RDB$FIELD_SOURCE
 WHERE TRIM(f.RDB$FUNCTION_NAME) = @objectName;
""";

        public string BuildRoutineParametersQuery()
            => """
SELECT '' AS SchemaName
     , TRIM(pp.RDB$PROCEDURE_NAME) AS ObjectName
     , CASE
         WHEN COALESCE(pp.RDB$PARAMETER_TYPE, 0) = 1 THEN 'OUT'
         ELSE 'IN'
       END AS ParameterMode
     , COALESCE(TRIM(pp.RDB$PARAMETER_NAME), 'return') AS ParameterName
     , '' AS DataType
     , pp.RDB$PARAMETER_NUMBER AS Ordinal
     , '' AS DefaultValue
     , CASE WHEN COALESCE(pp.RDB$NULL_FLAG, 0) = 0 THEN 'YES' ELSE 'NO' END AS IsNullable
     , CAST(NULL AS INTEGER) AS CharMaxLen
     , CAST(NULL AS INTEGER) AS NumPrecision
     , CAST(NULL AS INTEGER) AS NumScale
     , '0' AS IsVariadic
     , '0' AS IsOrderByClause
     , '0' AS IsFrameClause
  FROM RDB$PROCEDURE_PARAMETERS pp
WHERE TRIM(pp.RDB$PROCEDURE_NAME) = @objectName
UNION ALL
SELECT '' AS SchemaName
     , TRIM(fa.RDB$FUNCTION_NAME) AS ObjectName
     , CASE
         WHEN COALESCE(TRIM(fa.RDB$ARGUMENT_NAME), '') = '' THEN 'RETURN'
         WHEN fa.RDB$ARGUMENT_POSITION = COALESCE(f.RDB$RETURN_ARGUMENT, 0) THEN 'RETURN'
         ELSE 'IN'
       END AS ParameterMode
     , COALESCE(TRIM(fa.RDB$ARGUMENT_NAME), 'return') AS ParameterName
     , COALESCE(CAST(fr.RDB$FIELD_TYPE AS VARCHAR(20)), CAST(fa.RDB$FIELD_TYPE AS VARCHAR(20)), '') AS DataType
     , fa.RDB$ARGUMENT_POSITION AS Ordinal
     , '' AS DefaultValue
     , CASE WHEN COALESCE(fa.RDB$NULL_FLAG, 0) = 0 THEN 'YES' ELSE 'NO' END AS IsNullable
     , CAST(COALESCE(fr.RDB$CHARACTER_LENGTH, fa.RDB$CHARACTER_LENGTH) AS INTEGER) AS CharMaxLen
     , CAST(COALESCE(fr.RDB$FIELD_PRECISION, fa.RDB$FIELD_PRECISION) AS INTEGER) AS NumPrecision
     , CAST(ABS(COALESCE(fr.RDB$FIELD_SCALE, fa.RDB$FIELD_SCALE)) AS INTEGER) AS NumScale
     , '0' AS IsVariadic
     , '0' AS IsOrderByClause
     , '0' AS IsFrameClause
  FROM RDB$FUNCTION_ARGUMENTS fa
  JOIN RDB$FUNCTIONS f
    ON f.RDB$FUNCTION_NAME = fa.RDB$FUNCTION_NAME
  LEFT JOIN RDB$FIELDS fr
    ON fr.RDB$FIELD_NAME = fa.RDB$FIELD_SOURCE
 WHERE TRIM(f.RDB$FUNCTION_NAME) = @objectName
""";
    }

}
