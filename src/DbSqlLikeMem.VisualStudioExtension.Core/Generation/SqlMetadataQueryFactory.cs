namespace DbSqlLikeMem.VisualStudioExtension.Core.Generation;

/// <summary>
/// Represents this public API type.
/// Representa este tipo público da API.
/// </summary>
public static class SqlMetadataQueryFactory
{
    private static readonly IReadOnlyDictionary<string, ISqlMetadataQueryStrategy> Strategies =
        new Dictionary<string, ISqlMetadataQueryStrategy>(StringComparer.Ordinal)
        {
            ["mysql"] = new MySqlMetadataQueryStrategy(),
            ["sqlserver"] = new SqlServerMetadataQueryStrategy(),
            ["postgresql"] = new PostgreSqlMetadataQueryStrategy(),
            ["oracle"] = new OracleMetadataQueryStrategy(),
            ["sqlite"] = new SqliteMetadataQueryStrategy(),
            ["db2"] = new Db2MetadataQueryStrategy(),
        };

    /// <summary>
    /// Builds the metadata query that lists schema objects for the specified database type.
    /// </summary>
    /// <param name="databaseType">The target database type.</param>
    /// <returns>The SQL query text.</returns>
    public static string BuildListObjectsQuery(string databaseType)
        => ResolveStrategy(databaseType).BuildListObjectsQuery();

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

    private static ISqlMetadataQueryStrategy ResolveStrategy(string databaseType)
    {
        var normalizedType = Normalize(databaseType);
        if (Strategies.TryGetValue(normalizedType, out var strategy))
        {
            return strategy;
        }

        throw new NotSupportedException($"Database type not supported for metadata queries: {databaseType}");
    }

    private interface ISqlMetadataQueryStrategy
    {
        string BuildListObjectsQuery();
        string BuildObjectColumnsQuery();
        string BuildPrimaryKeyQuery();
        string BuildIndexesQuery();
        string BuildForeignKeysQuery();
        string BuildTriggersQuery();
    }

    private sealed class MySqlMetadataQueryStrategy : ISqlMetadataQueryStrategy
    {
        public string BuildListObjectsQuery()
            => """
SELECT TABLE_SCHEMA AS SchemaName, TABLE_NAME AS ObjectName,
       CASE TABLE_TYPE WHEN 'BASE TABLE' THEN 'Table' WHEN 'VIEW' THEN 'View' ELSE TABLE_TYPE END AS ObjectType
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = @databaseName
UNION ALL
SELECT ROUTINE_SCHEMA AS SchemaName, ROUTINE_NAME AS ObjectName, 'Procedure' AS ObjectType
FROM INFORMATION_SCHEMA.ROUTINES
WHERE ROUTINE_SCHEMA = @databaseName
ORDER BY SchemaName, ObjectType, ObjectName;
""";

        public string BuildObjectColumnsQuery()
            => """
SELECT COLUMN_NAME AS ColumnName, DATA_TYPE AS DataType, ORDINAL_POSITION AS Ordinal,
       IS_NULLABLE AS IsNullable, EXTRA AS Extra,
       COLUMN_DEFAULT AS DefaultValue, CHARACTER_MAXIMUM_LENGTH AS CharMaxLen,
       NUMERIC_PRECISION AS NumPrecision, NUMERIC_SCALE AS NumScale, COLUMN_TYPE AS ColumnType,
       GENERATION_EXPRESSION AS Generated
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = @schemaName AND TABLE_NAME = @objectName
ORDER BY ORDINAL_POSITION;
""";

        public string BuildPrimaryKeyQuery()
            => "SELECT COLUMN_NAME AS ColumnName FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_SCHEMA=@schemaName AND TABLE_NAME=@objectName AND INDEX_NAME='PRIMARY' ORDER BY SEQ_IN_INDEX;";

        public string BuildIndexesQuery()
            => "SELECT INDEX_NAME AS IndexName, NON_UNIQUE AS NonUnique, COLUMN_NAME AS ColumnName, SEQ_IN_INDEX AS Seq FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_SCHEMA=@schemaName AND TABLE_NAME=@objectName ORDER BY INDEX_NAME, SEQ_IN_INDEX;";

        public string BuildForeignKeysQuery()
            => """
SELECT KCU.COLUMN_NAME AS ColumnName, KCU.REFERENCED_TABLE_NAME AS RefTable, KCU.REFERENCED_COLUMN_NAME AS RefColumn
FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE KCU
WHERE KCU.TABLE_SCHEMA=@schemaName AND KCU.TABLE_NAME=@objectName AND KCU.REFERENCED_TABLE_NAME IS NOT NULL
ORDER BY KCU.CONSTRAINT_NAME, KCU.ORDINAL_POSITION;
""";

        public string BuildTriggersQuery()
            => """
SELECT TRIGGER_NAME AS TriggerName
FROM INFORMATION_SCHEMA.TRIGGERS
WHERE TRIGGER_SCHEMA=@schemaName AND EVENT_OBJECT_TABLE=@objectName
ORDER BY TRIGGER_NAME;
""";
    }

    private sealed class SqlServerMetadataQueryStrategy : ISqlMetadataQueryStrategy
    {
        public string BuildListObjectsQuery()
            => """
SELECT s.name AS SchemaName, o.name AS ObjectName,
       CASE o.type WHEN 'U' THEN 'Table' WHEN 'V' THEN 'View' WHEN 'P' THEN 'Procedure' ELSE o.type END AS ObjectType
FROM sys.objects o
JOIN sys.schemas s ON s.schema_id = o.schema_id
WHERE o.type IN ('U', 'V', 'P')
ORDER BY s.name, ObjectType, o.name;
""";

        public string BuildObjectColumnsQuery()
            => """
SELECT c.name AS ColumnName, t.name AS DataType, c.column_id AS Ordinal,
       c.is_nullable AS IsNullable, c.is_identity AS IsIdentity,
       OBJECT_DEFINITION(c.default_object_id) AS DefaultValue,
       c.max_length AS CharMaxLen, c.precision AS NumPrecision, c.scale AS NumScale,
       '' AS ColumnType, '' AS Generated
FROM sys.columns c
JOIN sys.types t ON t.user_type_id = c.user_type_id
JOIN sys.objects o ON o.object_id = c.object_id
JOIN sys.schemas s ON s.schema_id = o.schema_id
WHERE s.name = @schemaName AND o.name = @objectName
ORDER BY c.column_id;
""";

        public string BuildPrimaryKeyQuery()
            => """
SELECT c.name AS ColumnName
FROM sys.indexes i
JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
JOIN sys.objects o ON o.object_id = i.object_id
JOIN sys.schemas s ON s.schema_id = o.schema_id
WHERE i.is_primary_key = 1 AND s.name=@schemaName AND o.name=@objectName
ORDER BY ic.key_ordinal;
""";

        public string BuildIndexesQuery()
            => """
SELECT i.name AS IndexName, i.is_unique AS IsUnique, c.name AS ColumnName, ic.key_ordinal AS Seq
FROM sys.indexes i
JOIN sys.index_columns ic ON ic.object_id=i.object_id AND ic.index_id=i.index_id
JOIN sys.columns c ON c.object_id=ic.object_id AND c.column_id=ic.column_id
JOIN sys.objects o ON o.object_id=i.object_id
JOIN sys.schemas s ON s.schema_id=o.schema_id
WHERE i.is_hypothetical=0 AND i.name IS NOT NULL AND s.name=@schemaName AND o.name=@objectName
ORDER BY i.name, ic.key_ordinal;
""";

        public string BuildForeignKeysQuery()
            => """
SELECT pc.name AS ColumnName, rt.name AS RefTable, rc.name AS RefColumn
FROM sys.foreign_key_columns fkc
JOIN sys.objects o ON o.object_id=fkc.parent_object_id
JOIN sys.schemas s ON s.schema_id=o.schema_id
JOIN sys.columns pc ON pc.object_id=fkc.parent_object_id AND pc.column_id=fkc.parent_column_id
JOIN sys.objects rt ON rt.object_id=fkc.referenced_object_id
JOIN sys.columns rc ON rc.object_id=fkc.referenced_object_id AND rc.column_id=fkc.referenced_column_id
WHERE s.name=@schemaName AND o.name=@objectName
ORDER BY fkc.constraint_column_id;
""";

        public string BuildTriggersQuery()
            => """
SELECT tr.name AS TriggerName
FROM sys.triggers tr
JOIN sys.objects o ON o.object_id=tr.parent_id
JOIN sys.schemas s ON s.schema_id=o.schema_id
WHERE s.name=@schemaName AND o.name=@objectName
ORDER BY tr.name;
""";
    }

    private sealed class PostgreSqlMetadataQueryStrategy : ISqlMetadataQueryStrategy
    {
        public string BuildListObjectsQuery()
            => """
SELECT n.nspname AS SchemaName, c.relname AS ObjectName,
       CASE c.relkind WHEN 'r' THEN 'Table' WHEN 'v' THEN 'View' ELSE c.relkind::text END AS ObjectType
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind IN ('r','v') AND n.nspname NOT IN ('pg_catalog', 'information_schema')
UNION ALL
SELECT routine_schema AS SchemaName, routine_name AS ObjectName, 'Procedure' AS ObjectType
FROM information_schema.routines
WHERE routine_schema NOT IN ('pg_catalog', 'information_schema')
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
       '' AS Generated
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
    }

    private sealed class OracleMetadataQueryStrategy : ISqlMetadataQueryStrategy
    {
        public string BuildListObjectsQuery()
            => """
SELECT OWNER AS SchemaName, OBJECT_NAME AS ObjectName,
       CASE OBJECT_TYPE WHEN 'TABLE' THEN 'Table' WHEN 'VIEW' THEN 'View' WHEN 'PROCEDURE' THEN 'Procedure' ELSE OBJECT_TYPE END AS ObjectType
FROM ALL_OBJECTS
WHERE OBJECT_TYPE IN ('TABLE', 'VIEW', 'PROCEDURE')
ORDER BY OWNER, ObjectType, OBJECT_NAME;
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
       '' AS Generated
FROM ALL_TAB_COLUMNS
WHERE OWNER = @schemaName AND TABLE_NAME = @objectName
ORDER BY COLUMN_ID;
""";

        public string BuildPrimaryKeyQuery()
            => """
SELECT cols.COLUMN_NAME AS ColumnName
FROM ALL_CONSTRAINTS cons
JOIN ALL_CONS_COLUMNS cols ON cols.OWNER = cons.OWNER AND cols.CONSTRAINT_NAME = cons.CONSTRAINT_NAME
WHERE cons.CONSTRAINT_TYPE='P' AND cons.OWNER=@schemaName AND cons.TABLE_NAME=@objectName
ORDER BY cols.POSITION;
""";

        public string BuildIndexesQuery()
            => """
SELECT i.INDEX_NAME AS IndexName, i.UNIQUENESS AS Uniqueness, c.COLUMN_NAME AS ColumnName, c.COLUMN_POSITION AS Seq
FROM ALL_INDEXES i
JOIN ALL_IND_COLUMNS c ON c.INDEX_OWNER=i.OWNER AND c.INDEX_NAME=i.INDEX_NAME
WHERE i.TABLE_OWNER=@schemaName AND i.TABLE_NAME=@objectName
ORDER BY i.INDEX_NAME, c.COLUMN_POSITION;
""";

        public string BuildForeignKeysQuery()
            => """
SELECT cc.COLUMN_NAME AS ColumnName, rcc.TABLE_NAME AS RefTable, rcc.COLUMN_NAME AS RefColumn
FROM ALL_CONSTRAINTS c
JOIN ALL_CONS_COLUMNS cc ON cc.OWNER=c.OWNER AND cc.CONSTRAINT_NAME=c.CONSTRAINT_NAME
JOIN ALL_CONSTRAINTS rc ON rc.OWNER=c.R_OWNER AND rc.CONSTRAINT_NAME=c.R_CONSTRAINT_NAME
JOIN ALL_CONS_COLUMNS rcc ON rcc.OWNER=rc.OWNER AND rcc.CONSTRAINT_NAME=rc.CONSTRAINT_NAME AND rcc.POSITION=cc.POSITION
WHERE c.CONSTRAINT_TYPE='R' AND c.OWNER=@schemaName AND c.TABLE_NAME=@objectName
ORDER BY cc.POSITION;
""";

        public string BuildTriggersQuery()
            => """
SELECT TRIGGER_NAME AS TriggerName
FROM ALL_TRIGGERS
WHERE OWNER=@schemaName AND TABLE_NAME=@objectName
ORDER BY TRIGGER_NAME;
""";
    }

    private sealed class SqliteMetadataQueryStrategy : ISqlMetadataQueryStrategy
    {
        public string BuildListObjectsQuery()
            => """
SELECT '' AS SchemaName, name AS ObjectName,
       CASE type WHEN 'table' THEN 'Table' WHEN 'view' THEN 'View' ELSE type END AS ObjectType
FROM sqlite_master
WHERE type IN ('table','view') AND name NOT LIKE 'sqlite_%'
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
       '' AS Generated
FROM pragma_table_info(@objectName)
ORDER BY cid;
""";

        public string BuildPrimaryKeyQuery()
            => "SELECT name AS ColumnName FROM pragma_table_info(@objectName) WHERE pk > 0 ORDER BY pk;";

        public string BuildIndexesQuery()
            => "-- sqlite indexes fetched through pragma index_list/index_info externally; return empty by default";

        public string BuildForeignKeysQuery()
            => "SELECT \"\" AS ColumnName, \"\" AS RefTable, \"\" AS RefColumn WHERE 1=0;";

        public string BuildTriggersQuery()
            => "SELECT \"\" AS TriggerName WHERE 1=0;";
    }

    private sealed class Db2MetadataQueryStrategy : ISqlMetadataQueryStrategy
    {
        public string BuildListObjectsQuery()
            => """
SELECT RTRIM(TABSCHEMA) AS SchemaName, RTRIM(TABNAME) AS ObjectName,
       CASE TYPE WHEN 'T' THEN 'Table' WHEN 'V' THEN 'View' ELSE TYPE END AS ObjectType
FROM SYSCAT.TABLES
WHERE TYPE IN ('T','V')
UNION ALL
SELECT RTRIM(ROUTINESCHEMA) AS SchemaName, RTRIM(ROUTINENAME) AS ObjectName, 'Procedure' AS ObjectType
FROM SYSCAT.ROUTINES
WHERE ROUTINETYPE = 'P'
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
       '' AS Generated
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
    }

    private static string Normalize(string databaseType)
        => databaseType.Replace("_", string.Empty)
            .Replace("-", string.Empty)
            .Trim()
            .ToLowerInvariant();
}
