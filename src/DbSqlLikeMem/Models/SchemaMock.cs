using DbSqlLikeMem.Models;

namespace DbSqlLikeMem;

/// <summary>
/// EN: Base for an in-memory schema responsible for tables and procedures.
/// PT: Base de um schema em memória, responsável por tabelas e procedimentos.
/// </summary>
public abstract class SchemaMock
    : ISchemaMock
    , IEnumerable<KeyValuePair<string, ITableMock>>
{
    private const int ParallelFkScanThreshold = 2048;

    /// <summary>
    /// EN: Initializes the schema with name, database, and optional collections.
    /// PT: Inicializa o schema com nome, banco e coleções opcionais.
    /// </summary>
    /// <param name="schemaName">EN: Schema name. PT: Nome do schema.</param>
    /// <param name="db">EN: Parent database instance. PT: Instância do banco pai.</param>
    /// <param name="tables">EN: Initial table configuration. PT: Configuração inicial de tabelas.</param>
    /// <param name="procedures">EN: Initial procedures. PT: Procedimentos iniciais.</param>
    protected SchemaMock(
        string schemaName,
        DbMock db,
        IDictionary<string, (IEnumerable<Col> columns, IEnumerable<Dictionary<int, object?>>? rows)>? tables = null,
        IDictionary<string, ProcedureDef>? procedures = null/*,
        IDictionary<string, SqlSelectQuery>? views = null*/
    )
    {
        SchemaName = schemaName.NormalizeName();
        Db = db;
        if (tables != null)
            foreach (var it in tables)
                CreateTable(it.Key, it.Value.columns, it.Value.rows);
        if (procedures != null)
            foreach (var it in procedures)
                Procedures.Add(it.Key, it.Value);
        //if (views != null)
        //    foreach (var (viewName, config) in views)
        //        Views.AddTable(viewName, config);
    }

    /// <summary>
    /// EN: Normalized schema name.
    /// PT: Nome normalizado do schema.
    /// </summary>
    public string SchemaName { get; }

    /// <summary>
    /// EN: Database to which the schema belongs.
    /// PT: Banco ao qual o schema pertence.
    /// </summary>
    public DbMock Db { get; }

    /// <summary>
    /// EN: Internal table map with normalized names on access.
    /// PT: Mapa interno de tabelas, com nomes normalizados no acesso.
    /// </summary>
    private readonly TableDictionary tables = [];

    /// <summary>
    /// EN: Exposes schema tables.
    /// PT: Exposição das tabelas do schema.
    /// </summary>
    public ITableDictionary Tables => tables;

    /// <summary>
    /// EN: Stored procedure contracts (signature only).
    /// PT: Contratos de procedimentos armazenados (apenas assinatura).
    /// </summary>
    public IDictionary<string, ProcedureDef> Procedures { get; } =
        new Dictionary<string, ProcedureDef>(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// EN: Non-materialized views (definition only) evaluated on demand.
    /// PT: Views não materializadas (somente definição) avaliadas sob demanda.
    /// </summary>
    internal IDictionary<string, SqlSelectQuery> Views { get; } =
        new Dictionary<string, SqlSelectQuery>(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// EN: Creates a new table instance for this schema.
    /// PT: Cria uma nova instância de tabela para este schema.
    /// </summary>
    /// <param name="tableName">EN: Table name. PT: Nome da tabela.</param>
    /// <param name="columns">EN: Table columns. PT: Colunas da tabela.</param>
    /// <param name="rows">EN: Initial rows. PT: Linhas iniciais.</param>
    /// <returns>EN: New table instance. PT: Nova instância de tabela.</returns>
    protected abstract TableMock NewTable(
        string tableName,
        IEnumerable<Col> columns,
        IEnumerable<Dictionary<int, object?>>? rows = null);

    /// <summary>
    /// EN: Creates a table and registers it in the schema.
    /// PT: Cria uma tabela e a registra no schema.
    /// </summary>
    /// <param name="tableName">EN: Table name. PT: Nome da tabela.</param>
    /// <param name="columns">EN: Table columns. PT: Colunas da tabela.</param>
    /// <param name="rows">EN: Initial rows. PT: Linhas iniciais.</param>
    /// <returns>EN: Created table. PT: Tabela criada.</returns>
    public TableMock CreateTable(
        string tableName,
        IEnumerable<Col> columns,
        IEnumerable<Dictionary<int, object?>>? rows = null)
    {
        var t = NewTable(tableName, columns, rows);
        tables.Add(tableName, t);
        return t;
    }

    /// <summary>
    /// EN: Creates a table instance without registering it in the schema.
    /// PT: Cria uma instância de tabela sem registrar no schema.
    /// </summary>
    /// <param name="tableName">EN: Table name. PT: Nome da tabela.</param>
    /// <param name="columns">EN: Table columns. PT: Colunas da tabela.</param>
    /// <param name="rows">EN: Initial rows. PT: Linhas iniciais.</param>
    /// <returns>EN: New table instance. PT: Nova instância de tabela.</returns>
    internal TableMock CreateTableInstance(
        string tableName,
        IEnumerable<Col> columns,
        IEnumerable<Dictionary<int, object?>>? rows = null)
        => NewTable(tableName, columns, rows);

    #region Tables

    /// <summary>
    /// EN: Adds a table to the schema.
    /// PT: Adiciona uma tabela ao schema.
    /// </summary>
    /// <param name="key">EN: Table name. PT: Nome da tabela.</param>
    /// <param name="table">EN: Table to add. PT: Tabela a adicionar.</param>
    public void Add(string key, ITableMock table)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(key, nameof(key));
        ArgumentNullExceptionCompatible.ThrowIfNull(table, nameof(table));

        key = key.NormalizeName();

        if (!Db.ThreadSafe)
        {
            AddUnsafe(key, table);
            return;
        }

        lock (Db.SyncRoot)
        {
            AddUnsafe(key, table);
        }
    }

    private void AddUnsafe(string key, ITableMock table)
    {
        if (tables.ContainsKey(key))
            throw new InvalidOperationException($"Table '{key}' already exists.");

        tables.Add(key, table);
    }

    /// <summary>
    /// EN: Tries to get a table by name.
    /// PT: Tenta obter uma tabela pelo nome.
    /// </summary>
    /// <param name="key">EN: Table name. PT: Nome da tabela.</param>
    /// <param name="value">EN: Found table, if any. PT: Tabela encontrada, se houver.</param>
    /// <returns>EN: True if the table exists. PT: True se a tabela existir.</returns>
    public bool TryGetTable(string key, out ITableMock? value)
        => tables.TryGetValue(key.NormalizeName(), out value);

    /// <summary>
    /// EN: Gets or sets a table by name.
    /// PT: Obtém ou define uma tabela pelo nome.
    /// </summary>
    public ITableMock this[string key]
    {
        get => tables[key.NormalizeName()];
        set => tables[key.NormalizeName()] = value;
    }

    /// <summary>
    /// EN: Returns table names in the schema.
    /// PT: Retorna os nomes das tabelas do schema.
    /// </summary>
    public IEnumerable<string> Keys => tables.Keys.ToHashSet(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// EN: Returns tables in the schema.
    /// PT: Retorna as tabelas do schema.
    /// </summary>
    public IEnumerable<ITableMock> Values => tables.Values;

    /// <summary>
    /// EN: Returns an enumerator of schema tables.
    /// PT: Retorna enumerador das tabelas do schema.
    /// </summary>
    public IEnumerator<KeyValuePair<string, ITableMock>> GetEnumerator()
        => tables.GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator()
        => tables.GetEnumerator();

    #region Backup / Restore (best-effort)

    /// <summary>
    /// EN: Backs up all tables in the schema.
    /// PT: Faz backup de todas as tabelas do schema.
    /// </summary>
    public virtual void BackupAllTablesBestEffort()
    {
        foreach (var table in tables.Values)
            table.Backup();
    }

    /// <summary>
    /// EN: Restores backups of all tables in the schema.
    /// PT: Restaura backup de todas as tabelas do schema.
    /// </summary>
    public virtual void RestoreAllTablesBestEffort()
    {
        foreach (var table in tables.Values)
            table.Restore();
    }

    /// <summary>
    /// EN: Clears backups of all tables in the schema.
    /// PT: Limpa backup de todas as tabelas do schema.
    /// </summary>
    public virtual void ClearBackupAllTablesBestEffort()
    {
        foreach (var table in tables.Values)
            table.ClearBackup();
    }

    #endregion

    #endregion
    internal void ValidateForeignKeysOnDelete(
        string tableName,
        ITableMock table,
        IEnumerable<IReadOnlyDictionary<int, object?>> rowsToDelete)
    {
        foreach (var parentRow in rowsToDelete)
        {
            foreach (var childTable in tables.Values)
            {
                foreach (var fk in childTable.ForeignKeys.Values.Where(f =>
                    f.RefTable.TableName.Equals(tableName, StringComparison.OrdinalIgnoreCase)))
                {
                    if (HasReferenceByIndex(childTable, fk, parentRow)
                        || HasReferenceByScan(childTable, fk, parentRow))
                    {
                        throw table.ReferencedRow(tableName);
                    }
                }
            }
        }
    }

    private bool HasReferenceByScan(
        ITableMock childTable,
        ForeignDef fk,
        IReadOnlyDictionary<int, object?> parentRow)
    {
        if (Db.ThreadSafe && childTable.Count >= ParallelFkScanThreshold)
        {
            return childTable
                .AsParallel()
                .Any(childRow => fk.References.All(r =>
                    Equals(childRow[r.col.Index], parentRow[r.refCol.Index])));
        }

        return childTable.Any(childRow => fk.References.All(r =>
            Equals(childRow[r.col.Index], parentRow[r.refCol.Index])));
    }

    private static bool HasReferenceByIndex(
        ITableMock childTable,
        ForeignDef fk,
        IReadOnlyDictionary<int, object?> parentRow)
    {
        var matchingIndex = childTable.Indexes.Values
            .OrderByDescending(_ => _.KeyCols.Count)
            .FirstOrDefault(ix => fk.References.All(r =>
                ix.KeyCols.Contains(r.col.Name, StringComparer.OrdinalIgnoreCase)));

        if (matchingIndex is null)
            return false;

        var valuesByColumn = fk.References.ToDictionary(
            _ => _.col.Name.NormalizeName(),
            _ => parentRow[_.refCol.Index],
            StringComparer.OrdinalIgnoreCase);

        var key = matchingIndex.BuildIndexKeyFromValues(valuesByColumn);
        return matchingIndex.LookupMutable(key)?.Count > 0;
    }

}
