using DbSqlLikeMem.Dialect;

namespace DbSqlLikeMem;

/// <summary>
/// EN: Base for an in-memory schema responsible for tables, procedures, and sequences.
/// PT-br: Base de um schema em memória, responsável por tabelas, procedimentos e sequences.
/// </summary>
public abstract class SchemaMock
    : ISchemaMock
    , IEnumerable<KeyValuePair<string, ITableMock>>
{
    private const int ParallelFkScanThreshold = 2048;

    /// <summary>
    /// EN: Initializes the schema with name, database, and optional collections.
    /// PT-br: Inicializa o schema com nome, banco e coleções opcionais.
    /// </summary>
    /// <param name="schemaName">EN: Schema name. PT-br: Nome do schema.</param>
    /// <param name="db">EN: Parent database instance. PT-br: Instância do banco pai.</param>
    /// <param name="tables">EN: Initial table configuration. PT-br: Configuração inicial de tabelas.</param>
    /// <param name="functions">EN: Initial functions. PT-br: Funções iniciais.</param>
    /// <param name="procedures">EN: Initial procedures. PT-br: Procedimentos iniciais.</param>
    /// <param name="sequences">EN: Initial sequences. PT-br: Sequences iniciais.</param>
    /// <param name="views">EN: Initial sequences. PT-br: Sequences iniciais.</param>
    protected SchemaMock(
        string schemaName,
        DbMock db,
        IDictionary<string, (IEnumerable<Col> columns, IEnumerable<Dictionary<int, object?>>? rows)>? tables = null,
        IEnumerable<DbFunctionDef>? functions = null,
        IEnumerable<ProcedureDef>? procedures = null,
        IDictionary<string, SequenceDef>? sequences = null,
        IDictionary<string, string>? views = null
    )
    {
        SchemaName = schemaName.NormalizeName();
        Db = db;
        if (tables != null)
            foreach (var it in tables)
                CreateTable(it.Key, it.Value.columns, it.Value.rows);
        if (sequences != null)
            foreach (var it in sequences)
                this.sequences.Add(it.Key, it.Value);
        if (functions != null)
            foreach (var it in functions)
                Functions.Add(it.Name, it);
        if (procedures != null)
            foreach (var it in procedures)
                Procedures.Add(it.Name.NormalizeName(), it);
        if (views != null)
            foreach (var it in views)
                Views.Add(it.Key, ((SqlCreateViewQuery)SqlQueryParser.Parse(it.Value, db, db.Dialect)).Select);
    }

    /// <summary>
    /// EN: Normalized schema name.
    /// PT-br: Nome normalizado do schema.
    /// </summary>
    public string SchemaName { get; }

    /// <summary>
    /// EN: Database to which the schema belongs.
    /// PT-br: Banco ao qual o schema pertence.
    /// </summary>
    public DbMock Db { get; }

    /// <summary>
    /// EN: Internal table map with normalized names on access.
    /// PT-br: Mapa interno de tabelas, com nomes normalizados no acesso.
    /// </summary>
    private readonly TableDictionary tables = [];

    /// <summary>
    /// EN: Exposes schema tables.
    /// PT-br: Exposição das tabelas do schema.
    /// </summary>
    public ITableDictionary Tables => tables;

    #region Procedures

    /// <summary>
    /// EN: Stored procedure contracts (signature only).
    /// PT-br: Contratos de procedimentos armazenados (apenas assinatura).
    /// </summary>
    public IDictionary<string, ProcedureDef> Procedures { get; } =
        new Dictionary<string, ProcedureDef>(StringComparer.OrdinalIgnoreCase);

    internal bool TryGetProcedure(
        string procedureName,
        out ProcedureDef? procedure)
    {
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(procedureName, nameof(procedureName));
        return Procedures.TryGetValue(procedureName.NormalizeName(), out procedure)
            && procedure != null;
    }

    internal void CreateProcedure(
        string procedureName,
        ProcedureDef procedure,
        bool orReplace = false)
    {
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(procedureName, nameof(procedureName));
        ArgumentNullExceptionCompatible.ThrowIfNull(procedure, nameof(procedure));

        var normalized = procedureName.NormalizeName();
        if (Procedures.ContainsKey(normalized) && !orReplace)
            throw new InvalidOperationException($"Procedure '{normalized}' already exists.");

        Procedures[normalized] = procedure;
    }

    internal void RestoreProcedure(
        string procedureName,
        ProcedureDef procedure)
    {
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(procedureName, nameof(procedureName));
        ArgumentNullExceptionCompatible.ThrowIfNull(procedure, nameof(procedure));

        Procedures[procedureName.NormalizeName()] = procedure;
    }

    internal void RemoveProcedure(
        string procedureName)
    {
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(procedureName, nameof(procedureName));
        Procedures.Remove(procedureName.NormalizeName());
    }

    #endregion

    #region Functions

    internal FunctionDictionaryProcess Functions { get; } = [];

    internal bool TryGetFunction(
        string functionName,
        out DbFunctionDef? function)
    {
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(functionName, nameof(functionName));

        return Functions.TryGetValue(functionName.NormalizeName(), out function)
            && function != null;
    }

    internal void CreateFunction(
        DbFunctionDef function,
        bool orReplace = false)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(function, nameof(function));
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(function.Name, nameof(function.Name));

        var normalized = function.Name.NormalizeName();
        if (Functions.ContainsKey(normalized))
        {
            if (!orReplace)
                throw new InvalidOperationException($"Function '{function.Name}' already exists.");

            Functions[normalized] = function;
            return;
        }

        Functions.Add(normalized, function);
    }

    internal void DropFunction(
        string functionName,
        bool ifExists)
    {
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(functionName, nameof(functionName));

        if (Functions.Remove(functionName))
            return;

        if (ifExists)
            return;

        throw new InvalidOperationException($"Function '{functionName}' does not exist.");
    }

    internal void RestoreFunction(
        string functionName,
        DbFunctionDef definition)
    {
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(functionName, nameof(functionName));
        ArgumentNullExceptionCompatible.ThrowIfNull(definition, nameof(definition));

        Functions[functionName.NormalizeName()] = definition;
    }

    internal void RemoveFunction(
        string functionName)
    {
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(functionName, nameof(functionName));
        Functions.Remove(functionName);
    }

    #endregion

    /// <summary>
    /// EN: Sequence definitions registered in the schema.
    /// PT-br: Definições de sequence registradas no schema.
    /// </summary>
    public IReadOnlyDictionary<string, SequenceDef> Sequences => sequences;

    private readonly Dictionary<string, SequenceDef> sequences =
        new(StringComparer.OrdinalIgnoreCase);

    internal IDictionary<string, SequenceDef> MutableSequences => sequences;

    /// <summary>
    /// EN: Non-materialized views (definition only) evaluated on demand.
    /// PT-br: Views não materializadas (somente definição) avaliadas sob demanda.
    /// </summary>
    internal IDictionary<string, SqlSelectQuery> Views { get; } =
        new Dictionary<string, SqlSelectQuery>(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// EN: Creates a new table instance for this schema.
    /// PT-br: Cria uma nova instância de tabela para este schema.
    /// </summary>
    /// <param name="tableName">EN: Table name. PT-br: Nome da tabela.</param>
    /// <param name="columns">EN: Table columns. PT-br: Colunas da tabela.</param>
    /// <param name="rows">EN: Initial rows. PT-br: Linhas iniciais.</param>
    /// <returns>EN: New table instance. PT-br: Nova instância de tabela.</returns>
    protected abstract TableMock NewTable(
        string tableName,
        IEnumerable<Col> columns,
        IEnumerable<Dictionary<int, object?>>? rows = null);

    /// <summary>
    /// EN: Creates a table and registers it in the schema.
    /// PT-br: Cria uma tabela e a registra no schema.
    /// </summary>
    /// <param name="tableName">EN: Table name. PT-br: Nome da tabela.</param>
    /// <param name="columns">EN: Table columns. PT-br: Colunas da tabela.</param>
    /// <param name="rows">EN: Initial rows. PT-br: Linhas iniciais.</param>
    /// <returns>EN: Created table. PT-br: Tabela criada.</returns>
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
    /// PT-br: Cria uma instância de tabela sem registrar no schema.
    /// </summary>
    /// <param name="tableName">EN: Table name. PT-br: Nome da tabela.</param>
    /// <param name="columns">EN: Table columns. PT-br: Colunas da tabela.</param>
    /// <param name="rows">EN: Initial rows. PT-br: Linhas iniciais.</param>
    /// <returns>EN: New table instance. PT-br: Nova instância de tabela.</returns>
    internal TableMock CreateTableInstance(
        string tableName,
        IEnumerable<Col> columns,
        IEnumerable<Dictionary<int, object?>>? rows = null)
        => NewTable(tableName, columns, rows);

    #region Tables

    /// <summary>
    /// EN: Adds a table to the schema.
    /// PT-br: Adiciona uma tabela ao schema.
    /// </summary>
    /// <param name="key">EN: Table name. PT-br: Nome da tabela.</param>
    /// <param name="table">EN: Table to add. PT-br: Tabela a adicionar.</param>
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
            throw new InvalidOperationException(SqlExceptionMessages.TableAlreadyExists(key));

        tables.Add(key, table);
    }

    /// <summary>
    /// EN: Tries to get a table by name.
    /// PT-br: Tenta obter uma tabela pelo nome.
    /// </summary>
    /// <param name="key">EN: Table name. PT-br: Nome da tabela.</param>
    /// <param name="value">EN: Found table, if any. PT-br: Tabela encontrada, se houver.</param>
    /// <returns>EN: True if the table exists. PT-br: True se a tabela existir.</returns>
    public bool TryGetTable(string key, out ITableMock? value)
        => tables.TryGetValue(key.NormalizeName(), out value);

    /// <summary>
    /// EN: Gets or sets a table by name.
    /// PT-br: Obtém ou define uma tabela pelo nome.
    /// </summary>
    public ITableMock this[string key]
    {
        get => tables[key.NormalizeName()];
        set => tables[key.NormalizeName()] = value;
    }

    /// <summary>
    /// EN: Returns table names in the schema.
    /// PT-br: Retorna os nomes das tabelas do schema.
    /// </summary>
    public IEnumerable<string> Keys => tables.Keys.ToHashSet(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// EN: Returns tables in the schema.
    /// PT-br: Retorna as tabelas do schema.
    /// </summary>
    public IEnumerable<ITableMock> Values => tables.Values;

    /// <summary>
    /// EN: Returns an enumerator of schema tables.
    /// PT-br: Retorna enumerador das tabelas do schema.
    /// </summary>
    public IEnumerator<KeyValuePair<string, ITableMock>> GetEnumerator()
        => tables.GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator()
        => tables.GetEnumerator();

    #region Backup / Restore (best-effort)

    /// <summary>
    /// EN: Backs up all tables in the schema.
    /// PT-br: Faz backup de todas as tabelas do schema.
    /// </summary>
    public virtual void BackupAllTablesBestEffort()
    {
        var tableList = tables.Values.ToArray();
        if (Db.ThreadSafe && tableList.Length > 1)
        {
            Parallel.ForEach(tableList, table => table.Backup());
            return;
        }

        foreach (var table in tableList)
            table.Backup();
    }

    /// <summary>
    /// EN: Restores backups of all tables in the schema.
    /// PT-br: Restaura backup de todas as tabelas do schema.
    /// </summary>
    public virtual void RestoreAllTablesBestEffort()
    {
        var tableList = tables.Values.ToArray();
        if (Db.ThreadSafe && tableList.Length > 1)
        {
            Parallel.ForEach(tableList, table => table.Restore());
            return;
        }

        foreach (var table in tableList)
            table.Restore();
    }

    /// <summary>
    /// EN: Clears backups of all tables in the schema.
    /// PT-br: Limpa backup de todas as tabelas do schema.
    /// </summary>
    public virtual void ClearBackupAllTablesBestEffort()
    {
        var tableList = tables.Values.ToArray();
        if (Db.ThreadSafe && tableList.Length > 1)
        {
            Parallel.ForEach(tableList, table => table.ClearBackup());
            return;
        }

        foreach (var table in tableList)
            table.ClearBackup();
    }

    #endregion

    #endregion
    internal void ValidateForeignKeysOnDelete(
        string tableName,
        ITableMock table,
        IEnumerable<IReadOnlyDictionary<int, object?>> rowsToDelete)
    {
        var parentRows = rowsToDelete as IReadOnlyList<IReadOnlyDictionary<int, object?>>
            ?? [.. rowsToDelete];

        if (!Db.ThreadSafe || parentRows.Count <= 1)
        {
            foreach (var parentRow in parentRows)
                ValidateForeignKeysOnDeleteForRow(tableName, table, parentRow);
            return;
        }

        Exception? failure = null;
        var gate = new object();
        Parallel.ForEach(parentRows, (parentRow, state) =>
        {
            try
            {
                ValidateForeignKeysOnDeleteForRow(tableName, table, parentRow);
            }
            catch (Exception ex)
            {
                lock (gate)
                {
                    failure ??= ex;
                }

                state.Stop();
            }
        });

        if (failure is not null)
            throw failure;
    }

    private void ValidateForeignKeysOnDeleteForRow(
        string tableName,
        ITableMock table,
        IReadOnlyDictionary<int, object?> parentRow)
    {
        foreach (var childTable in tables.Values)
        {
            if (childTable.ForeignKeys.Count == 0)
                continue;

            foreach (var fk in childTable.ForeignKeys.Values)
            {
                if (!fk.RefTable.TableName.Equals(tableName, StringComparison.OrdinalIgnoreCase))
                    continue;

                if (HasReferenceByIndex(childTable, fk, parentRow)
                    || HasReferenceByScan(childTable, fk, parentRow))
                {
                    throw table.ReferencedRow(tableName);
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

        foreach (var childRow in childTable)
        {
            var matches = true;
            foreach (var reference in fk.References)
            {
                if (!Equals(childRow[reference.col.Index], parentRow[reference.refCol.Index]))
                {
                    matches = false;
                    break;
                }
            }

            if (matches)
                return true;
        }

        return false;
    }

    private static bool HasReferenceByIndex(
        ITableMock childTable,
        ForeignDef fk,
        IReadOnlyDictionary<int, object?> parentRow)
    {
        if (!fk.TryGetChildLookupPlan(out var lookupPlan))
            return false;

        var key = lookupPlan.BuildKey(parentRow);
        return lookupPlan.Index.LookupMutable(key)?.Count > 0;
    }

}
