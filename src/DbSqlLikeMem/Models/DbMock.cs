namespace DbSqlLikeMem;

/// <summary>
/// EN: Base of an in-memory database with schemas, tables, procedures, and sequences.
/// PT: Base de um banco em memória com schemas, tabelas, procedimentos e sequences.
/// </summary>
public abstract class DbMock
    : Dictionary<string, SchemaMock>
    , ISchemaDictionary
{
    /// <summary>
    /// EN: Simulated database version.
    /// PT: Versão do banco simulada.
    /// </summary>
    public int Version { get; }

    /// <summary>
    /// EN: Indicates whether operations should lock for thread safety.
    /// PT: Indica se operações devem aplicar bloqueio para segurança de threads.
    /// </summary>
    public bool ThreadSafe { get; set; }

    internal object SyncRoot { get; } = new();

    internal abstract SqlDialectBase Dialect { get; set; }

    private readonly Dictionary<string, ITableMock> _globalTemporaryTables =
        new(StringComparer.OrdinalIgnoreCase);

    IEnumerable<string> IReadOnlyDictionary<string, ISchemaMock>.Keys => Keys;

    IEnumerable<ISchemaMock> IReadOnlyDictionary<string, ISchemaMock>.Values => Values;

    ISchemaMock IReadOnlyDictionary<string, ISchemaMock>.this[string key]
    {
        get
        {
            if (base.TryGetValue(key, out var schema) && schema != null)
                return schema;
            throw new KeyNotFoundException($"Schema not found: {key}");
        }
    }

    /// <summary>
    /// EN: Initializes the database with the given version and a default schema.
    /// PT: Inicializa o banco com a versão informada e um schema padrão.
    /// </summary>
    /// <param name="version">EN: Simulated database version. PT: Versão simulada do banco.</param>
    protected DbMock(
        int version)
        : base(StringComparer.OrdinalIgnoreCase)
    {
        Version = version;
        CreateSchema("DefaultSchema");
    }

    #region Schema

    /// <summary>
    /// EN: Creates a new schema instance for the database.
    /// PT: Cria uma nova instância de schema para o banco.
    /// </summary>
    /// <param name="schemaName">EN: Schema name. PT: Nome do schema.</param>
    /// <param name="tables">EN: Initial tables. PT: Tabelas iniciais.</param>
    /// <returns>EN: New schema instance. PT: Nova instância de schema.</returns>
    protected abstract SchemaMock NewSchema(
        string schemaName,
        IDictionary<string, (IEnumerable<Col> columns, IEnumerable<Dictionary<int, object?>>? rows)>? tables = null);

    /// <summary>
    /// EN: Creates a schema and registers it in the database.
    /// PT: Cria um schema e o registra no banco.
    /// </summary>
    /// <param name="schemaName">EN: Schema name. PT: Nome do schema.</param>
    /// <param name="tables">EN: Initial schema tables. PT: Tabelas iniciais do schema.</param>
    /// <returns>EN: Created schema. PT: Schema criado.</returns>
    public ISchemaMock CreateSchema(
        string schemaName,
        IDictionary<string, (IEnumerable<Col> columns, IEnumerable<Dictionary<int, object?>>? rows)>? tables = null)
    {
        var s = NewSchema(schemaName, tables);
        Add(schemaName, s);
        return s;
    }

    internal string GetSchemaName(string? schemaName)
    {
        if (schemaName == null)
        {
            if (Count > 1)
                throw new InvalidOperationException(Resources.SqlExceptionMessages.MultipleSchemasRequireExplicitName(string.Join(",", this.Keys)));
            if (Count == 0)
                throw new InvalidOperationException(Resources.SqlExceptionMessages.NoSchemaRegistered());
            schemaName = this.Keys.First();
        }
        return schemaName.NormalizeName();
    }

    /// <summary>
    /// EN: Tries to get a schema by name.
    /// PT: Tenta obter um schema pelo nome.
    /// </summary>
    /// <param name="key">EN: Schema name. PT: Nome do schema.</param>
    /// <param name="value">EN: Found schema, if any. PT: Schema encontrado, se houver.</param>
    /// <returns>EN: True if the schema exists. PT: True se o schema existir.</returns>
    public bool TryGetValue(
        string key,
        out ISchemaMock value
    )
    {
        if (base.TryGetValue(key, out var v) && v != null)
        {
            value = v;
            return true;
        }
        value = null!;
        return false;
    }

    #endregion

    #region Table

    private string BuildTemporaryTableKey(
        string tableName,
        string? schemaName)
    {
        var sc = GetSchemaName(schemaName);
        return $"{sc}:{tableName.NormalizeName()}";
    }

    internal ITableMock AddGlobalTemporaryTable(
        string tableName,
        IEnumerable<Col>? columns = null,
        IEnumerable<Dictionary<int, object?>>? rows = null,
        string? schemaName = null)
    {
        var schemaKey = GetSchemaName(schemaName);
        var key = BuildTemporaryTableKey(tableName, schemaKey);
        if (!TryGetValue(schemaKey, out var schemaMock) || schemaMock == null)
            schemaMock = CreateSchema(schemaKey);
        var schema = (SchemaMock)schemaMock;
        var table = schema.CreateTableInstance(tableName, columns ?? [], rows);
        _globalTemporaryTables.Add(key, table);
        return table;
    }

    internal bool TryGetGlobalTemporaryTable(
        string tableName,
        out ITableMock? tb,
        string? schemaName = null)
    {
        var normalizedTableName = tableName.NormalizeName();

        if (!string.IsNullOrWhiteSpace(schemaName))
            return _globalTemporaryTables.TryGetValue(
                BuildTemporaryTableKey(normalizedTableName, schemaName),
                out tb);

        var matches = _globalTemporaryTables
            .Where(entry => entry.Key.EndsWith($":{normalizedTableName}", StringComparison.OrdinalIgnoreCase))
            .Select(entry => entry.Value)
            .Take(2)
            .ToList();

        if (matches.Count == 1)
        {
            tb = matches[0];
            return true;
        }

        tb = null;
        return false;
    }

    internal IReadOnlyList<ITableMock> ListGlobalTemporaryTables(
        string? schemaName = null)
    {
        var schema = GetSchemaName(schemaName);
        var prefix = $"{schema}:";
        return _globalTemporaryTables
            .Where(entry => entry.Key.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
            .Select(entry => entry.Value)
            .ToList()
            .AsReadOnly();
    }

    internal void ClearGlobalTemporaryTables()
        => _globalTemporaryTables.Clear();

    internal void DropGlobalTemporaryTable(
        string tableName,
        bool ifExists,
        string? schemaName = null)
    {
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(tableName, nameof(tableName));
        var key = BuildTemporaryTableKey(tableName.NormalizeName(), schemaName);
        if (_globalTemporaryTables.Remove(key))
            return;

        if (ifExists)
            return;

        throw SqlUnsupported.ForNormalizedTableDoesNotExist(tableName);
    }

    /// <summary>
    /// EN: Creates and adds a table to the specified schema.
    /// PT: Cria e adiciona uma tabela ao schema indicado.
    /// </summary>
    /// <param name="tableName">EN: Table name. PT: Nome da tabela.</param>
    /// <param name="columns">EN: Column definitions. PT: Definição das colunas.</param>
    /// <param name="rows">EN: Initial rows. PT: Linhas iniciais.</param>
    /// <param name="schemaName">EN: Target schema. PT: Schema alvo.</param>
    /// <returns>EN: Created table. PT: Tabela criada.</returns>
    public ITableMock AddTable(
        string tableName,
        IEnumerable<Col>? columns = null,
        IEnumerable<Dictionary<int, object?>>? rows = null,
        string? schemaName = null)
    {
        var sc = GetSchemaName(schemaName);
        if (!this.TryGetValue(sc, out var s))
            s = CreateSchema(sc);
        return s!.CreateTable(tableName, columns ?? [], rows);
    }

    /// <summary>
    /// EN: Gets a table by name, throwing if it does not exist.
    /// PT: Obtém uma tabela pelo nome, lançando erro se não existir.
    /// </summary>
    /// <param name="tableName">EN: Table name. PT: Nome da tabela.</param>
    /// <param name="schemaName">EN: Target schema. PT: Schema alvo.</param>
    /// <returns>EN: Found table. PT: Tabela encontrada.</returns>
    public ITableMock GetTable(
        string tableName,
        string? schemaName = null)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(tableName, nameof(tableName));
        var sc = GetSchemaName(schemaName);
        if (!this[sc].TryGetTable(tableName, out var tb)
            || tb == null)
            throw SqlUnsupported.ForNormalizedTableDoesNotExist(tableName);
        return tb;
    }

    /// <summary>
    /// EN: Tries to get a table by name.
    /// PT: Tenta obter uma tabela pelo nome.
    /// </summary>
    /// <param name="tableName">EN: Table name. PT: Nome da tabela.</param>
    /// <param name="tb">EN: Found table, if any. PT: Tabela encontrada, se houver.</param>
    /// <param name="schemaName">EN: Target schema. PT: Schema alvo.</param>
    /// <returns>EN: True if the table exists. PT: True se a tabela existir.</returns>
    public bool TryGetTable(
        string tableName,
        out ITableMock? tb,
        string? schemaName = null)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(tableName, nameof(tableName));
        var sc = GetSchemaName(schemaName);
        return this[sc].TryGetTable(tableName, out tb)
            && tb != null;
    }

    /// <summary>
    /// EN: Checks whether a table exists in the specified schema.
    /// PT: Verifica se uma tabela existe no schema informado.
    /// </summary>
    /// <param name="tableName">EN: Table name. PT: Nome da tabela.</param>
    /// <param name="schemaName">EN: Target schema. PT: Schema alvo.</param>
    /// <returns>EN: True if it exists. PT: True se existir.</returns>
    public bool ContainsTable(
        string tableName,
        string? schemaName = null)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(tableName, nameof(tableName));
        var sc = GetSchemaName(schemaName);
        return this[sc].TryGetTable(tableName, out var tb)
            && tb != null;
    }

    internal IReadOnlyList<ITableMock> ListTables(
        string? schemaName = null)
    {
        var sc = GetSchemaName(schemaName);
        return this[sc].Tables.Select(_ => _.Value).ToList().AsReadOnly();
    }

    internal void DropTable(
        string tableName,
        bool ifExists,
        string? schemaName = null)
    {
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(tableName, nameof(tableName));
        var sc = GetSchemaName(schemaName);
        var normalized = tableName.NormalizeName();
        var removed = this[sc].Tables.Remove(normalized);
        if (removed || ifExists)
            return;

        throw SqlUnsupported.ForNormalizedTableDoesNotExist(normalized);
    }

    internal void CreateIndex(
        string indexName,
        string tableName,
        IEnumerable<string> keyColumns,
        bool unique,
        string? schemaName = null)
    {
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(indexName, nameof(indexName));
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(tableName, nameof(tableName));
        ArgumentNullExceptionCompatible.ThrowIfNull(keyColumns, nameof(keyColumns));

        var table = GetTable(tableName, schemaName);
        table.CreateIndex(indexName, keyColumns, unique: unique);
    }

    internal void DropIndex(
        string indexName,
        bool ifExists,
        string? tableName = null,
        string? schemaName = null)
    {
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(indexName, nameof(indexName));

        if (!string.IsNullOrWhiteSpace(tableName))
        {
            if (GetTable(tableName!, schemaName) is not TableMock namedTable)
                throw new InvalidOperationException($"Table '{tableName!.NormalizeName()}' does not support index mutation.");

            namedTable.DropIndex(indexName, ifExists);
            return;
        }

        var normalizedIndexName = indexName.NormalizeName();
        var sc = GetSchemaName(schemaName);
        var matchingTables = this[sc].Tables.Values
            .OfType<TableMock>()
            .Where(table => table.Indexes.ContainsKey(normalizedIndexName))
            .ToList();

        if (matchingTables.Count == 1)
        {
            matchingTables[0].DropIndex(indexName, ifExists);
            return;
        }

        if (matchingTables.Count > 1)
            throw new InvalidOperationException($"DROP INDEX '{normalizedIndexName}' is ambiguous without table name.");

        if (ifExists)
            return;

        throw new InvalidOperationException($"Index '{normalizedIndexName}' does not exist.");
    }

    internal void AlterTableAddColumn(
        string tableName,
        string columnName,
        DbType dbType,
        bool nullable,
        int? size = null,
        int? decimalPlaces = null,
        object? defaultValue = null,
        string? schemaName = null)
    {
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(tableName, nameof(tableName));
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(columnName, nameof(columnName));

        var table = GetTable(tableName, schemaName);
        table.AddColumn(
            columnName,
            dbType,
            nullable,
            size,
            decimalPlaces,
            identity: false,
            defaultValue: defaultValue);
    }

    /// <summary>
    /// EN: Resets volatile in-memory data for all tables and optionally global temporary tables.
    /// PT: Reseta dados voláteis em memória de todas as tabelas e, opcionalmente, das tabelas temporárias globais.
    /// </summary>
    /// <param name="includeGlobalTemporaryTables">
    /// EN: Includes global temporary tables in the reset.
    /// PT: Inclui tabelas temporárias globais no reset.
    /// </param>
    public void ResetVolatileData(bool includeGlobalTemporaryTables = true)
    {
        if (!ThreadSafe)
        {
            ResetVolatileDataCore(includeGlobalTemporaryTables);
            return;
        }

        lock (SyncRoot)
            ResetVolatileDataCore(includeGlobalTemporaryTables);
    }

    #endregion

    #region View

    internal void AddView(
        SqlCreateViewQuery query,
        string? schemaName = null)
    {
        var name = query.Table?.Name?.NormalizeName();
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(name, nameof(name));

        var sc = GetSchemaName(schemaName);
        var schema = this[sc];

        if (schema.Views.ContainsKey(name!))
        {
            if (query.OrReplace)
            {
                schema.Views[name!] = query.Select;
                return;
            }

            if (query.IfNotExists)
            {
                return; // não cria, não dá erro
            }

            throw new InvalidOperationException(Resources.SqlExceptionMessages.ViewAlreadyExists(name!));
        }

        schema.Views[name!] = query.Select;
    }


    internal SqlSelectQuery GetView(
        string viewName,
        string? schemaName = null)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(viewName, nameof(viewName));
        var sc = GetSchemaName(schemaName);
        if (!this[sc].Views.TryGetValue(viewName, out var vw)
            || vw == null)
            throw new InvalidOperationException(Resources.SqlExceptionMessages.ViewDoesNotExist(viewName));
        return vw;
    }

    internal bool TryGetView(
        string viewName,
        out SqlSelectQuery? vw,
        string? schemaName = null)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(viewName, nameof(viewName));
        var sc = GetSchemaName(schemaName);
        return this[sc].Views.TryGetValue(viewName, out vw)
            && vw != null;
    }

    internal void DropView(
        string viewName,
        bool ifExists,
        string? schemaName = null)
    {
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(viewName, nameof(viewName));

        var sc = GetSchemaName(schemaName);
        var schema = this[sc];
        var normalized = viewName.NormalizeName();

        if (schema.Views.Remove(normalized))
            return;

        if (ifExists)
            return;

        throw new InvalidOperationException(Resources.SqlExceptionMessages.ViewDoesNotExist(normalized));
    }

    #endregion

    #region Procedures

    /// <summary>
    /// EN: Registers a stored procedure in the specified schema.
    /// PT: Registra um procedimento armazenado no schema informado.
    /// </summary>
    /// <param name="procName">EN: Procedure name. PT: Nome do procedimento.</param>
    /// <param name="pr">EN: Procedure definition. PT: Definição do procedimento.</param>
    /// <param name="schemaName">EN: Target schema. PT: Schema alvo.</param>
    public void AddProdecure(
        string procName,
        ProcedureDef pr,
        string? schemaName = null)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(procName, nameof(procName));
        var sc = GetSchemaName(schemaName);
        if (!this.TryGetValue(sc, out var s) || s == null)
            CreateSchema(sc);
        this[sc].Procedures[procName] = pr;
    }

    /// <summary>
    /// EN: Tries to get a stored procedure by name.
    /// PT: Tenta obter um procedimento armazenado pelo nome.
    /// </summary>
    /// <param name="procName">EN: Procedure name. PT: Nome do procedimento.</param>
    /// <param name="pr">EN: Found procedure, if any. PT: Procedimento encontrado, se houver.</param>
    /// <param name="schemaName">EN: Target schema. PT: Schema alvo.</param>
    /// <returns>EN: True if it exists. PT: True se existir.</returns>
    public bool TryGetProcedure(
        string procName,
        out ProcedureDef? pr,
        string? schemaName = null)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(procName, nameof(procName));
        var sc = GetSchemaName(schemaName);
        return this[sc].Procedures.TryGetValue(procName, out pr)
            && pr != null;
    }

    #endregion

    #region Functions

    internal bool TryGetFunction(
        string functionName,
        out ScalarFunctionDef? function,
        string? schemaName = null)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(functionName, nameof(functionName));
        var sc = GetSchemaName(schemaName);
        return this[sc].Functions.TryGetValue(functionName.NormalizeName(), out function)
            && function != null;
    }

    internal void CreateFunction(
        string functionName,
        string returnTypeSql,
        IReadOnlyList<ScalarFunctionParameterDef> parameters,
        SqlExpr body,
        bool orReplace = false,
        string? schemaName = null)
    {
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(functionName, nameof(functionName));
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(returnTypeSql, nameof(returnTypeSql));
        ArgumentNullExceptionCompatible.ThrowIfNull(parameters, nameof(parameters));
        ArgumentNullExceptionCompatible.ThrowIfNull(body, nameof(body));
        var sc = GetSchemaName(schemaName);
        if (!this.TryGetValue(sc, out var s) || s == null)
            CreateSchema(sc);

        var normalized = functionName.NormalizeName();
        var functions = this[sc].Functions;
        if (functions.ContainsKey(normalized) && !orReplace)
            throw new InvalidOperationException($"Function '{normalized}' already exists.");

        functions[normalized] = new ScalarFunctionDef(functionName, returnTypeSql.Trim(), parameters, body);
    }

    internal void DropFunction(
        string functionName,
        bool ifExists,
        string? schemaName = null)
    {
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(functionName, nameof(functionName));
        var sc = GetSchemaName(schemaName);
        var normalized = functionName.NormalizeName();

        if (this[sc].Functions.Remove(normalized))
            return;

        if (ifExists)
            return;

        throw new InvalidOperationException($"Function '{normalized}' does not exist.");
    }

    #endregion

    #region Sequences

    /// <summary>
    /// EN: Registers a sequence in the specified schema.
    /// PT: Registra uma sequence no schema informado.
    /// </summary>
    /// <param name="sequenceName">EN: Sequence name. PT: Nome da sequence.</param>
    /// <param name="sequence">EN: Sequence definition. PT: Definição da sequence.</param>
    /// <param name="schemaName">EN: Target schema. PT: Schema alvo.</param>
    public void AddSequence(
        string sequenceName,
        SequenceDef sequence,
        string? schemaName = null)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(sequenceName, nameof(sequenceName));
        ArgumentNullExceptionCompatible.ThrowIfNull(sequence, nameof(sequence));
        var sc = GetSchemaName(schemaName);
        if (!this.TryGetValue(sc, out var s) || s == null)
            CreateSchema(sc);
        this[sc].MutableSequences[sequenceName.NormalizeName()] = sequence;
    }

    /// <summary>
    /// EN: Creates and registers a sequence in the specified schema.
    /// PT: Cria e registra uma sequence no schema informado.
    /// </summary>
    /// <param name="sequenceName">EN: Sequence name. PT: Nome da sequence.</param>
    /// <param name="startValue">EN: First sequence value. PT: Primeiro valor da sequence.</param>
    /// <param name="incrementBy">EN: Increment step. PT: Passo de incremento.</param>
    /// <param name="currentValue">EN: Current value when known. PT: Valor atual quando conhecido.</param>
    /// <param name="schemaName">EN: Target schema. PT: Schema alvo.</param>
    /// <returns>EN: Registered sequence. PT: Sequence registrada.</returns>
    public SequenceDef AddSequence(
        string sequenceName,
        long startValue = 1,
        long incrementBy = 1,
        long? currentValue = null,
        string? schemaName = null)
    {
        var sequence = new SequenceDef(sequenceName, startValue, incrementBy, currentValue);
        AddSequence(sequenceName, sequence, schemaName);
        return sequence;
    }

    /// <summary>
    /// EN: Tries to get a sequence by name.
    /// PT: Tenta obter uma sequence pelo nome.
    /// </summary>
    /// <param name="sequenceName">EN: Sequence name. PT: Nome da sequence.</param>
    /// <param name="sequence">EN: Found sequence, if any. PT: Sequence encontrada, se houver.</param>
    /// <param name="schemaName">EN: Target schema. PT: Schema alvo.</param>
    /// <returns>EN: True if it exists. PT: True se existir.</returns>
    public bool TryGetSequence(
        string sequenceName,
        out SequenceDef? sequence,
        string? schemaName = null)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(sequenceName, nameof(sequenceName));
        var sc = GetSchemaName(schemaName);
        return this[sc].MutableSequences.TryGetValue(sequenceName.NormalizeName(), out sequence)
            && sequence != null;
    }

    internal void CreateSequence(
        string sequenceName,
        bool ifNotExists,
        long startValue = 1,
        long incrementBy = 1,
        string? schemaName = null)
    {
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(sequenceName, nameof(sequenceName));
        var sc = GetSchemaName(schemaName);
        if (!this.TryGetValue(sc, out var s) || s == null)
            CreateSchema(sc);

        var normalized = sequenceName.NormalizeName();
        var sequences = this[sc].MutableSequences;
        if (sequences.ContainsKey(normalized))
        {
            if (ifNotExists)
                return;

            throw new InvalidOperationException($"Sequence '{normalized}' already exists.");
        }

        sequences[normalized] = new SequenceDef(sequenceName, startValue, incrementBy);
    }

    internal void DropSequence(
        string sequenceName,
        bool ifExists,
        string? schemaName = null)
    {
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(sequenceName, nameof(sequenceName));
        var sc = GetSchemaName(schemaName);
        var normalized = sequenceName.NormalizeName();

        if (this[sc].MutableSequences.Remove(normalized))
            return;

        if (ifExists)
            return;

        throw new InvalidOperationException($"Sequence '{normalized}' does not exist.");
    }

    #endregion

    internal virtual IReadOnlyList<ITableMock> ListAllTablesBestEffort()
    {
        var allTables = new List<ITableMock>();
        foreach (var schema in Values)
            allTables.AddRange(schema.Tables.Values);
        allTables.AddRange(_globalTemporaryTables.Values);
        return allTables;
    }

    private void ResetVolatileDataCore(bool includeGlobalTemporaryTables)
    {
        foreach (var schema in Values)
        {
            foreach (var table in schema.Tables.Values)
                ResetTableState(table);
        }

        if (!includeGlobalTemporaryTables)
            return;

        foreach (var table in _globalTemporaryTables.Values)
            ResetTableState(table);
    }

    private static void ResetTableState(ITableMock table)
    {
        while (table.Count > 0)
            table.RemoveAt(table.Count - 1);

        table.NextIdentity = 1;
    }

    #region Backup / Restore (best-effort)

    internal virtual void BackupAllTablesBestEffort()
    {
        foreach (var schemas in this)
            schemas.Value.BackupAllTablesBestEffort();
    }

    internal virtual void RestoreAllTablesBestEffort()
    {
        foreach (var schemas in this)
            schemas.Value.RestoreAllTablesBestEffort();
    }

    internal virtual void ClearBackupAllTablesBestEffort()
    {
        foreach (var schemas in this)
            schemas.Value.ClearBackupAllTablesBestEffort();
    }

    #endregion

    IEnumerator<KeyValuePair<string, ISchemaMock>> IEnumerable<KeyValuePair<string, ISchemaMock>>.GetEnumerator()
    {
        foreach (var it in this)
            yield return new KeyValuePair<string, ISchemaMock>(it.Key, it.Value);
    }
}
