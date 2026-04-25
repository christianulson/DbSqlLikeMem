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

    /// <summary>
    /// EN: Controls whether execution plans are formatted and captured during command execution.
    /// PT: Controla se planos de execução serão formatados e capturados durante a execução dos comandos.
    /// </summary>
    public bool CaptureExecutionPlans { get; set; } = true;

    /// <summary>
    /// EN: Indicates whether global temporary table definitions are shared across connections for this database.
    /// PT: Indica se as definicoes de tabelas temporarias globais sao compartilhadas entre conexoes neste banco.
    /// </summary>
    public virtual bool GlobalTemporaryTablesShareDefinitionAcrossConnections => false;

    /// <summary>
    /// EN: Indicates whether global temporary table rows are shared across connections for this database.
    /// PT: Indica se as linhas de tabelas temporarias globais sao compartilhadas entre conexoes neste banco.
    /// </summary>
    public virtual bool GlobalTemporaryTablesShareRowsAcrossConnections => false;

    /// <summary>
    /// EN: Gets the implicit schema used when callers omit an explicit schema name.
    /// PT: Obtém o schema implícito usado quando os chamadores omitem um nome de schema explicito.
    /// </summary>
    protected virtual string DefaultSchemaName => "DefaultSchema";

    /// <summary>
    /// EN: Gets the schema lookup order used when callers omit an explicit schema name.
    /// PT: Obtém a ordem de consulta de schema usada quando os chamadores omitem um nome de schema explicito.
    /// </summary>
    protected virtual IReadOnlyList<string> ImplicitSchemaLookupOrder => [DefaultSchemaName];

    internal object SyncRoot { get; } = new();

    /// <summary>
    /// EN: Executes the given action while holding the database lock when ThreadSafe is enabled.
    /// PT: Executa a acao informada enquanto segura o lock do banco quando ThreadSafe estiver habilitado.
    /// </summary>
    /// <typeparam name="T">EN: Return type of the action. PT: Tipo de retorno da acao.</typeparam>
    /// <param name="action">EN: Action to execute under the lock. PT: Acao a executar sob o lock.</param>
    /// <returns>EN: Result returned by the action. PT: Resultado retornado pela acao.</returns>
    internal T ExecuteWithLock<T>(Func<T> action)
    {
        if (!ThreadSafe)
            return action();
        lock (SyncRoot)
            return action();
    }

    /// <summary>
    /// EN: Executes the given action while holding the database lock when ThreadSafe is enabled.
    /// PT: Executa a acao informada enquanto segura o lock do banco quando ThreadSafe estiver habilitado.
    /// </summary>
    /// <param name="action">EN: Action to execute under the lock. PT: Acao a executar sob o lock.</param>
    internal void ExecuteWithLock(Action action)
    {
        if (!ThreadSafe)
        {
            action();
            return;
        }
        lock (SyncRoot)
            action();
    }

    internal abstract SqlDialectBase Dialect { get; set; }

    private readonly Dictionary<string, ITableMock> _globalTemporaryTables =
        new(StringComparer.OrdinalIgnoreCase);
    private readonly HashSet<string> _runtimeFunctions =
        new(StringComparer.OrdinalIgnoreCase);
    private int _nextFirebirdTransactionId = 1;

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
            if (base.TryGetValue(DefaultSchemaName, out var defaultSchema)
                && defaultSchema != null)
                return DefaultSchemaName.NormalizeName();

            if (Count > 1)
                throw new InvalidOperationException(SqlExceptionMessages.MultipleSchemasRequireExplicitName(string.Join(",", this.Keys)));
            if (Count == 0)
                throw new InvalidOperationException(SqlExceptionMessages.NoSchemaRegistered());
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

    internal int AllocateFirebirdTransactionId()
        => _nextFirebirdTransactionId++;

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

    internal void RestoreGlobalTemporaryTable(
        ITableMock table,
        string? schemaName = null)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(table, nameof(table));
        var key = BuildTemporaryTableKey(table.TableName, schemaName ?? table.Schema.SchemaName);
        _globalTemporaryTables[key] = table;
    }

    internal void RemoveGlobalTemporaryTable(
        string tableName,
        string? schemaName = null)
        => _globalTemporaryTables.Remove(BuildTemporaryTableKey(tableName, schemaName));

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
        if (schemaName is null)
        {
            if (TryGetTable(tableName, out var implicitTable))
                return implicitTable!;

            throw SqlUnsupported.ForNormalizedTableDoesNotExist(tableName);
        }

        var sc = GetSchemaName(schemaName);
        if (!base.TryGetValue(sc, out var schema)
            || schema is null
            || !schema.TryGetTable(tableName, out var tb)
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
        if (schemaName is null)
            return TryGetTableInImplicitSchemas(tableName, out tb);

        var sc = GetSchemaName(schemaName);
        if (!base.TryGetValue(sc, out var schema) || schema is null)
        {
            tb = null;
            return false;
        }

        return schema.TryGetTable(tableName, out tb)
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
        if (schemaName is null)
            return TryGetTableInImplicitSchemas(tableName, out var _);

        var sc = GetSchemaName(schemaName);
        return base.TryGetValue(sc, out var schema)
            && schema is not null
            && schema.TryGetTable(tableName, out var tb)
            && tb != null;
    }

    internal IReadOnlyList<ITableMock> ListTables(
        string? schemaName = null)
    {
        var sc = GetSchemaName(schemaName);
        return base.TryGetValue(sc, out var schema) && schema is not null
            ? schema.Tables.Select(_ => _.Value).ToList().AsReadOnly()
            : Array.Empty<ITableMock>();
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

    internal IReadOnlyList<string> ListOwnedSequenceNames(
        string tableName,
        string? schemaName = null)
    {
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(tableName, nameof(tableName));
        var sc = GetSchemaName(schemaName);
        if (!this.TryGetValue(sc, out var schema) || schema is null)
            return [];

        var normalizedTableName = tableName.NormalizeName();
        return schema.Sequences
            .Where(entry =>
                entry.Value.OwnedByTable != null
                && entry.Value.OwnedByTable.Equals(normalizedTableName, StringComparison.OrdinalIgnoreCase)
                && string.Equals(entry.Value.OwnedBySchema ?? sc, sc, StringComparison.OrdinalIgnoreCase))
            .Select(entry => entry.Key)
            .ToList()
            .AsReadOnly();
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

            namedTable.IndexManager.DropIndex(indexName, ifExists);
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
            matchingTables[0].IndexManager.DropIndex(indexName, ifExists);
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
        string? computedExpression = null,
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
            defaultValue: defaultValue,
            computedExpression: computedExpression);
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
        if (!base.TryGetValue(sc, out var schema) || schema == null)
            schema = (SchemaMock)CreateSchema(sc);

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

            throw new InvalidOperationException(SqlExceptionMessages.ViewAlreadyExists(name!));
        }

        schema.Views[name!] = query.Select;
    }


    internal SqlSelectQuery GetView(
        string viewName,
        string? schemaName = null)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(viewName, nameof(viewName));
        var sc = GetSchemaName(schemaName);
        if (!base.TryGetValue(sc, out var schema)
            || schema is null
            || !schema.Views.TryGetValue(viewName, out var vw)
            || vw == null)
            throw new InvalidOperationException(SqlExceptionMessages.ViewDoesNotExist(viewName));
        return vw;
    }

    internal bool TryGetView(
        string viewName,
        out SqlSelectQuery? vw,
        string? schemaName = null)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(viewName, nameof(viewName));
        var sc = GetSchemaName(schemaName);
        if (!base.TryGetValue(sc, out var schema) || schema is null)
        {
            vw = null;
            return false;
        }

        return schema.Views.TryGetValue(viewName, out vw)
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

        throw new InvalidOperationException(SqlExceptionMessages.ViewDoesNotExist(normalized));
    }

    internal void RestoreView(
        string viewName,
        SqlSelectQuery definition,
        string? schemaName = null)
    {
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(viewName, nameof(viewName));
        ArgumentNullExceptionCompatible.ThrowIfNull(definition, nameof(definition));
        var sc = GetSchemaName(schemaName);
        this[sc].Views[viewName.NormalizeName()] = definition;
    }

    internal void RemoveView(
        string viewName,
        string? schemaName = null)
    {
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(viewName, nameof(viewName));
        var sc = GetSchemaName(schemaName);
        this[sc].Views.Remove(viewName.NormalizeName());
    }

    #endregion

    #region Procedures

    /// <summary>
    /// EN: Registers a stored procedure in the specified schema.
    /// PT: Registra um procedimento armazenado no schema informado.
    /// </summary>
    /// <param name="procedure">EN: Procedure definition. PT: Definição do procedimento.</param>
    /// <param name="schemaName">EN: Target schema. PT: Schema alvo.</param>
    public void AddProcedure(
        ProcedureDef procedure,
        string? schemaName = null)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(procedure.Name, nameof(procedure.Name));
        var sc = GetSchemaName(schemaName);
        if (!this.TryGetValue(sc, out var s) || s == null)
            CreateSchema(sc);
        this[sc].CreateProcedure(procedure.Name, procedure, orReplace: true);
    }

    /// <summary>
    /// EN: Registers a stored procedure using the legacy method name kept for compatibility.
    /// PT: Registra um procedimento armazenado usando o nome de metodo legado mantido por compatibilidade.
    /// </summary>
    /// <param name="pr">EN: Procedure definition. PT: Definição do procedimento.</param>
    /// <param name="schemaName">EN: Target schema. PT: Schema alvo.</param>
    public void AddProdecure(
        ProcedureDef pr,
        string? schemaName = null)
        => AddProcedure(pr, schemaName);

    /// <summary>
    /// EN: Registers a user-defined function in the specified schema.
    /// PT: Registra uma funcao definida pelo usuario no schema informado.
    /// </summary>
    /// <param name="function">EN: Function definition. PT: Definicao da funcao.</param>
    /// <param name="schemaName">EN: Target schema. PT: Schema alvo.</param>
    public void AddFunction(
        DbFunctionDef function,
        string? schemaName = null)
        => CreateFunction(function, orReplace: true, schemaName);

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
        if (!base.TryGetValue(sc, out var schema) || schema is null)
        {
            pr = null;
            return false;
        }

        return schema.TryGetProcedure(procName, out pr);
    }

    #endregion

    #region Functions

    internal bool TryGetFunction(
        string functionName,
        out DbFunctionDef? function,
        string? schemaName = null)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(functionName, nameof(functionName));
        if (schemaName is null)
        {
            foreach (var schema1 in Values)
            {
                if (schema1 is null)
                    continue;

                if (schema1.TryGetFunction(functionName, out function))
                    return true;
            }

            function = null;
            return false;
        }

        var sc = GetSchemaName(schemaName);
        if (!base.TryGetValue(sc, out var schema) || schema is null)
        {
            function = null;
            return false;
        }

        return schema.TryGetFunction(functionName, out function);
    }

    internal bool ContainsRuntimeFunction(string functionName)
    {
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(functionName, nameof(functionName));
        return _runtimeFunctions.Contains(functionName.NormalizeName());
    }

    /// <summary>
    /// EN: Replaces or registers a user-defined function in the specified schema.
    /// PT: Substitui ou registra uma funcao definida pelo usuario no schema informado.
    /// </summary>
    /// <param name="definition">EN: Function definition. PT: Definicao da funcao.</param>
    /// <param name="orReplace">EN: True to replace an existing definition. PT: True para substituir uma definicao existente.</param>
    /// <param name="schemaName">EN: Target schema. PT: Schema alvo.</param>
    public void CreateFunction(
        DbFunctionDef definition,
        bool orReplace = false,
        string? schemaName = null)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(definition, nameof(definition));
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(definition.Name, nameof(definition.Name));
        var sc = GetSchemaName(schemaName);
        if (!this.TryGetValue(sc, out var s) || s == null)
            CreateSchema(sc);
        this[sc].CreateFunction(
            definition,
            orReplace);
        _runtimeFunctions.Add(definition.Name.NormalizeName());

        if (TryGetFunction(definition.Name, out var currentDefinition, sc) && currentDefinition is not null)
            Dialect.Functions.Add(currentDefinition);
        else
            Dialect.Functions.Remove(definition.Name);
    }

    internal void DropFunction(
        string functionName,
        bool ifExists,
        string? schemaName = null)
    {
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(functionName, nameof(functionName));
        var sc = GetSchemaName(schemaName);
        this[sc].DropFunction(functionName, ifExists);
        _runtimeFunctions.Remove(functionName.NormalizeName());

        if (TryGetFunction(functionName, out var currentDefinition, null) && currentDefinition is not null)
            Dialect.Functions.Add(currentDefinition);
        else
            Dialect.Functions.Remove(functionName);
    }

    internal void RestoreFunction(
        string functionName,
        DbFunctionDef definition,
        string? schemaName = null)
    {
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(functionName, nameof(functionName));
        ArgumentNullExceptionCompatible.ThrowIfNull(definition, nameof(definition));
        var sc = GetSchemaName(schemaName);
        this[sc].RestoreFunction(functionName, definition);
        _runtimeFunctions.Add(functionName.NormalizeName());
        if (TryGetFunction(functionName, out var currentDefinition, sc) && currentDefinition is not null)
            Dialect.Functions.Add(currentDefinition);
        else
            Dialect.Functions.Remove(functionName);
        SqlQueryParser.ClearAstCache();
    }

    internal void RemoveFunction(
        string functionName,
        string? schemaName = null)
    {
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(functionName, nameof(functionName));
        var sc = GetSchemaName(schemaName);
        this[sc].RemoveFunction(functionName);
        _runtimeFunctions.Remove(functionName.NormalizeName());
        if (TryGetFunction(functionName, out var currentDefinition, null) && currentDefinition is not null)
            Dialect.Functions.Add(currentDefinition);
        else
            Dialect.Functions.Remove(functionName);
        SqlQueryParser.ClearAstCache();
    }

    #endregion

    #region Procedures

    /// <summary>
    /// EN: Replaces or registers a stored procedure in the specified schema.
    /// PT: Substitui ou registra um procedimento armazenado no schema informado.
    /// </summary>
    /// <param name="procedureName">EN: Procedure name. PT: Nome da procedure.</param>
    /// <param name="procedure">EN: Procedure definition. PT: Definição da procedure.</param>
    /// <param name="schemaName">EN: Target schema. PT: Schema alvo.</param>
    /// <param name="orReplace">EN: True to replace an existing definition. PT: True para substituir uma definição existente.</param>
    public void CreateProcedure(
        string procedureName,
        ProcedureDef procedure,
        bool orReplace = false,
        string? schemaName = null)
    {
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(procedureName, nameof(procedureName));
        ArgumentNullExceptionCompatible.ThrowIfNull(procedure, nameof(procedure));
        var sc = GetSchemaName(schemaName);
        if (!this.TryGetValue(sc, out var s) || s == null)
            CreateSchema(sc);
        this[sc].CreateProcedure(procedureName, procedure, orReplace);
    }

    /// <summary>
    /// EN: Restores a stored procedure definition in the specified schema.
    /// PT: Restaura uma definição de procedimento armazenado no schema informado.
    /// </summary>
    /// <param name="procedureName">EN: Procedure name. PT: Nome da procedure.</param>
    /// <param name="procedure">EN: Procedure definition. PT: Definição da procedure.</param>
    /// <param name="schemaName">EN: Target schema. PT: Schema alvo.</param>
    internal void RestoreProcedure(
        string procedureName,
        ProcedureDef procedure,
        string? schemaName = null)
    {
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(procedureName, nameof(procedureName));
        ArgumentNullExceptionCompatible.ThrowIfNull(procedure, nameof(procedure));
        var sc = GetSchemaName(schemaName);
        this[sc].RestoreProcedure(procedureName, procedure);
    }

    /// <summary>
    /// EN: Removes a stored procedure from the specified schema.
    /// PT: Remove um procedimento armazenado do schema informado.
    /// </summary>
    /// <param name="procedureName">EN: Procedure name. PT: Nome da procedure.</param>
    /// <param name="schemaName">EN: Target schema. PT: Schema alvo.</param>
    internal void RemoveProcedure(
        string procedureName,
        string? schemaName = null)
    {
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(procedureName, nameof(procedureName));
        var sc = GetSchemaName(schemaName);
        this[sc].RemoveProcedure(procedureName);
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
    /// <param name="minValue">EN: Minimum allowed value when the sequence is bounded. PT: Valor minimo permitido quando a sequence e limitada.</param>
    /// <param name="maxValue">EN: Maximum allowed value when the sequence is bounded. PT: Valor maximo permitido quando a sequence e limitada.</param>
    /// <param name="isCycle">EN: Whether the sequence wraps around when it reaches a bound. PT: Indica se a sequence reinicia ao atingir um limite.</param>
    /// <param name="ownedBySchema">EN: Schema of the owning table when the sequence is attached to a column. PT: Schema da tabela proprietaria quando a sequence e vinculada a uma coluna.</param>
    /// <param name="ownedByTable">EN: Owning table name when the sequence is attached to a column. PT: Nome da tabela proprietaria quando a sequence e vinculada a uma coluna.</param>
    /// <param name="ownedByColumn">EN: Owning column name when the sequence is attached to a column. PT: Nome da coluna proprietaria quando a sequence e vinculada a uma coluna.</param>
    /// <param name="schemaName">EN: Target schema. PT: Schema alvo.</param>
    /// <returns>EN: Registered sequence. PT: Sequence registrada.</returns>
    public SequenceDef AddSequence(
        string sequenceName,
        long startValue = 1,
        long incrementBy = 1,
        long? currentValue = null,
        long? minValue = null,
        long? maxValue = null,
        bool isCycle = false,
        string? ownedBySchema = null,
        string? ownedByTable = null,
        string? ownedByColumn = null,
        string? schemaName = null)
    {
        var sequence = new SequenceDef(
            sequenceName,
            startValue,
            incrementBy,
            currentValue,
            minValue,
            maxValue,
            isCycle,
            ownedBySchema,
            ownedByTable,
            ownedByColumn);
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
        if (schemaName is null)
            return TryGetSequenceInImplicitSchemas(sequenceName, out sequence);

        var sc = GetSchemaName(schemaName);
        if (!TryGetValue(sc, out var schema) || schema == null)
        {
            sequence = null;
            return false;
        }

        return schema.Sequences.TryGetValue(sequenceName.NormalizeName(), out sequence)
            && sequence != null;
    }

    private bool TryGetTableInImplicitSchemas(
        string tableName,
        out ITableMock? tb)
    {
        foreach (var schemaName in ImplicitSchemaLookupOrder)
        {
            if (string.IsNullOrWhiteSpace(schemaName))
                continue;

            if (!base.TryGetValue(schemaName, out var schema) || schema is null)
                continue;

            if (schema.TryGetTable(tableName, out tb) && tb != null)
                return true;
        }

        tb = null;
        return false;
    }

    private bool TryGetSequenceInImplicitSchemas(
        string sequenceName,
        out SequenceDef? sequence)
    {
        foreach (var schemaName in ImplicitSchemaLookupOrder)
        {
            if (string.IsNullOrWhiteSpace(schemaName))
                continue;

            if (!base.TryGetValue(schemaName, out var schema) || schema is null)
                continue;

            if (schema.Sequences.TryGetValue(sequenceName.NormalizeName(), out sequence)
                && sequence != null)
                return true;
        }

        sequence = null;
        return false;
    }

    internal void CreateSequence(
        string sequenceName,
        bool ifNotExists,
        long startValue = 1,
        long incrementBy = 1,
        long? minValue = null,
        long? maxValue = null,
        bool isCycle = false,
        string? ownedBySchema = null,
        string? ownedByTable = null,
        string? ownedByColumn = null,
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

        sequences[normalized] = new SequenceDef(
            sequenceName,
            startValue,
            incrementBy,
            null,
            minValue,
            maxValue,
            isCycle,
            ownedBySchema,
            ownedByTable,
            ownedByColumn);
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

    internal void RestoreSequence(
        string sequenceName,
        SequenceDef sequence,
        string? schemaName = null)
    {
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(sequenceName, nameof(sequenceName));
        ArgumentNullExceptionCompatible.ThrowIfNull(sequence, nameof(sequence));
        var sc = GetSchemaName(schemaName);
        this[sc].MutableSequences[sequenceName.NormalizeName()] = sequence;
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
        var schemas = Values.ToArray();
        if (ThreadSafe && schemas.Length > 1)
        {
            Parallel.ForEach(schemas, schema =>
            {
                foreach (var table in schema.Tables.Values)
                    ResetTableState(table);
            });
        }
        else
        {
            foreach (var schema in schemas)
            {
                foreach (var table in schema.Tables.Values)
                    ResetTableState(table);
            }
        }

        if (!includeGlobalTemporaryTables)
            return;

        var globalTables = _globalTemporaryTables.Values.ToArray();
        if (ThreadSafe && globalTables.Length > 1)
        {
            Parallel.ForEach(globalTables, ResetTableState);
            return;
        }

        foreach (var table in globalTables)
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
        var schemas = Values.ToArray();
        if (ThreadSafe && schemas.Length > 1)
        {
            Parallel.ForEach(schemas, schema => schema.BackupAllTablesBestEffort());
            return;
        }

        foreach (var schema in schemas)
            schema.BackupAllTablesBestEffort();
    }

    internal virtual void RestoreAllTablesBestEffort()
    {
        var schemas = Values.ToArray();
        if (ThreadSafe && schemas.Length > 1)
        {
            Parallel.ForEach(schemas, schema => schema.RestoreAllTablesBestEffort());
            return;
        }

        foreach (var schema in schemas)
            schema.RestoreAllTablesBestEffort();
    }

    internal virtual void ClearBackupAllTablesBestEffort()
    {
        var schemas = Values.ToArray();
        if (ThreadSafe && schemas.Length > 1)
        {
            Parallel.ForEach(schemas, schema => schema.ClearBackupAllTablesBestEffort());
            return;
        }

        foreach (var schema in schemas)
            schema.ClearBackupAllTablesBestEffort();
    }

    #endregion

    IEnumerator<KeyValuePair<string, ISchemaMock>> IEnumerable<KeyValuePair<string, ISchemaMock>>.GetEnumerator()
    {
        foreach (var it in this)
            yield return new KeyValuePair<string, ISchemaMock>(it.Key, it.Value);
    }
}
