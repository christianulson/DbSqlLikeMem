using System.Buffers;
using System.Collections.ObjectModel;
using System.Collections.Concurrent;
using System.Linq.Expressions;

namespace DbSqlLikeMem;

/// <summary>
/// EN: Base for an in-memory table with data, columns, and _indexes.
/// PT-br: Base de uma tabela em memória com dados, colunas e índices.
/// </summary>
public abstract class TableMock
    : ITableMock
{
    private const string PRIMARY = SqlConst.PRIMARY;
    private static readonly ConcurrentDictionary<Type, IReadOnlyDictionary<string, Func<object, object?>>> _itemAccessorCache = new();

    /// <summary>
    /// EN: Initializes the table with name, schema, and columns, with optional rows.
    /// PT-br: Inicializa a tabela com nome, schema e colunas, com linhas opcionais.
    /// </summary>
    /// <param name="tableName">EN: Table name. PT-br: Nome da tabela.</param>
    /// <param name="schema">EN: Parent schema. PT-br: Schema pai.</param>
    /// <param name="columns">EN: Table columns. PT-br: Colunas da tabela.</param>
    /// <param name="rows">EN: Initial rows. PT-br: Linhas iniciais.</param>
    protected TableMock(
        string tableName,
        SchemaMock schema,
        IEnumerable<Col> columns,
        IEnumerable<object?[]>? rows = null)
    {
        TableName = tableName.NormalizeName();
        Schema = schema;
        _triggerManager = new TableTriggerManager(this);
        _foreignKeyManager = new TableForeignKeyManager(this, ForeignKeyFails);
        _indexManager = new TableIndexManager(this);
        _stateManager = new TableStateManager(this);
        _columnsView = new ReadOnlyDictionary<string, ColumnDef>(_columns);
        _indexesView = new ReadOnlyDictionary<string, IndexDef>(_indexes);
        _primaryKeyIndexesView = new ReadOnlyHashSet<int>(_primaryKeyIndexes);
        foreach (var c in columns)
            AddColumn(c);
        AddRangeArray(rows ?? []);
    }

    /// <summary>
    /// EN: Normalized table name.
    /// PT-br: Nome normalizado da tabela.
    /// </summary>
    public string TableName { get; }

    /// <summary>
    /// EN: Schema to which the table belongs.
    /// PT-br: Schema ao qual a tabela pertence.
    /// </summary>
    public SchemaMock Schema { get; }

    /// <summary>
    /// EN: Next identity value for auto-increment columns.
    /// PT-br: Próximo valor de identidade para colunas auto incrementais.
    /// </summary>
    public int NextIdentity { get; set; } = 1;

    /// <summary>
    /// EN: Enables explicit values for identity columns when building scenarios or executing inserts.
    /// PT-br: Habilita valores explícitos para colunas identity ao montar cenários ou executar inserções.
    /// </summary>
    public bool AllowIdentityInsert { get; set; }

    /// <summary>
    /// EN: Partitioning clause captured from CREATE TABLE when the provider persists partition metadata.
    /// PT-br: Clausula de particionamento capturada do CREATE TABLE quando o provedor persiste metadados de particionamento.
    /// </summary>
    public string? PartitionClauseSql { get; set; }

    internal bool MatchesRequestedPartitions(
        IReadOnlyDictionary<int, object?> row,
        IReadOnlyCollection<string> requestedPartitionNames)
        => TablePartitionRouter.MatchesRequestedPartitions(this, row, requestedPartitionNames);

    internal bool TryGetPartitionName(
        IReadOnlyDictionary<int, object?> row,
        out string partitionName)
        => TablePartitionRouter.TryGetPartitionName(this, row, out partitionName);

    internal bool TryInferRequestedPartitionNames(
        IReadOnlyDictionary<string, object?> equalsByColumn,
        out IReadOnlyList<string> partitionNames)
        => TablePartitionRouter.TryInferRequestedPartitionNames(this, equalsByColumn, out partitionNames);

    internal bool TryInferRequestedPartitionNames(
        IEnumerable<object?> rawValues,
        out IReadOnlyList<string> partitionNames)
        => TablePartitionRouter.TryInferRequestedPartitionNames(this, rawValues, out partitionNames);

    internal bool TryInferRequestedPartitionNamesForRange(
        object? lowValue,
        object? highValue,
        out IReadOnlyList<string> partitionNames)
        => TablePartitionRouter.TryInferRequestedPartitionNamesForRange(this, lowValue, highValue, out partitionNames);

    internal bool TryInferRequestedPartitionNamesForRanges(
        IEnumerable<(object? Low, object? High)> ranges,
        out IReadOnlyList<string> partitionNames)
        => TablePartitionRouter.TryInferRequestedPartitionNamesForRanges(this, ranges, out partitionNames);

    internal bool TryGetPartitionNameForValue(
        object? rawValue,
        out string partitionName)
        => TablePartitionRouter.TryGetPartitionNameForValue(this, rawValue, out partitionName);

    internal bool TryGetPartitionedColumnName(out string partitionedColumnName)
        => TablePartitionRouter.TryGetPartitionedColumnName(this, out partitionedColumnName);

    /// <summary>
    /// EN: Sets the AllowIdentityInsert property.
    /// PT-br: Define a propriedade AllowIdentityInsert.
    /// </summary>
    /// <param name="allowIdentityInsert">
    /// EN: Enables explicit values for identity columns when building scenarios or executing inserts.
    /// PT-br: Habilita valores explícitos para colunas identity ao montar cenários ou executar inserções.
    /// </param>
    /// <returns></returns>
    public ITableMock SetAllowIdentityInsert(bool allowIdentityInsert)
    {
        AllowIdentityInsert = allowIdentityInsert;
        return this;
    }

    private readonly ColumnDictionary _columns = [];
    private readonly Dictionary<int, string> _columnsByIndex = [];
    private readonly List<ColumnDef> _columnsByOrdinal = [];
    private readonly ReadOnlyDictionary<string, ColumnDef> _columnsView;
    private readonly ReadOnlyDictionary<string, IndexDef> _indexesView;
    private readonly List<IndexDef> _uniqueIndexes = [];
    private readonly List<SchemaSnapshotCheckConstraint> _checkConstraints = [];
    private IReadOnlyList<CompiledCheckConstraint>? _compiledCheckConstraints;
    private int _compiledCheckConstraintCount = -1;
    private bool _hasPersistedComputedColumns;
    private bool _hasPersistedComputedColumnsInitialized;
    private int _indexVersion;
    private IndexDef[]? _cachedIndexes;
    private IReadOnlyHashSet<int> _primaryKeyIndexesView;

    /// <summary>
    /// EN: Table column dictionary.
    /// PT-br: Dicionário de colunas da tabela.
    /// </summary>
    public IReadOnlyDictionary<string, ColumnDef> Columns
        => _columnsView;

    internal IReadOnlyDictionary<int, string> ColumnsByIndex => _columnsByIndex;
    internal ColumnDictionary ColumnsRaw => _columns;
    internal IReadOnlyList<ColumnDef> ColumnsByOrdinal => _columnsByOrdinal;
    internal ColumnDef? IdentityColumn => _identityColumn;
    internal ColumnDef GetColumnByIndex(int index) => _columnsByOrdinal[index];
    internal TableIndexManager IndexManager => _indexManager;
    internal TableForeignKeyManager ForeignKeyManager => _foreignKeyManager;
    internal TableTriggerManager TriggerManager => _triggerManager;
    internal IReadOnlyList<IndexDef> UniqueIndexes => _uniqueIndexes;
    internal List<SchemaSnapshotCheckConstraint> CheckConstraintsMutable => _checkConstraints;
    internal int IndexVersion => _indexVersion;
    internal IndexDictionary IndexesMutable => _indexes;
    internal List<IndexDef> UniqueIndexesMutable => _uniqueIndexes;
    internal ReadOnlyDictionary<string, IndexDef> IndexesView => _indexesView;
    internal int IndexVersionValue
    {
        get => _indexVersion;
        set => _indexVersion = value;
    }

    internal void InvalidateIndexesCache() => _cachedIndexes = null;

    private readonly List<object?[]> _items = [];

    /// <summary>
    /// EN: Gets the read-only list of items in the table.
    /// PT-br: Obtem a lista somente leitura de itens na tabela.
    /// </summary>
    public IReadOnlyList<IReadOnlyDictionary<int, object?>> Items => new ArrayRowListAdapter(_items);

    internal object?[] GetRawRow(int index) => _items[index];

    /// <summary>
    /// EN: Gets the check constraints configured for the table.
    /// PT-br: Obtém as restricoes check configuradas para a tabela.
    /// </summary>
    public IReadOnlyList<SchemaSnapshotCheckConstraint> CheckConstraints => _checkConstraints;

    private sealed class ArrayRowListAdapter(List<object?[]> items) : IReadOnlyList<IReadOnlyDictionary<int, object?>>
    {
        public int Count => items.Count;
        public IReadOnlyDictionary<int, object?> this[int index] => new ArrayRow(items[index]);
        public IEnumerator<IReadOnlyDictionary<int, object?>> GetEnumerator()
        {
            foreach (var row in items)
                yield return new ArrayRow(row);
        }
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }

    internal HashSet<int> _primaryKeyIndexes = [];
    private int[] _pkIndexArray = [];
    private ColumnDef? _identityColumn;

    internal HashSet<int> PrimaryKeyIndexesMutable => _primaryKeyIndexes;
    internal int[] PkIndexArray => _pkIndexArray;
    internal Dictionary<IndexKey, int> PrimaryKeyLookup => _pkIndex;
    internal IReadOnlyHashSet<int> PrimaryKeyIndexesViewMutable => _primaryKeyIndexesView;
    internal void SetPrimaryKeyIndexArray(int[] value) => _pkIndexArray = value;
    internal void SetPrimaryKeyIndexesView(IReadOnlyHashSet<int> value) => _primaryKeyIndexesView = value;

    /// <summary>
    /// EN: Fast lookup dictionary mapping serialized PK values to row positions.
    /// PT-br: Dicionário rápido que mapeia valores de PK serializados para posições de linha.
    /// </summary>
    private readonly Dictionary<IndexKey, int> _pkIndex = new();

    // ---------- Wave D : índices ---------------------------------
    /// <summary>
    /// EN: Indexes of columns that form the primary key.
    /// PT-br: Índices das colunas que formam a chave primária.
    /// </summary>
    public IReadOnlyHashSet<int> PrimaryKeyIndexes => _primaryKeyIndexesView;

    /// <summary>
    /// EN: Adds primary key index columns by name.
    /// PT-br: Adiciona colunas de indice de chave primaria pelo nome.
    /// </summary>
    /// <param name="columns">EN: Primary key columns. PT-br: Colunas da chave primaria.</param>
    public void AddPrimaryKeyIndexes(params string[] columns)
        => _indexManager.AddPrimaryKeyIndexes(columns);

    /// <summary>
    /// EN: Tries to find a row by its primary key using the fast PK index.
    /// PT-br: Tenta encontrar uma linha pela chave primária usando o índice PK rápido.
    /// </summary>
    /// <param name="row">EN: Row containing PK values. PT-br: Linha contendo valores de PK.</param>
    /// <param name="rowIndex">EN: Found row index. PT-br: Índice da linha encontrada.</param>
    /// <returns>EN: True if a matching row was found. PT-br: True se uma linha correspondente foi encontrada.</returns>
    public bool TryFindRowByPk(IReadOnlyDictionary<int, object?> row, out int rowIndex)
        => _indexManager.TryFindRowByPk(row, out rowIndex);

    /// <summary>
    /// EN: Tries to find a row by primary key values already ordered by the PK definition.
    /// PT-br: Tenta encontrar uma linha por valores de chave primaria ja ordenados pela definicao da PK.
    /// </summary>
    /// <param name="values">EN: Primary key values in PK order. PT-br: Valores da chave primaria na ordem da PK.</param>
    /// <param name="rowIndex">EN: Found row index. PT-br: Indice da linha encontrada.</param>
    /// <returns>EN: True if a matching row was found. PT-br: True se uma linha correspondente foi encontrada.</returns>
    internal bool TryFindRowByPkValues(object?[] values, out int rowIndex)
        => _indexManager.TryFindRowByPkValues(values, out rowIndex);

    /// <summary>
    /// EN: Tries to find a row by a single primary key value.
    /// PT-br: Tenta encontrar uma linha por um unico valor de chave primaria.
    /// </summary>
    /// <param name="value">EN: Primary key value. PT-br: Valor da chave primaria.</param>
    /// <param name="rowIndex">EN: Found row index. PT-br: Indice da linha encontrada.</param>
    /// <returns>EN: True if a matching row was found. PT-br: True se uma linha correspondente foi encontrada.</returns>
    internal bool TryFindRowByPkValues(object? value, out int rowIndex)
        => _indexManager.TryFindRowByPkValues(value, out rowIndex);

    /// <summary>
    /// EN: Tries to find a row by two primary key values.
    /// PT-br: Tenta encontrar uma linha por dois valores de chave primaria.
    /// </summary>
    /// <param name="v1">EN: First primary key value. PT-br: Primeiro valor da chave primaria.</param>
    /// <param name="v2">EN: Second primary key value. PT-br: Segundo valor da chave primaria.</param>
    /// <param name="rowIndex">EN: Found row index. PT-br: Indice da linha encontrada.</param>
    /// <returns>EN: True if a matching row was found. PT-br: True se uma linha correspondente foi encontrada.</returns>
    internal bool TryFindRowByPkValues(object? v1, object? v2, out int rowIndex)
        => _indexManager.TryFindRowByPkValues(v1, v2, out rowIndex);

    /// <summary>
    /// EN: Tries to find a row by three primary key values.
    /// PT-br: Tenta encontrar uma linha por tres valores de chave primaria.
    /// </summary>
    /// <param name="v1">EN: First primary key value. PT-br: Primeiro valor da chave primaria.</param>
    /// <param name="v2">EN: Second primary key value. PT-br: Segundo valor da chave primaria.</param>
    /// <param name="v3">EN: Third primary key value. PT-br: Terceiro valor da chave primaria.</param>
    /// <param name="rowIndex">EN: Found row index. PT-br: Indice da linha encontrada.</param>
    /// <returns>EN: True if a matching row was found. PT-br: True se uma linha correspondente foi encontrada.</returns>
    internal bool TryFindRowByPkValues(object? v1, object? v2, object? v3, out int rowIndex)
        => _indexManager.TryFindRowByPkValues(v1, v2, v3, out rowIndex);

    private readonly TableForeignKeyManager _foreignKeyManager;
    private readonly TableIndexManager _indexManager;
    private readonly TableStateManager _stateManager;

    /// <summary>
    /// EN: List of foreign keys defined in the table.
    /// PT-br: Lista de chaves estrangeiras definidas na tabela.
    /// </summary>
    public IReadOnlyDictionary<string, ForeignDef> ForeignKeys => _foreignKeyManager.ForeignKeys;

    internal IndexDictionary _indexes = [];
    internal IndexDictionary IndexesRaw => _indexes;

    /// <summary>
    /// EN: Indexes declared on the table.
    /// PT-br: Índices declarados na tabela.
    /// </summary>
    public IReadOnlyDictionary<string, IndexDef> Indexes
        => _indexesView;

    private readonly TableTriggerManager _triggerManager;
    internal event Action<TableMutationNotification>? MutationApplied;

    /// <summary>
    /// EN: Registers a trigger callback for the specified table event.
    /// PT-br: Registra um callback de trigger para o evento de tabela especificado.
    /// </summary>
    /// <param name="evt">EN: Trigger event. PT-br: Evento da trigger.</param>
    /// <param name="handler">EN: Callback handler. PT-br: Manipulador de callback.</param>
    public void AddTrigger(TableTriggerEvent evt, Action<TableTriggerContext> handler)
        => _triggerManager.AddTrigger(evt, handler);

    /// <inheritdoc />
    public bool HasTriggers(TableTriggerEvent evt)
        => _triggerManager.HasTriggers(evt);

    /// <summary>
    /// EN: Creates a detachable row snapshot without allocating a dictionary.
    /// PT-br: Cria um snapshot de linha sem alocar um dicionário.
    /// </summary>
    internal static IReadOnlyDictionary<int, object?> SnapshotRow(object?[]? row)
    {
        if (row is null)
            return null!;

        var snapshot = new object?[row.Length];
        Array.Copy(row, snapshot, row.Length);

        var metrics = DbMetrics.Current;
        if (metrics is null)
            return new ArrayRow(snapshot);

        metrics.IncrementPerformancePhaseHit(DbPerformanceMetricKeys.RowSnapshot);
        var startedAt = Stopwatch.GetTimestamp();
        try
        {
            return new ArrayRow(snapshot);
        }
        finally
        {
            metrics.IncrementPerformancePhaseElapsedTicks(
                DbPerformanceMetricKeys.RowSnapshot,
                StopwatchCompatible.GetElapsedTicks(startedAt));
        }
    }

    /// <summary>
    /// EN: Creates a detachable row snapshot from an IReadOnlyDictionary row.
    /// PT-br: Cria um snapshot de linha a partir de uma linha IReadOnlyDictionary.
    /// </summary>
    internal static IReadOnlyDictionary<int, object?> SnapshotRow(IReadOnlyDictionary<int, object?>? row)
    {
        if (row is null)
            return null!;

        return SnapshotRow(CloneRow(row));
    }

    private void NotifyMutationApplied(
        TableMutationKind kind,
        int rowIndex,
        object?[] row,
        object?[]? oldRowSnapshot = null,
        int previousNextIdentity = 0)
    {
        if (MutationApplied is null)
            return;

        MutationApplied(new TableMutationNotification(
            this,
            kind,
            rowIndex,
            row,
            oldRowSnapshot,
            previousNextIdentity));
    }

    /// <summary>
    /// EN: Add new Column to Table
    /// PT-br: Incluir nova coluna na tabela
    /// </summary>
    /// <param name="name"></param>
    /// <param name="dbType"></param>
    /// <param name="nullable"></param>
    /// <param name="size"></param>
    /// <param name="decimalPlaces"></param>
    /// <param name="identity"></param>
    /// <param name="defaultValue"></param>
    /// <param name="enumValues"></param>
    /// <param name="computedExpression"></param>
    /// <returns></returns>
    public ColumnDef AddColumn(
        string name,
        DbType dbType,
        bool nullable,
        int? size = null,
        int? decimalPlaces = null,
        bool identity = false,
        object? defaultValue = null,
        IList<string>? enumValues = null,
        string? computedExpression = null)
    {
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(name, nameof(name));

        var normalizedName = name.NormalizeName();
        if (_columns.ContainsKey(normalizedName))
            throw new InvalidOperationException(SqlExceptionMessages.ColumnAlreadyExistsInTable(normalizedName, TableName));

        if (_items.Count != 0 && !nullable && defaultValue == null && !identity)
            throw new InvalidOperationException($"Cannot add NOT NULL column '{name}' without default value when the table already has rows.");

        var idx = _columns.Count;
        var col = new ColumnDef(
            table: this,
            name: normalizedName,
            index: idx,
            dbType: dbType,
            nullable: nullable,
            size: size,
            decimalPlaces: decimalPlaces,
            identity: identity,
            defaultValue: defaultValue,
            enumValues: enumValues,
            computedExpression: computedExpression);

        _columns.Add(normalizedName, col);
        _columnsByIndex[col.Index] = normalizedName;
        _columnsByOrdinal.Add(col);
        col.MetadataChanged = InvalidatePersistedComputedColumnsCache;
        InvalidatePersistedComputedColumnsCache();
        if (identity && _identityColumn is null)
            _identityColumn = col;
        BackfillAddedColumn(col);
        return col;
    }

    /// <summary>
    /// EN: Add new Column to Table
    /// PT-br: Incluir nova coluna na tabela
    /// </summary>
    /// <returns></returns>
    public ColumnDef AddColumn(
        Col column)
        => AddColumn(
            column.name,
            column.dbType,
            column.nullable,
            column.size,
            column.decimalPlaces,
            column.identity,
            column.defaultValue,
            column.enumValues,
            column.computedExpression);

    private void BackfillAddedColumn(ColumnDef column)
    {
        if (_items.Count == 0)
            return;

        var requiredSize = _columns.Count;
        for (var i = 0; i < _items.Count; i++)
        {
            var oldRow = _items[i];
            if (oldRow.Length < requiredSize)
            {
                var newRow = new object?[requiredSize];
                Array.Copy(oldRow, newRow, oldRow.Length);
                _items[i] = newRow;
                oldRow = newRow;
            }

            object? value;
            if (column.Identity)
                value = NextIdentity++;
            else if (column.DefaultValue is SequenceDef sequenceDefault)
                value = ResolveSequenceDefault(sequenceDefault);
            else if (GuidDefaultValueHelper.TryGetGeneratedGuidDefaultValue(column.DefaultValue, out var generatedGuidDefaultValue))
                value = GuidDefaultValueHelper.CreateGuidValue(generatedGuidDefaultValue);
            else
                value = column.DefaultValue;

            oldRow[column.Index] = value;

            if (column.GetGenValue != null && column.PersistComputedValue)
                oldRow[column.Index] = column.GetGenValue(new ArrayRow(oldRow), this);

            if (column.GetGenValue != null && !column.PersistComputedValue)
                continue;

            if (!column.Nullable && oldRow[column.Index] == null)
                throw ColumnCannotBeNull(column.Name);
        }
    }

    private void InvalidatePersistedComputedColumnsCache()
    {
        _hasPersistedComputedColumnsInitialized = false;
    }

    /// <summary>
    /// EN: Returns the ColumnDef for <paramref name="columnName"/> or throws UnknownColumn.
    /// PT-br: Retorna o ColumnDef para <paramref name="columnName"/>
    /// ou lança UnknownColumn se não existir.
    /// </summary>
    public ColumnDef GetColumn(string columnName)
    {
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(columnName, nameof(columnName));
        var normalized = columnName.NormalizeName();
        if (!_columns.TryGetValue(normalized, out var info))
        {
            var dotIndex = normalized.LastIndexOf('.');
            if (dotIndex >= 0 && dotIndex + 1 < normalized.Length)
                normalized = normalized[(dotIndex + 1)..];
        }
        if (!_columns.TryGetValue(normalized, out info))
            throw UnknownColumn(columnName);
        return info;
    }

    /// <summary>
    /// EN: Creates and registers an index on the table.
    /// PT-br: Cria e registra um índice na tabela.
    /// </summary>
    public IndexDef CreateIndex(
        string name,
        IEnumerable<string> keyCols,
        string[]? include = null,
        bool unique = false)
        => _indexManager.CreateIndex(name, keyCols, include, unique);

    /// <summary>
    /// EN: Looks up values in the index using the given key.
    /// PT-br: Procura valores no índice usando a chave informada.
    /// </summary>
    /// <param name="def">EN: Index definition. PT-br: Definição do índice.</param>
    /// <param name="key">EN: Key to search. PT-br: Chave a buscar.</param>
    /// <returns>EN: List of positions or null. PT-br: Lista de posições ou null.</returns>
    public IReadOnlyDictionary<int, IReadOnlyDictionary<string, object?>>? Lookup(
        IndexDef def,
        IndexKey key)
        => _indexManager.Lookup(def, key);

    /// <summary>
    /// EN: Updates index structures using the specified row.
    /// PT-br: Atualiza estruturas de indice usando a linha indicada.
    /// </summary>
    /// <param name="rowIdx">EN: Updated row index. PT-br: Indice da linha atualizada.</param>
    public void UpdateIndexesWithRow(int rowIdx)
        => _indexManager.UpdateIndexesWithRow(rowIdx);

    /// <summary>
    /// EN: Rebuilds all table _indexes.
    /// PT-br: Reconstrói todos os índices da tabela.
    /// </summary>
    public void RebuildAllIndexes()
    {
        _indexManager.RebuildAllIndexes();
        _indexManager.RebuildPkIndex();
    }


    /// <summary>
    /// EN: Creates and registers a foreign key definition for the current table.
    /// PT-br: Cria e registra uma definicao de chave estrangeira para a tabela atual.
    /// </summary>
    public ForeignDef CreateForeignKey(
        string name,
        string refTable,
        HashSet<(string col, string refCol)> references)
        => _foreignKeyManager.CreateForeignKey(name, refTable, references);

    /// <summary>
    /// EN: Adds multiple items by converting them into rows.
    /// PT-br: Adiciona vários itens convertendo-os em linhas.
    /// </summary>
    /// <typeparam name="T">EN: Item type. PT-br: Tipo dos itens.</typeparam>
    /// <param name="items">EN: Items to insert. PT-br: Itens a inserir.</param>
    public ITableMock AddRangeItems<T>(IEnumerable<T> items)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(items, nameof(items));
        var rows = items is ICollection<T> collection
            ? new List<object?[]>(collection.Count)
            : new List<object?[]>();
        foreach (var item in items)
            rows.Add(MaterializeItem(item));

        return AddBatchArray(rows);
    }

    /// <summary>
    /// EN: Adds already materialized rows.
    /// PT-br: Adiciona linhas já materializadas.
    /// </summary>
    /// <param name="items">EN: Rows to insert. PT-br: Linhas a inserir.</param>
    public ITableMock AddRange(IEnumerable<Dictionary<int, object?>> items)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(items, nameof(items));
        var rows = new List<object?[]>();
        var explicitSets = new List<ISet<int>?>();
        foreach (var dict in items)
        {
            rows.Add(DictToArray(dict));
            explicitSets.Add(new HashSet<int>(dict.Keys));
        }
        return AddBatchArray(rows, explicitSets);
    }

    internal ITableMock AddRangeArray(IEnumerable<object?[]> items)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(items, nameof(items));
        if (items is IReadOnlyList<object?[]> materializedRows)
            return AddBatchArray(materializedRows);

        var rows = items is ICollection<object?[]> collection
            ? new List<object?[]>(collection.Count)
            : new List<object?[]>();
        foreach (var row in items)
            rows.Add(row);

        return AddBatchArray(rows);
    }

    /// <summary>
    /// EN: Adds an item by converting it into a table row.
    /// PT-br: Adiciona um item convertendo-o em linha da tabela.
    /// </summary>
    /// <typeparam name="T">EN: Item type. PT-br: Tipo do item.</typeparam>
    /// <param name="item">EN: Item to insert. PT-br: Item a inserir.</param>
    public ITableMock AddItem<T>(T item)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(item, nameof(item));
        AddArray(MaterializeItem(item));
        return this;
    }

    private object?[] MaterializeItem<T>(T item)
    {
        var metrics = DbMetrics.Current;
        if (metrics is null)
            return MaterializeItemCore(item);

        metrics.IncrementPerformancePhaseHit(DbPerformanceMetricKeys.MaterializationObjectRow);
        var startedAt = Stopwatch.GetTimestamp();
        try
        {
            return MaterializeItemCore(item);
        }
        finally
        {
            metrics.IncrementPerformancePhaseElapsedTicks(
                DbPerformanceMetricKeys.MaterializationObjectRow,
                StopwatchCompatible.GetElapsedTicks(startedAt));
        }
    }

    private object?[] MaterializeItemCore<T>(T item)
    {
        var row = new object?[_columns.Count];
        var accessors = GetItemAccessors(typeof(T));

        foreach (var p in Columns)
        {
            if (!accessors.TryGetValue(p.Key, out var getter))
                continue;

            object? value;
#pragma warning disable CA1031 // Do not catch general exception types
            try
            {
                value = getter(item!);
            }
            catch
            {
                value = null;
            }
#pragma warning restore CA1031 // Do not catch general exception types

            row[p.Value.Index] = value;
        }

        return row;
    }

    private static IReadOnlyDictionary<string, Func<object, object?>> GetItemAccessors(Type itemType)
        => _itemAccessorCache.GetOrAdd(itemType, BuildItemAccessors);

    private static IReadOnlyDictionary<string, Func<object, object?>> BuildItemAccessors(Type itemType)
    {
        var accessors = new Dictionary<string, Func<object, object?>>(StringComparer.OrdinalIgnoreCase);
        foreach (var property in itemType.GetProperties(BindingFlags.Instance | BindingFlags.Public))
        {
            if (property.GetIndexParameters().Length > 0 || !property.CanRead)
                continue;

            accessors[property.Name] = BuildItemAccessor(itemType, property);
        }

        return accessors;
    }

    private static Func<object, object?> BuildItemAccessor(Type itemType, PropertyInfo property)
    {
        var instance = Expression.Parameter(typeof(object), "instance");
        var typedInstance = Expression.Convert(instance, itemType);
        var propertyAccess = Expression.Property(typedInstance, property);
        var boxedValue = Expression.Convert(propertyAccess, typeof(object));
        return Expression.Lambda<Func<object, object?>>(boxedValue, instance).Compile();
    }

    /// <summary>
    /// EN: Adds multiple rows in batch (compatibility shim).
    /// PT-br: Adiciona multiplas linhas em lote (shim de compatibilidade).
    /// </summary>
    public ITableMock AddBatch(IReadOnlyList<Dictionary<int, object?>> values)
    {
        var converted = new List<object?[]>(values.Count);
        var explicitSets = new List<ISet<int>?>(values.Count);
        foreach (var dict in values)
        {
            converted.Add(DictToArray(dict));
            explicitSets.Add(new HashSet<int>(dict.Keys));
        }
        return AddBatchArray(converted, explicitSets);
    }

    /// <summary>
    /// EN: Adds multiple rows in batch while validating uniqueness and updating indexes incrementally.
    /// PT-br: Adiciona multiplas linhas em lote validando unicidade e atualizando indices de forma incremental.
    /// </summary>
    /// <param name="values">EN: Rows to insert. PT-br: Linhas a inserir.</param>
    /// <param name="explicitColumnSets"></param>
    public ITableMock AddBatchArray(IReadOnlyList<object?[]> values, IReadOnlyList<ISet<int>?>? explicitColumnSets = null)
    {
        var valueCount = values.Count;
        if (valueCount == 0)
            return this;

        if (valueCount == 1 || HasSelfReferencingForeignKey())
        {
            for (var i = 0; i < valueCount; i++)
            {
                var explicitSet = explicitColumnSets?[i];
                AddArray(values[i], explicitSet is not null ? new HashSet<int>(explicitSet) : null);
            }
            return this;
        }

        var hasForeignKeys = _foreignKeyManager.HasForeignKeys;
        if (_indexes.Count == 0 && _uniqueIndexes.Count == 0)
            return AddBatchWithoutSecondaryIndexes(values, hasForeignKeys, explicitColumnSets);

        var context = new BatchInsertContext(this, values, explicitColumnSets, hasForeignKeys);
        ValidateAndPrepareBatch(context);
        InsertBatchRows(context);
        UpdateBatchIndexes(context);
        NotifyBatchMutations(context);
        ReturnIndexKeyMatrix(context);
        return this;
    }

    private sealed record BatchInsertContext(
        TableMock Table,
        IReadOnlyList<object?[]> Values,
        IReadOnlyList<ISet<int>?>? ExplicitColumnSets,
        bool HasForeignKeys)
    {
        public int ValueCount => Values.Count;
        public IndexDef[] AllIndexes => _allIndexes ??= Table._cachedIndexes ??= Table._indexes.Values.ToArray();
        private IndexDef[]? _allIndexes;

        public int AllIndexCount => AllIndexes.Length;
        public int UniqueIndexCount => Table._uniqueIndexes.Count;

        public int[] UniqueIndexSlots => _uniqueIndexSlots ??= BuildUniqueIndexSlots();
        private int[]? _uniqueIndexSlots;

        public HashSet<IndexKey>? BatchPrimaryKeys => Table._primaryKeyIndexes.Count > 0
            ? _batchPrimaryKeys ??= new HashSet<IndexKey>()
            : null;
        private HashSet<IndexKey>? _batchPrimaryKeys;

        public bool HasMutationApplied => Table.MutationApplied is not null;

        public int[]? PreviousNextIdentities => HasMutationApplied
            ? _previousNextIdentities ??= new int[ValueCount]
            : null;
        private int[]? _previousNextIdentities;

        public HashSet<IndexKey>?[]? BatchUniqueSets => UniqueIndexCount > 0
            ? _batchUniqueSets ??= CreateBatchUniqueSets()
            : null;
        private HashSet<IndexKey>?[]? _batchUniqueSets;

        public IndexKey[][] IndexKeysMatrix => _indexKeysMatrix ??= RentIndexKeyMatrix();
        private IndexKey[][]? _indexKeysMatrix;

        public bool HasPersistedComputedColumns => Table.HasPersistedComputedColumns();
        public bool HasPrimaryKey => Table._primaryKeyIndexes.Count > 0;

        public int StartIndex => Table._items.Count;

        private int[] BuildUniqueIndexSlots()
        {
            if (UniqueIndexCount == 0) return [];
            var slots = new int[UniqueIndexCount];
            var slot = 0;
            for (var i = 0; i < AllIndexCount; i++)
            {
                if (AllIndexes[i].Unique)
                    slots[slot++] = i;
            }
            return slots;
        }

        private HashSet<IndexKey>?[] CreateBatchUniqueSets()
        {
            var sets = new HashSet<IndexKey>?[UniqueIndexCount];
            for (var i = 0; i < UniqueIndexCount; i++)
                sets[i] = new HashSet<IndexKey>();
            return sets;
        }

        private IndexKey[][] RentIndexKeyMatrix()
        {
            var matrix = new IndexKey[AllIndexCount][];
            for (int i = 0; i < AllIndexCount; i++)
                matrix[i] = ArrayPool<IndexKey>.Shared.Rent(ValueCount);
            return matrix;
        }
    }

    private void ValidateAndPrepareBatch(BatchInsertContext ctx)
    {
        for (var valueIndex = 0; valueIndex < ctx.ValueCount; valueIndex++)
        {
            var row = ctx.Values[valueIndex];
            var explicitSet = ctx.ExplicitColumnSets?[valueIndex];

            if (ctx.HasMutationApplied)
                ctx.PreviousNextIdentities![valueIndex] = NextIdentity;

            ApplyDefaultValues(row, explicitSet);
            if (ctx.HasPersistedComputedColumns)
                RefreshPersistedComputedValues(row);
            ValidateCheckConstraintsOnRow(row);
            if (ctx.HasForeignKeys)
                _foreignKeyManager.ValidateForeignKeysOnRow(row);

            if (ctx.BatchPrimaryKeys is not null)
                _indexManager.EnsurePrimaryKeyUniqueOnInsert(new ArrayRow(row), ctx.BatchPrimaryKeys);

            for (int i = 0; i < ctx.AllIndexCount; i++)
            {
                var index = ctx.AllIndexes[i];
                ctx.IndexKeysMatrix[i][valueIndex] = index.BuildIndexKey(new ArrayRow(row));
            }

            for (var uniqueSlot = 0; uniqueSlot < ctx.UniqueIndexCount; uniqueSlot++)
            {
                var index = ctx.AllIndexes[ctx.UniqueIndexSlots[uniqueSlot]];
                var key = ctx.IndexKeysMatrix[ctx.UniqueIndexSlots[uniqueSlot]][valueIndex];
                var uniqueKeys = ctx.BatchUniqueSets![uniqueSlot]!;
                if (index.LookupMutable(key)?.Count > 0 || !uniqueKeys.Add(key))
                    throw DuplicateKey(TableName, index.Name, key);
            }
        }
    }

    private void InsertBatchRows(BatchInsertContext ctx)
    {
        _items.AddRange(ctx.Values);
    }

    private void UpdateBatchIndexes(BatchInsertContext ctx)
    {
        for (var rowOffset = 0; rowOffset < ctx.ValueCount; rowOffset++)
        {
            var rowIndex = ctx.StartIndex + rowOffset;
            for (int i = 0; i < ctx.AllIndexCount; i++)
                ctx.AllIndexes[i].UpdateIndexesWithRow(rowIndex, new ArrayRow(ctx.Values[rowOffset]), ctx.IndexKeysMatrix[i][rowOffset]);
        }

        if (ctx.HasPrimaryKey)
            _indexManager.RegisterPrimaryKeys(ctx.StartIndex, ctx.Values);
    }

    private void NotifyBatchMutations(BatchInsertContext ctx)
    {
        if (!ctx.HasMutationApplied) return;
        for (var rowOffset = 0; rowOffset < ctx.ValueCount; rowOffset++)
        {
            NotifyMutationApplied(
                TableMutationKind.Insert,
                ctx.StartIndex + rowOffset,
                ctx.Values[rowOffset],
                previousNextIdentity: ctx.PreviousNextIdentities![rowOffset]);
        }
    }

    private static void ReturnIndexKeyMatrix(BatchInsertContext ctx)
    {
        for (int i = 0; i < ctx.AllIndexCount; i++)
            ArrayPool<IndexKey>.Shared.Return(ctx.IndexKeysMatrix[i], clearArray: true);
    }

    private ITableMock AddBatchWithoutSecondaryIndexes(
        IReadOnlyList<object?[]> values,
        bool hasForeignKeys,
        IReadOnlyList<ISet<int>?>? explicitColumnSets = null)
    {
        var valueCount = values.Count;
        var hasMutationApplied = MutationApplied is not null;
        var previousNextIdentities = hasMutationApplied ? new int[valueCount] : null;
        var hasPersistedComputedColumns = HasPersistedComputedColumns();
        var batchPrimaryKeys = _primaryKeyIndexes.Count > 0
            ? new HashSet<IndexKey>()
            : null;
        for (var valueIndex = 0; valueIndex < valueCount; valueIndex++)
        {
            var row = values[valueIndex];
            if (hasMutationApplied)
                previousNextIdentities![valueIndex] = NextIdentity;

            ApplyDefaultValues(row, explicitColumnSets?[valueIndex]);
            if (hasPersistedComputedColumns)
                RefreshPersistedComputedValues(row);
            ValidateCheckConstraintsOnRow(row);
            if (hasForeignKeys)
                _foreignKeyManager.ValidateForeignKeysOnRow(row);

            if (batchPrimaryKeys is null)
                continue;

            _indexManager.EnsurePrimaryKeyUniqueOnInsert(new ArrayRow(row), batchPrimaryKeys);
        }

        var startIndex = _items.Count;
        _items.AddRange(values);

        if (batchPrimaryKeys is not null)
            _indexManager.RegisterPrimaryKeys(startIndex, values);

        if (hasMutationApplied)
        {
            for (var rowOffset = 0; rowOffset < valueCount; rowOffset++)
            {
                NotifyMutationApplied(
                    TableMutationKind.Insert,
                    startIndex + rowOffset,
                    values[rowOffset],
                    previousNextIdentity: previousNextIdentities![rowOffset]);
            }
        }

        return this;
    }

    private bool HasSelfReferencingForeignKey()
        => _foreignKeyManager.HasSelfReferencingForeignKey;

    /// <summary>
    /// EN: Adds a row ensuring default values and uniqueness.
    /// PT-br: Adiciona uma linha garantindo valores padrao e unicidade.
    /// </summary>
    /// <param name="value">EN: Row to insert. PT-br: Linha a inserir.</param>
    public ITableMock Add(Dictionary<int, object?> value)
    {
        var arr = DictToArray(value);
        return AddArray(arr, new HashSet<int>(value.Keys));
    }

    internal ITableMock AddArray(object?[] value, ISet<int>? explicitlyProvidedColumns = null)
    {
        var hasMutationApplied = MutationApplied is not null;
        var previousNextIdentity = hasMutationApplied ? NextIdentity : 0;
        var hasPersistedComputedColumns = HasPersistedComputedColumns();
        ApplyDefaultValues(value, explicitlyProvidedColumns);
        if (hasPersistedComputedColumns)
            RefreshPersistedComputedValues(value);
        ValidateCheckConstraintsOnRow(value);
        if (_foreignKeyManager.HasForeignKeys)
            _foreignKeyManager.ValidateForeignKeysOnRow(value);
        _indexManager.EnsureUniqueOnInsert(new ArrayRow(value));
        _items.Add(value);
        // Update _indexes with the new row
        int newIdx = Count - 1;
        _indexManager.UpdateIndexesWithRow(newIdx);
        _indexManager.RegisterPrimaryKey(newIdx, new ArrayRow(value));
        if (hasMutationApplied)
        {
            NotifyMutationApplied(
                TableMutationKind.Insert,
                newIdx,
                value,
                previousNextIdentity: previousNextIdentity);
        }
        return this;
    }

    private object?[] DictToArray(Dictionary<int, object?> dict)
    {
        var arr = new object?[_columns.Count];
        foreach (var kv in dict)
            if (kv.Key >= 0 && kv.Key < arr.Length)
                arr[kv.Key] = kv.Value;
        return arr;
    }

    private void ApplyDefaultValues(object?[] value, ISet<int>? explicitlyProvidedColumns = null)
    {
        foreach (var col in _columnsByOrdinal)
        {
            var currentValue = value[col.Index];

            if (!col.Identity)
            {
                if (explicitlyProvidedColumns is not null && explicitlyProvidedColumns.Contains(col.Index))
                    continue;

                if (currentValue is not null)
                    continue;

                if (col.DefaultValue is SequenceDef sequenceDefault)
                    value[col.Index] = ResolveSequenceDefault(sequenceDefault);
                else if (GuidDefaultValueHelper.TryGetGeneratedGuidDefaultValue(col.DefaultValue, out var generatedGuidDefaultValue))
                    value[col.Index] = GuidDefaultValueHelper.CreateGuidValue(generatedGuidDefaultValue);
                else if (col.DefaultValue != null)
                    value[col.Index] = col.DefaultValue;
                else
                    value[col.Index] = null;
            }
            else if (AllowIdentityInsert && currentValue is not null)
                UpdateNextIdentityFromExplicitValue(currentValue);
            else
                value[col.Index] = NextIdentity++;

            if (col.GetGenValue != null && col.PersistComputedValue)
                value[col.Index] = col.GetGenValue(new ArrayRow(value), this);

            if (col.GetGenValue != null && !col.PersistComputedValue)
                continue;

            if (!col.Nullable && AstQueryExecutorBase.IsNullish(value[col.Index]))
                throw ColumnCannotBeNull(col.Name);
        }
    }

    internal void ValidateCheckConstraintsOnRow(object?[] row)
    {
        if (_checkConstraints.Count == 0)
            return;

        var compiledConstraints = GetCompiledCheckConstraints();
        TableCheckConstraintEvaluator.Validate(this, new ArrayRow(row), compiledConstraints);
    }

    private IReadOnlyList<CompiledCheckConstraint> GetCompiledCheckConstraints()
    {
        if (_compiledCheckConstraints is not null
            && _compiledCheckConstraintCount == _checkConstraints.Count)
        {
            return _compiledCheckConstraints;
        }

        var compiledConstraints = new List<CompiledCheckConstraint>(_checkConstraints.Count);
        for (var i = 0; i < _checkConstraints.Count; i++)
            compiledConstraints.Add(TableCheckConstraintEvaluator.Compile(this, _checkConstraints[i]));

        _compiledCheckConstraints = compiledConstraints;
        _compiledCheckConstraintCount = _checkConstraints.Count;
        return _compiledCheckConstraints;
    }

    private object ResolveSequenceDefault(SequenceDef sequenceDefault)
    {
        var targetSchema = sequenceDefault.OwnedBySchema ?? Schema.SchemaName;
        if (!Schema.Db.TryGetSequence(sequenceDefault.Name, out var sequence, targetSchema) || sequence is null)
            sequence = Schema.Db.AddSequence(
                sequenceDefault.Name,
                sequenceDefault.StartValue,
                sequenceDefault.IncrementBy,
                sequenceDefault.CurrentValue,
                schemaName: targetSchema);

        return sequence.NextValue();
    }

    private void UpdateNextIdentityFromExplicitValue(object? explicitValue)
    {
        if (explicitValue is null)
            return;

        if (!TryConvertIdentityValue(explicitValue, out var numericValue))
            return;

        if (numericValue >= NextIdentity)
            NextIdentity = numericValue + 1;
    }

    private static bool TryConvertIdentityValue(object value, out int numericValue)
    {
        try
        {
            numericValue = Convert.ToInt32(value, CultureInfo.InvariantCulture);
            return true;
        }
        catch
        {
            numericValue = default;
            return false;
        }
    }

    private void RefreshPersistedComputedValues(object?[] row)
    {
        foreach (var col in _columnsByOrdinal)
        {
            if (col.GetGenValue == null || !col.PersistComputedValue)
                continue;

            row[col.Index] = col.GetGenValue(new ArrayRow(row), this);
        }
    }

    private bool HasPersistedComputedColumns()
    {
        if (_hasPersistedComputedColumnsInitialized)
            return _hasPersistedComputedColumns;

        _hasPersistedComputedColumns = false;
        foreach (var col in _columnsByOrdinal)
        {
            if (col.GetGenValue != null && col.PersistComputedValue)
            {
                _hasPersistedComputedColumns = true;
                break;
            }
        }

        _hasPersistedComputedColumnsInitialized = true;
        return _hasPersistedComputedColumns;
    }

    internal static string? ResolveWhereRaw(
        string? whereRaw,
        string? rawSql)
    {
        var w = whereRaw;
        if (string.IsNullOrWhiteSpace(w) && !string.IsNullOrWhiteSpace(rawSql))
        {
            w = TryExtractWhereRaw(rawSql!);
        }

        return w;
    }

    internal static List<(string C, string Op, string V)> ParseWhereSimple(string? w)
    {
        var list = new List<(string C, string Op, string V)>();
        if (string.IsNullOrWhiteSpace(w)) return list;

        var span = w.AsSpan().Trim();
        if (span.StartsWith("WHERE ", StringComparison.OrdinalIgnoreCase))
            span = span[6..].TrimStart();

        var partStart = 0;
        while (partStart < span.Length)
        {
            var andIndex = IndexOfAndSeparator(span, partStart);
            var partEnd = andIndex >= 0 ? andIndex : span.Length;
            var part = span[partStart..partEnd].Trim();
            if (part.Length > 0 && TryParseSimpleWherePart(part, out var condition))
                list.Add(condition);

            if (andIndex < 0)
                break;

            partStart = andIndex + 3;
            while (partStart < span.Length && char.IsWhiteSpace(span[partStart]))
                partStart++;
        }

        return list;
    }

    private static int IndexOfAndSeparator(ReadOnlySpan<char> span, int start)
    {
        var parenDepth = 0;
        var inSingleQuote = false;
        var inDoubleQuote = false;

        for (var i = start + 1; i + 2 < span.Length; i++)
        {
            var ch = span[i];

            if (inSingleQuote)
            {
                if (ch == '\'')
                {
                    if (i + 1 < span.Length && span[i + 1] == '\'')
                    {
                        i++;
                        continue;
                    }

                    inSingleQuote = false;
                }

                continue;
            }

            if (inDoubleQuote)
            {
                if (ch == '"')
                    inDoubleQuote = false;

                continue;
            }

            switch (ch)
            {
                case '\'':
                    inSingleQuote = true;
                    continue;
                case '"':
                    inDoubleQuote = true;
                    continue;
                case '(':
                    parenDepth++;
                    continue;
                case ')':
                    if (parenDepth > 0)
                        parenDepth--;
                    continue;
            }

            if (parenDepth > 0)
                continue;

            if (!IsAndToken(span, i))
                continue;

            if (i == 0 || !char.IsWhiteSpace(span[i - 1]))
                continue;

            var afterToken = i + 3;
            if (afterToken >= span.Length || !char.IsWhiteSpace(span[afterToken]))
                continue;

            return i;
        }

        return -1;
    }

    private static bool IsAndToken(ReadOnlySpan<char> span, int index)
        => (span[index] == 'A' || span[index] == 'a')
            && (span[index + 1] == 'N' || span[index + 1] == 'n')
            && (span[index + 2] == 'D' || span[index + 2] == 'd');

    private static bool TryParseSimpleWherePart(
        ReadOnlySpan<char> part,
        out (string C, string Op, string V) condition)
    {
        condition = default;

        var pos = 0;
        while (pos < part.Length && char.IsWhiteSpace(part[pos]))
            pos++;

        var columnStart = pos;
        while (pos < part.Length)
        {
            var ch = part[pos];
            if (char.IsWhiteSpace(ch) || ch is '<' or '>' or '=' or '!')
                break;

            pos++;
        }

        if (pos <= columnStart)
            return false;

        var column = part[columnStart..pos].Trim();
        if (column.Length == 0)
            return false;

        var rest = part[pos..].TrimStart();
        if (rest.Length == 0)
            return false;

        if (TryParseInClause(rest, out var inValue))
        {
            condition = (column.ToString(), SqlConst.IN, inValue);
            return true;
        }

        if (TryParseComparisonClause(rest, out var opLength))
        {
            var value = rest[opLength..].TrimStart();
            if (value.Length == 0)
                return false;

            condition = (column.ToString(), rest[..opLength].ToString(), value.ToString());
            return true;
        }

        return false;
    }

    private static bool TryParseInClause(ReadOnlySpan<char> rest, out string value)
    {
        value = string.Empty;
        if (rest.Length < 2)
            return false;

        if (!((rest[0] == 'I' || rest[0] == 'i') && (rest[1] == 'N' || rest[1] == 'n')))
            return false;

        if (rest.Length > 2 && !char.IsWhiteSpace(rest[2]) && rest[2] != '(')
            return false;

        value = rest[2..].TrimStart().ToString();
        return value.Length > 0;
    }

    private static bool TryParseComparisonClause(ReadOnlySpan<char> rest, out int opLength)
    {
        opLength = 0;
        if (rest.Length == 0)
            return false;

        var first = rest[0];
        if (first == '=')
        {
            opLength = 1;
            return true;
        }

        if (first == '<' || first == '>')
        {
            if (rest.Length > 1 && (rest[1] == '=' || rest[1] == '>'))
                opLength = 2;
            else
                opLength = 1;

            return true;
        }

        if (first == '!' && rest.Length > 1 && rest[1] == '=')
        {
            opLength = 2;
            return true;
        }

        return false;
    }

    private static Dictionary<string, (string C, string Op, string V)> BuildEqualityConditionLookup(
        List<(string C, string Op, string V)> conditions,
        out int equalityCount)
    {
        var lookup = new Dictionary<string, (string C, string Op, string V)>(conditions.Count * 2, StringComparer.OrdinalIgnoreCase);
        equalityCount = 0;

        for (var i = 0; i < conditions.Count; i++)
        {
            var condition = conditions[i];
            if (condition.Op != "=")
                continue;

            equalityCount++;
            AddEqualityConditionLookup(lookup, condition.C.NormalizeName(), condition);

            var trimmed = condition.C.Trim('`', '"', '[', ']').NormalizeName();
            AddEqualityConditionLookup(lookup, trimmed, condition);
        }

        return lookup;
    }

    private static void AddEqualityConditionLookup(
        Dictionary<string, (string C, string Op, string V)> lookup,
        string key,
        (string C, string Op, string V) condition)
    {
        if (string.IsNullOrWhiteSpace(key))
            return;

        lookup.TryAdd(key, condition);
    }

    /// <summary>
    /// EN: Attempts to find a single row using PK shortcut when WHERE conditions match an exact PK equality.
    /// PT-br: Tenta encontrar uma unica linha usando atalho PK quando as condicoes WHERE correspondem a uma igualdade exata de PK.
    /// </summary>
    /// <param name="table">EN: Target table. PT-br: Tabela alvo.</param>
    /// <param name="context"></param>
    /// <param name="pars">EN: Query parameters. PT-br: Parametros da consulta.</param>
    /// <param name="conditions">EN: Parsed WHERE conditions. PT-br: Condicoes WHERE parseadas.</param>
    /// <param name="rowIndex">EN: Found row index. PT-br: Indice da linha encontrada.</param>
    /// <returns>EN: True if a single row was found via PK shortcut. PT-br: True se uma unica linha foi encontrada via atalho PK.</returns>
    internal static bool TryFindRowByPkConditions(
        ITableMock table,
        QueryExecutionContext? context,
        DbParameterCollection? pars,
        List<(string C, string Op, string V)> conditions,
        out int rowIndex)
    {
        rowIndex = -1;
        if (conditions.Count == 0 || table.PrimaryKeyIndexes.Count == 0)
            return false;

        if (TryFindSinglePkConditionShortcut(table, context, pars, conditions, out rowIndex))
            return true;

        var eqConditionsByName = BuildEqualityConditionLookup(conditions, out var eqConditionCount);
        if (eqConditionCount < table.PrimaryKeyIndexes.Count)
            return false;

        var exactPkEqualityOnly = eqConditionCount == table.PrimaryKeyIndexes.Count
            && eqConditionCount == conditions.Count;

        if (table is TableMock tableMock)
        {
            var pkIndexes = tableMock.PkIndexArray;
            if (pkIndexes.Length == 1)
            {
                if (!TryResolvePkConditionValue(pkIndexes[0], out var pkValue0))
                    return false;

                if (!tableMock.TryFindRowByPkValues(pkValue0, out rowIndex))
                    return false;

                if (exactPkEqualityOnly)
                    return true;

                var pkMatchedRow1 = table[rowIndex];
                return IsMatchSimple(table, context, pars, conditions, pkMatchedRow1);
            }

            if (pkIndexes.Length == 2)
            {
                if (!TryResolvePkConditionValue(pkIndexes[0], out var pkValue0)
                    || !TryResolvePkConditionValue(pkIndexes[1], out var pkValue1))
                {
                    return false;
                }

                if (!tableMock.TryFindRowByPkValues(pkValue0, pkValue1, out rowIndex))
                    return false;

                if (exactPkEqualityOnly)
                    return true;

                var pkMatchedRow2 = table[rowIndex];
                return IsMatchSimple(table, context, pars, conditions, pkMatchedRow2);
            }

            if (pkIndexes.Length == 3)
            {
                if (!TryResolvePkConditionValue(pkIndexes[0], out var pkValue0)
                    || !TryResolvePkConditionValue(pkIndexes[1], out var pkValue1)
                    || !TryResolvePkConditionValue(pkIndexes[2], out var pkValue2))
                {
                    return false;
                }

                if (!tableMock.TryFindRowByPkValues(pkValue0, pkValue1, pkValue2, out rowIndex))
                    return false;

                if (exactPkEqualityOnly)
                    return true;

                var pkMatchedRow3 = table[rowIndex];
                return IsMatchSimple(table, context, pars, conditions, pkMatchedRow3);
            }

            var pkValues = new object?[pkIndexes.Length];
            for (var i = 0; i < pkIndexes.Length; i++)
            {
                if (!TryResolvePkConditionValue(pkIndexes[i], out var pkValue))
                    return false;

                pkValues[i] = pkValue;
            }

            if (!tableMock.TryFindRowByPkValues(pkValues, out rowIndex))
                return false;

            if (exactPkEqualityOnly)
                return true;

            var pkMatchedRow4 = table[rowIndex];
            return IsMatchSimple(table, context, pars, conditions, pkMatchedRow4);

            bool TryResolvePkConditionValue(int pkIdx, out object? value)
            {
                var col = tableMock.GetColumnByIndex(pkIdx);
                if (!eqConditionsByName.TryGetValue(col.Name, out var matchingCond))
                {
                    value = null;
                    return false;
                }

                value = ResolveConditionValue(table, context, pars, matchingCond.V, col.DbType, col.Nullable);
                return true;
            }
        }

        var syntheticRow = new Dictionary<int, object?>(table.PrimaryKeyIndexes.Count);

        foreach (var pkIdx in table.PrimaryKeyIndexes)
        {
            var col = table is TableMock tableAsMock
                ? tableAsMock.GetColumnByIndex(pkIdx)
                : GetColumnByIndex(table, pkIdx);
            if (!eqConditionsByName.TryGetValue(col.Name, out var matchingCond))
                return false;

            syntheticRow[pkIdx] = ResolveConditionValue(table, context, pars, matchingCond.V, col.DbType, col.Nullable);
        }

        if (!table.TryFindRowByPk(syntheticRow, out rowIndex))
            return false;

        if (exactPkEqualityOnly)
            return true;

        var syntheticMatchedRow = table[rowIndex];
        return IsMatchSimple(table, context, pars, conditions, syntheticMatchedRow);
    }

    internal static bool TryFindRowByPkConditions(
        ITableMock table,
        DbParameterCollection? pars,
        List<(string C, string Op, string V)> conditions,
        out int rowIndex)
    {
        rowIndex = -1;
        if (conditions.Count == 0 || table.PrimaryKeyIndexes.Count == 0)
            return false;

        if (TryFindSinglePkConditionShortcut(table, null, pars, conditions, out rowIndex))
            return true;

        var eqConditionsByName = BuildEqualityConditionLookup(conditions, out var eqConditionCount);
        if (eqConditionCount < table.PrimaryKeyIndexes.Count)
            return false;

        var exactPkEqualityOnly = eqConditionCount == table.PrimaryKeyIndexes.Count
            && eqConditionCount == conditions.Count;

        if (table is TableMock tableMock)
        {
            var pkIndexes = tableMock.PkIndexArray;
            if (pkIndexes.Length == 1)
            {
                if (!TryResolvePkConditionValue(pkIndexes[0], out var pkValue0))
                    return false;

                if (!tableMock.TryFindRowByPkValues(pkValue0, out rowIndex))
                    return false;

                if (exactPkEqualityOnly)
                    return true;

                var pkMatchedRow1 = table[rowIndex];
                return IsMatchSimple(table, pars, conditions, pkMatchedRow1);
            }

            if (pkIndexes.Length == 2)
            {
                if (!TryResolvePkConditionValue(pkIndexes[0], out var pkValue0)
                    || !TryResolvePkConditionValue(pkIndexes[1], out var pkValue1))
                {
                    return false;
                }

                if (!tableMock.TryFindRowByPkValues(pkValue0, pkValue1, out rowIndex))
                    return false;

                if (exactPkEqualityOnly)
                    return true;

                var pkMatchedRow2 = table[rowIndex];
                return IsMatchSimple(table, pars, conditions, pkMatchedRow2);
            }

            if (pkIndexes.Length == 3)
            {
                if (!TryResolvePkConditionValue(pkIndexes[0], out var pkValue0)
                    || !TryResolvePkConditionValue(pkIndexes[1], out var pkValue1)
                    || !TryResolvePkConditionValue(pkIndexes[2], out var pkValue2))
                {
                    return false;
                }

                if (!tableMock.TryFindRowByPkValues(pkValue0, pkValue1, pkValue2, out rowIndex))
                    return false;

                if (exactPkEqualityOnly)
                    return true;

                var pkMatchedRow3 = table[rowIndex];
                return IsMatchSimple(table, pars, conditions, pkMatchedRow3);
            }

            var pkValues = new object?[pkIndexes.Length];
            for (var i = 0; i < pkIndexes.Length; i++)
            {
                if (!TryResolvePkConditionValue(pkIndexes[i], out var pkValue))
                    return false;

                pkValues[i] = pkValue;
            }

            if (!tableMock.TryFindRowByPkValues(pkValues, out rowIndex))
                return false;

            if (exactPkEqualityOnly)
                return true;

            var pkMatchedRow4 = table[rowIndex];
            return IsMatchSimple(table, pars, conditions, pkMatchedRow4);

            bool TryResolvePkConditionValue(int pkIdx, out object? value)
            {
                var col = tableMock.GetColumnByIndex(pkIdx);
                if (!eqConditionsByName.TryGetValue(col.Name, out var matchingCond))
                {
                    value = null;
                    return false;
                }

                table.CurrentColumn = matchingCond.C;
                try
                {
                    var resolved = table.Resolve(matchingCond.V, col.DbType, col.Nullable, pars, table.Columns);
                    value = resolved is DBNull ? null : resolved;
                    return true;
                }
                finally
                {
                    table.CurrentColumn = null;
                }
            }
        }

        var syntheticRow = new Dictionary<int, object?>(table.PrimaryKeyIndexes.Count);

        foreach (var pkIdx in table.PrimaryKeyIndexes)
        {
            var col = table is TableMock tableAsMock
                ? tableAsMock.GetColumnByIndex(pkIdx)
                : GetColumnByIndex(table, pkIdx);
            if (!eqConditionsByName.TryGetValue(col.Name, out var matchingCond))
                return false;

            table.CurrentColumn = matchingCond.C;
            var resolved = table.Resolve(matchingCond.V, col.DbType, col.Nullable, pars, table.Columns);
            table.CurrentColumn = null;
            syntheticRow[pkIdx] = resolved is DBNull ? null : resolved;
        }

        if (!table.TryFindRowByPk(syntheticRow, out rowIndex))
            return false;

        if (exactPkEqualityOnly)
            return true;

        var syntheticMatchedRow = table[rowIndex];
        return IsMatchSimple(table, pars, conditions, syntheticMatchedRow);
    }

    private static bool TryFindSinglePkConditionShortcut(
        ITableMock table,
        QueryExecutionContext? context,
        DbParameterCollection? pars,
        List<(string C, string Op, string V)> conditions,
        out int rowIndex)
    {
        rowIndex = -1;
        if (conditions.Count != 1 || table.PrimaryKeyIndexes.Count != 1)
            return false;

        var condition = conditions[0];
        if (condition.Op != "=")
            return false;

        if (table is TableMock tableMock)
        {
            if (tableMock.PkIndexArray.Length != 1)
                return false;

            var pkIdx1 = tableMock.PkIndexArray[0];
            var pkColumn1 = tableMock.GetColumnByIndex(pkIdx1);
            if (!string.Equals(condition.C.NormalizeName(), pkColumn1.Name.NormalizeName(), StringComparison.OrdinalIgnoreCase))
                return false;

            var pkValue1 = ResolveConditionValue(table, context, pars, condition.V, pkColumn1.DbType, pkColumn1.Nullable);
            return tableMock.TryFindRowByPkValues(pkValue1, out rowIndex);
        }

        var pkIdx = -1;
        foreach (var index in table.PrimaryKeyIndexes)
        {
            pkIdx = index;
            break;
        }

        if (pkIdx < 0)
            return false;

        var pkColumn = GetColumnByIndex(table, pkIdx);
        if (!string.Equals(condition.C.NormalizeName(), pkColumn.Name.NormalizeName(), StringComparison.OrdinalIgnoreCase))
            return false;

        var pkValue = ResolveConditionValue(table, context, pars, condition.V, pkColumn.DbType, pkColumn.Nullable);
        var syntheticRow = new Dictionary<int, object?>(1)
        {
            [pkIdx] = pkValue
        };

        return table.TryFindRowByPk(syntheticRow, out rowIndex);
    }

    private static ColumnDef GetColumnByIndex(ITableMock table, int index)
    {
        foreach (var column in table.Columns.Values)
        {
            if (column.Index == index)
                return column;
        }

        throw new InvalidOperationException("Column index not found.");
    }

    internal static bool IsMatchSimple(
        ITableMock table,
        QueryExecutionContext? context,
        DbParameterCollection? pars,
        List<(string C, string Op, string V)> conditions,
        IReadOnlyDictionary<int, object?> row)
    {
        foreach (var cond in conditions)
        {
            var info = table.GetColumn(cond.C);
            var actual = info.GetGenValue != null ? info.GetGenValue(row, table) : row[info.Index];

            if (cond.Op.Equals(SqlConst.IN, StringComparison.OrdinalIgnoreCase))
            {
                var rhs = cond.V.Trim();

                var candidates = rhs.StartsWith('(')
                    && rhs.EndsWith(')')
                    ? GetCandidatesFromSub(table, pars, cond, info, rhs)
                    : GetCanditateFromTable(table, pars, cond, info, rhs);

                var matched = false;
                foreach (var cand in candidates)
                {
                    if (ValuesEqual(actual, cand))
                    {
                        matched = true;
                        break;
                    }
                }

                if (!matched)
                    return false;

                continue;
            }

            if (!TryMatchComparison(table, context, pars, cond, info, actual, out var value)
                || !value)
            {
                return false;
            }
        }

        return true;
    }

    private static bool TryMatchComparison(
        ITableMock table,
        QueryExecutionContext? context,
        DbParameterCollection? pars,
        (string C, string Op, string V) cond,
        ColumnDef info, object? actual,
        out bool value)
    {
        value = default;

        switch (cond.Op)
        {
            case "=":
            case "<>":
            case "!=":
            case "<":
            case "<=":
            case ">":
            case ">=":
                table.CurrentColumn = cond.C;
                var exp = ResolveConditionValue(table, context, pars, cond.V, info.DbType, info.Nullable);
                table.CurrentColumn = null;

                if (cond.Op == "=")
                {
                    value = ValuesEqual(actual, exp);
                    return true;
                }

                if (cond.Op is "<>" or "!=")
                {
                    value = !ValuesEqual(actual, exp);
                    return true;
                }

                value = CompareSimple(actual, exp, cond.Op);
                return true;
            default:
                return false;
        }
    }

    private static object? ResolveConditionValue(
        ITableMock table,
        QueryExecutionContext? context,
        DbParameterCollection? pars,
        string token,
        DbType dbType,
        bool isNullable)
    {
        var trimmed = token.Trim();
        if (context is not null && context.TryResolveParameter(trimmed, out var contextualValue))
            return contextualValue;

        var resolved = table.Resolve(trimmed, dbType, isNullable, pars, table.Columns);
        return resolved is DBNull ? null : resolved;
    }

    internal static bool IsMatchSimple(
        ITableMock table,
        DbParameterCollection? pars,
        List<(string C, string Op, string V)> conditions,
        IReadOnlyDictionary<int, object?> row)
    {
        foreach (var cond in conditions)
        {
            var info = table.GetColumn(cond.C);
            var actual = info.GetGenValue != null ? info.GetGenValue(row, table) : row[info.Index];

            if (cond.Op.Equals(SqlConst.IN, StringComparison.OrdinalIgnoreCase))
            {
                var rhs = cond.V.Trim();

                var candidates = rhs.StartsWith('(')
                    && rhs.EndsWith(')')
                    ? GetCandidatesFromSub(table, pars, cond, info, rhs)
                    : GetCanditateFromTable(table, pars, cond, info, rhs);

                var matched = false;
                foreach (var cand in candidates)
                {
                    if (ValuesEqual(actual, cand))
                    {
                        matched = true;
                        break;
                    }
                }

                if (!matched)
                    return false;

                continue;
            }

            if (!TryMatchComparison(table, pars, cond, info, actual, out var value)
                || !value)
            {
                return false;
            }
        }

        return true;
    }

    private static bool TryMatchComparison(
        ITableMock table,
        DbParameterCollection? pars,
        (string C, string Op, string V) cond,
        ColumnDef info, object? actual,
        out bool value)
    {
        value = default;

        switch (cond.Op)
        {
            case "=":
            case "<>":
            case "!=":
            case "<":
            case "<=":
            case ">":
            case ">=":
                table.CurrentColumn = cond.C;
                var exp = table.Resolve(cond.V, info.DbType, info.Nullable, pars, table.Columns);
                table.CurrentColumn = null;
                exp = exp is DBNull ? null : exp;

                if (cond.Op == "=")
                {
                    value = ValuesEqual(actual, exp);
                    return true;
                }

                if (cond.Op is "<>" or "!=")
                {
                    value = !ValuesEqual(actual, exp);
                    return true;
                }

                value = CompareSimple(actual, exp, cond.Op);
                return true;
            default:
                return false;
        }
    }

    private static bool CompareSimple(object? actual, object? expected, string op)
    {
        if (actual is null || expected is null)
            return false;

        if (TryConvertToDecimal(actual, out var left) && TryConvertToDecimal(expected, out var right))
        {
            var comparison = left.CompareTo(right);
            return op switch
            {
                "<" => comparison < 0,
                "<=" => comparison <= 0,
                ">" => comparison > 0,
                ">=" => comparison >= 0,
                _ => false
            };
        }

        var comparisonText = StringComparer.OrdinalIgnoreCase.Compare(
            Convert.ToString(actual, CultureInfo.InvariantCulture),
            Convert.ToString(expected, CultureInfo.InvariantCulture));
        return op switch
        {
            "<" => comparisonText < 0,
            "<=" => comparisonText <= 0,
            ">" => comparisonText > 0,
            ">=" => comparisonText >= 0,
            _ => false
        };
    }

    private static bool TryConvertToDecimal(object value, out decimal result)
    {
        if (value is DateTime dt)
        {
            result = dt.Ticks;
            return true;
        }

        if (value is DateTimeOffset dto)
        {
            result = dto.Ticks;
            return true;
        }

        if (value is TimeSpan ts)
        {
            result = ts.Ticks;
            return true;
        }

        if (value is bool boolValue)
        {
            result = boolValue ? 1m : 0m;
            return true;
        }

        if (value is IConvertible)
        {
            try
            {
                result = Convert.ToDecimal(value, CultureInfo.InvariantCulture);
                return true;
            }
            catch
            {
                // fallback to text parsing below
            }
        }

        return decimal.TryParse(value.ToString(), NumberStyles.Any, CultureInfo.InvariantCulture, out result);
    }

    private static bool ValuesEqual(object? left, object? right)
    {
        if (ReferenceEquals(left, right))
            return true;

        if (left is null || left is DBNull)
            return right is null || right is DBNull;

        if (right is null || right is DBNull)
            return false;

        if (TryConvertToDecimal(left, out var leftNumber)
            && TryConvertToDecimal(right, out var rightNumber))
        {
            return leftNumber == rightNumber;
        }

        if (left is string leftText && right is string rightText)
            return string.Equals(leftText, rightText, StringComparison.OrdinalIgnoreCase);

        if (left is byte[] leftBytes && right is byte[] rightBytes)
            return leftBytes.AsSpan().SequenceEqual(rightBytes);

        return Equals(left, right);
    }

    private static IEnumerable<object?> GetCanditateFromTable(
        ITableMock table,
        DbParameterCollection? pars,
        (string C, string Op, string V) cond,
        ColumnDef info,
        string rhs)
    {
        table.CurrentColumn = cond.C;
        var resolved = table.Resolve(rhs, info.DbType, info.Nullable, pars, table.Columns);
        table.CurrentColumn = null;

        resolved = resolved is DBNull ? null : resolved;

        if (!(resolved is IEnumerable ie and not string))
            return [resolved];

        var tmp = resolved is ICollection collection
            ? new List<object?>(collection.Count)
            : new List<object?>();
        foreach (var v in ie) tmp.Add(v);
        return tmp;
    }

    private static List<object?> GetCandidatesFromSub(
        ITableMock table,
        DbParameterCollection? pars,
        (string C, string Op, string V) cond,
        ColumnDef info,
        string rhs)
    {
        var inner = rhs[1..^1].Trim();

        if (Regex.IsMatch(inner, @"^(SELECT|WITH)\b", RegexOptions.IgnoreCase))
            return ResolveInSubqueryCandidates(table, info, inner, pars);

        var parts = inner.Split(',');

        var tmp = new List<object?>(parts.Length);
        foreach (var part in parts)
        {
            var trimmedPart = part.Trim();
            table.CurrentColumn = cond.C;
            var val = table.Resolve(trimmedPart, info.DbType, info.Nullable, pars, table.Columns);
            table.CurrentColumn = null;
            val = val is DBNull ? null : val;

            if (val is not IEnumerable ie || val is string)
            {
                tmp.Add(val);
                continue;
            }
            foreach (var v in ie) tmp.Add(v);
        }
        return tmp;
    }

    private static List<object?> ResolveInSubqueryCandidates(
        ITableMock table,
        ColumnDef targetInfo,
        string subquerySql,
        DbParameterCollection? pars)
    {
        var m = Regex.Match(
            subquerySql,
            @"^SELECT\s+(?<col>[A-Za-z0-9_`\.]+)\s+FROM\s+`?(?<table>[A-Za-z0-9_]+)`?(?:\s+(?:AS\s+)?(?<alias>[A-Za-z0-9_]+))?(?:\s+WHERE\s+(?<where>[\s\S]+))?$",
            RegexOptions.IgnoreCase | RegexOptions.Singleline);

        if (!m.Success)
            return [];

        var sourceTableName = m.Groups["table"].Value.NormalizeName();
        if (!table.Schema.TryGetTable(sourceTableName, out var sourceTableObj) || sourceTableObj == null)
            return [];

        var sourceTable = sourceTableObj;
        var sourceColName = m.Groups["col"].Value.Trim().Trim('`');
        var dot = sourceColName.LastIndexOf('.');
        if (dot >= 0 && dot + 1 < sourceColName.Length)
            sourceColName = sourceColName[(dot + 1)..];

        var sourceCol = sourceTable.GetColumn(sourceColName);
        var whereRaw = m.Groups["where"].Success ? m.Groups["where"].Value.Trim() : null;
        var whereConds = ParseWhereSimple(whereRaw);
        return GetParsedObject(targetInfo, pars, sourceTable, sourceCol, whereConds);
    }

    private static List<object?> GetParsedObject(
        ColumnDef targetInfo,
        DbParameterCollection? pars,
        ITableMock sourceTable,
        ColumnDef sourceCol,
        List<(string C, string Op, string V)> whereConds)
    {
        if (whereConds.Count > 0
            && TryFindRowByPkConditions(sourceTable, pars, whereConds, out var rowIndex))
        {
            return [ConvertParsedObjectValue(targetInfo, sourceTable, sourceCol, sourceTable[rowIndex])];
        }

        var tmp = new List<object?>();
        foreach (var row in sourceTable)
        {
            if (whereConds.Count > 0 && !IsMatchSimple(sourceTable, pars, whereConds, row))
                continue;

            tmp.Add(ConvertParsedObjectValue(targetInfo, sourceTable, sourceCol, row));
        }

        return tmp;
    }

    private static object? ConvertParsedObjectValue(
        ColumnDef targetInfo,
        ITableMock sourceTable,
        ColumnDef sourceCol,
        IReadOnlyDictionary<int, object?> row)
    {
        var value = sourceCol.GetGenValue != null ? sourceCol.GetGenValue(row, sourceTable) : row[sourceCol.Index];
        if (value is DBNull)
            value = null;

        if (value != null)
        {
            try
            {
                var text = value as string ?? Convert.ToString(value, CultureInfo.InvariantCulture);
                if (text is not null)
                    value = DbTypeParser.Parse(targetInfo.DbType, text);
            }
            catch
            {
                // best effort: keep original value when coercion is not possible
            }
        }

        return value;
    }

    private static string? TryExtractWhereRaw(string sql)
    {
        var norm = sql.NormalizeString();
        var whereIdx = norm.IndexOf(" WHERE ", StringComparison.OrdinalIgnoreCase);
        if (whereIdx < 0)
        {
            whereIdx = norm.IndexOf("WHERE ", StringComparison.OrdinalIgnoreCase);
            if (whereIdx < 0) return null;
        }

        var w = norm[(whereIdx + (norm[whereIdx] == ' ' ? 7 : 6))..];
        var stops = new[] { " ORDER ", " LIMIT ", " OFFSET ", " FETCH ", ";" };
        var cut = w.Length;
        foreach (var stop in stops)
        {
            var idx = w.IndexOf(stop, StringComparison.OrdinalIgnoreCase);
            if (idx >= 0) cut = Math.Min(cut, idx);
        }
        return w[..cut].Trim();
    }

    /// <summary>
    /// EN: Implements RemoveAt.
    /// PT-br: Implementa RemoveAt.
    /// </summary>
    public Dictionary<int, object?> RemoveAt(int idx)
    {
        var arr = RemoveAtArray(idx);
        var dict = new Dictionary<int, object?>(arr.Length);
        for (var i = 0; i < arr.Length; i++)
            dict[i] = arr[i];
        return dict;
    }

    internal object?[] RemoveAtArray(int idx)
    {
        var hasMutationApplied = MutationApplied is not null;
        var it = _items[idx];
        Schema.ValidateForeignKeysOnDelete(TableName, this, [new ArrayRow(it)]);
        _indexManager.RemoveRowFromIndexes(idx, new ArrayRow(it));
        _indexManager.RemovePrimaryKey(idx, new ArrayRow(it));
        _items.RemoveAt(idx);
        _indexManager.ShiftIndexPositionsAfterDelete(idx);
        if (hasMutationApplied)
            NotifyMutationApplied(TableMutationKind.Delete, idx, it);
        return it;
    }

    /// <summary>
    /// EN: Implements UpdateRowColumn.
    /// PT-br: Implementa UpdateRowColumn.
    /// </summary>
    public void UpdateRowColumn(
        int rowIdx,
        int colIdx,
        object? value)
    {
        var hasMutationApplied = MutationApplied is not null;
        var row = _items[rowIdx];
        var oldRow = hasMutationApplied ? CloneRow(new ArrayRow(row)) : null;
        var oldValue = row[colIdx];
        row[colIdx] = value;
        RefreshPersistedComputedValues(row);
        _indexManager.UpdatePrimaryKeyIfNeeded(rowIdx, colIdx, oldValue, new ArrayRow(row));
        if (hasMutationApplied)
            NotifyMutationApplied(TableMutationKind.Update, rowIdx, row, oldRow);
    }

    internal int FindRowIndexByReference(object?[] row)
        => _stateManager.FindRowIndexByReference(row);

    internal void RemoveRowByReference(object?[] row)
        => _stateManager.RemoveRowByReference(row);

    internal void InsertRestoredRow(int rowIndex, object?[] row)
        => _stateManager.InsertRestoredRow(rowIndex, row);

    internal void RestoreRowSnapshot(
        object?[] targetRow,
        IReadOnlyDictionary<int, object?> snapshot)
        => _stateManager.RestoreRowSnapshot(targetRow, snapshot);

    internal void RestoreIndexesAfterJournalReplay()
        => _stateManager.RestoreIndexesAfterJournalReplay();

    internal int FindRowIndexByReferenceCore(object?[] row)
    {
        for (var i = 0; i < _items.Count; i++)
        {
            if (ReferenceEquals(_items[i], row))
                return i;
        }

        return -1;
    }

    internal void RemoveRowByReferenceCore(int rowIndex)
    {
        if (rowIndex >= 0 && rowIndex < _items.Count)
            _items.RemoveAt(rowIndex);
    }

    internal void InsertRestoredRowCore(int rowIndex, object?[] row)
        => _items.Insert(rowIndex, row);

    internal void ClearRowsCore() => _items.Clear();

    /// <summary>
    /// EN: Backs up current rows.
    /// PT-br: Faz backup das linhas atuais.
    /// </summary>
    public void Backup()
        => _stateManager.Backup();

    /// <summary>
    /// EN: Restores the previous backup, if any.
    /// PT-br: Restaura o backup anterior, se existir.
    /// </summary>
    public void Restore()
        => _stateManager.Restore();

    /// <summary>
    /// EN: Clears the stored backup.
    /// PT-br: Limpa o backup armazenado.
    /// </summary>
    public void ClearBackup() => _stateManager.ClearBackup();

    internal static object?[] CloneRow(IReadOnlyDictionary<int, object?> row)
    {
        var clone = new object?[row.Count];
        foreach (var entry in row)
            clone[entry.Key] = entry.Value;

        return clone;
    }

    internal static object?[] CloneRow(object?[] row)
    {
        var clone = new object?[row.Length];
        Array.Copy(row, clone, row.Length);
        return clone;
    }

    /// <summary>
    /// EN: Gets or sets the column currently being evaluated.
    /// PT-br: Obtém ou define a coluna atualmente em avaliação.
    /// </summary>
    public abstract string? CurrentColumn { get; set; }

    /// <summary>
    /// EN: Gets or sets Count.
    /// PT-br: Obtém ou define Count.
    /// </summary>
    public int Count => _items.Count;

    /// <summary>
    /// EN: Gets or sets an item in this collection.
    /// PT-br: Obtém ou define um item desta coleção.
    /// </summary>
    public IReadOnlyDictionary<int, object?> this[int index] => new ArrayRow(_items[index]);

    /// <summary>
    /// EN: Resolves a token to a value in the table context.
    /// PT-br: ResolveRowsFrameRange um token para um valor no contexto da tabela.
    /// </summary>
    /// <param name="token">EN: Token to resolve. PT-br: Token a resolver.</param>
    /// <param name="dbType">EN: Expected type. PT-br: Tipo esperado.</param>
    /// <param name="isNullable">EN: Whether the value can be null. PT-br: Se o valor pode ser nulo.</param>
    /// <param name="pars">EN: Query parameters. PT-br: Parâmetros de consulta.</param>
    /// <param name="colDict">EN: Optional column dictionary. PT-br: Dicionário de colunas opcional.</param>
    /// <returns>EN: Resolved value. PT-br: Valor resolvido.</returns>
    public abstract object? Resolve(
        string token,
        DbType dbType,
        bool isNullable,
        IDataParameterCollection? pars = null,
        IReadOnlyDictionary<string, ColumnDef>? colDict = null);

    /// <summary>
    /// EN: Creates an exception for an unknown column.
    /// PT-br: Cria exceção para coluna inexistente.
    /// </summary>
    /// <param name="columnName">EN: Column name. PT-br: Nome da coluna.</param>
    public abstract Exception UnknownColumn(string columnName);
    /// <summary>
    /// EN: Creates an exception for a duplicate key.
    /// PT-br: Cria exceção para chave duplicada.
    /// </summary>
    /// <param name="tbl">EN: Affected table. PT-br: Tabela afetada.</param>
    /// <param name="key">EN: Key name. PT-br: Nome da chave.</param>
    /// <param name="val">EN: Duplicate value. PT-br: Valor duplicado.</param>
    public abstract Exception DuplicateKey(string tbl, string key, object? val);
    /// <summary>
    /// EN: Creates an exception for a non-nullable column.
    /// PT-br: Cria exceção para coluna que não aceita nulos.
    /// </summary>
    /// <param name="col">EN: Column name. PT-br: Nome da coluna.</param>
    public abstract Exception ColumnCannotBeNull(string col);
    /// <summary>
    /// EN: Creates an exception for a foreign key violation.
    /// PT-br: Cria exceção para violação de chave estrangeira.
    /// </summary>
    /// <param name="col">EN: Referencing column. PT-br: Coluna que referencia.</param>
    /// <param name="refTbl">EN: Referenced table. PT-br: Tabela referenciada.</param>
    public abstract Exception ForeignKeyFails(string col, string refTbl);
    /// <summary>
    /// EN: Creates an exception for deleting a referenced row.
    /// PT-br: Cria exceção para tentativa de remover linha referenciada.
    /// </summary>
    /// <param name="tbl">EN: Referenced table. PT-br: Tabela referenciada.</param>
    public abstract Exception ReferencedRow(string tbl);

    /// <summary>
    /// EN: Implements GetEnumerator.
    /// PT-br: Implementa GetEnumerator.
    /// </summary>
    public IEnumerator<IReadOnlyDictionary<int, object?>> GetEnumerator()
    {
        foreach (var row in _items)
            yield return new ArrayRow(row);
    }

    IEnumerator IEnumerable.GetEnumerator()
        => GetEnumerator();

}
