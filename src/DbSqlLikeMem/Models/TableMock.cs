using System.Collections.ObjectModel;
using System.Collections.Concurrent;
using System.Globalization;
using System.Linq.Expressions;
using System.Text.RegularExpressions;

namespace DbSqlLikeMem;

/// <summary>
/// EN: Base for an in-memory table with data, columns, and _indexes.
/// PT: Base de uma tabela em memória com dados, colunas e índices.
/// </summary>
public abstract class TableMock
    : ITableMock
{
    private const string PRIMARY = SqlConst.PRIMARY;
    private static readonly ConcurrentDictionary<Type, IReadOnlyDictionary<string, Func<object, object?>>> _itemAccessorCache = new();

    /// <summary>
    /// EN: Initializes the table with name, schema, and columns, with optional rows.
    /// PT: Inicializa a tabela com nome, schema e colunas, com linhas opcionais.
    /// </summary>
    /// <param name="tableName">EN: Table name. PT: Nome da tabela.</param>
    /// <param name="schema">EN: Parent schema. PT: Schema pai.</param>
    /// <param name="columns">EN: Table columns. PT: Colunas da tabela.</param>
    /// <param name="rows">EN: Initial rows. PT: Linhas iniciais.</param>
    protected TableMock(
        string tableName,
        SchemaMock schema,
        IEnumerable<Col> columns,
        IEnumerable<Dictionary<int, object?>>? rows = null)
    {
        TableName = tableName.NormalizeName();
        Schema = schema;
        foreach (var c in columns)
            AddColumn(c);
        AddRange(rows ?? []);
    }

    /// <summary>
    /// EN: Normalized table name.
    /// PT: Nome normalizado da tabela.
    /// </summary>
    public string TableName { get; }

    /// <summary>
    /// EN: Schema to which the table belongs.
    /// PT: Schema ao qual a tabela pertence.
    /// </summary>
    public SchemaMock Schema { get; }

    /// <summary>
    /// EN: Next identity value for auto-increment columns.
    /// PT: Próximo valor de identidade para colunas auto incrementais.
    /// </summary>
    public int NextIdentity { get; set; } = 1;

    /// <summary>
    /// EN: Enables explicit values for identity columns when building scenarios or executing inserts.
    /// PT: Habilita valores explícitos para colunas identity ao montar cenários ou executar inserções.
    /// </summary>
    public bool AllowIdentityInsert { get; set; }

    /// <summary>
    /// EN: Partitioning clause captured from CREATE TABLE when the provider persists partition metadata.
    /// PT: Clausula de particionamento capturada do CREATE TABLE quando o provedor persiste metadados de particionamento.
    /// </summary>
    public string? PartitionClauseSql { get; set; }

    private PartitionRoutingInfo? _partitionRoutingInfo;

    internal bool MatchesRequestedPartitions(
        IReadOnlyDictionary<int, object?> row,
        IReadOnlyCollection<string> requestedPartitionNames)
    {
        if (requestedPartitionNames.Count == 0 || string.IsNullOrWhiteSpace(PartitionClauseSql))
            return true;

        var routingInfo = GetPartitionRoutingInfo();
        if (routingInfo is null)
            return true;

        return routingInfo.TryGetPartitionName(row, this, out var partitionName)
            && requestedPartitionNames.Any(name => string.Equals(name, partitionName, StringComparison.OrdinalIgnoreCase));
    }

    internal bool TryGetPartitionName(
        IReadOnlyDictionary<int, object?> row,
        out string partitionName)
    {
        partitionName = string.Empty;
        if (string.IsNullOrWhiteSpace(PartitionClauseSql))
            return false;

        var routingInfo = GetPartitionRoutingInfo();
        if (routingInfo is null)
            return false;

        return routingInfo.TryGetPartitionName(row, this, out partitionName);
    }

    internal bool TryInferRequestedPartitionNames(
        IReadOnlyDictionary<string, object?> equalsByColumn,
        out IReadOnlyList<string> partitionNames)
    {
        partitionNames = [];
        if (string.IsNullOrWhiteSpace(PartitionClauseSql))
            return false;

        var routingInfo = GetPartitionRoutingInfo();
        if (routingInfo is null)
            return false;

        if (!equalsByColumn.TryGetValue(routingInfo.PartitionedColumnName, out var rawValue))
            return false;

        return TryInferRequestedPartitionNames([rawValue], out partitionNames);
    }

    internal bool TryInferRequestedPartitionNames(
        IEnumerable<object?> rawValues,
        out IReadOnlyList<string> partitionNames)
    {
        partitionNames = [];
        if (string.IsNullOrWhiteSpace(PartitionClauseSql))
            return false;

        var routingInfo = GetPartitionRoutingInfo();
        if (routingInfo is null)
            return false;

        var distinctPartitionNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var hasValue = false;
        foreach (var rawValue in rawValues)
        {
            hasValue = true;
            if (!TryGetPartitionNameForValue(rawValue, out var partitionName))
                return false;

            distinctPartitionNames.Add(partitionName);
        }

        if (!hasValue || distinctPartitionNames.Count == 0)
            return false;

        partitionNames = [.. distinctPartitionNames];
        return true;
    }

    internal bool TryInferRequestedPartitionNamesForRange(
        object? lowValue,
        object? highValue,
        out IReadOnlyList<string> partitionNames)
    {
        partitionNames = [];
        if (string.IsNullOrWhiteSpace(PartitionClauseSql))
            return false;

        if (!TryGetYearForPartitionValue(lowValue, out var lowYear)
            || !TryGetYearForPartitionValue(highValue, out var highYear))
        {
            return false;
        }

        if (lowYear > highYear)
            return false;

        var span = highYear - lowYear;
        if (span > 32)
            return false;

        var distinctPartitionNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        for (var year = lowYear; year <= highYear; year++)
        {
            if (!TryGetPartitionNameForValue(year, out var partitionName))
                return false;

            distinctPartitionNames.Add(partitionName);
        }

        if (distinctPartitionNames.Count == 0)
            return false;

        partitionNames = [.. distinctPartitionNames];
        return true;
    }

    internal bool TryInferRequestedPartitionNamesForRanges(
        IEnumerable<(object? Low, object? High)> ranges,
        out IReadOnlyList<string> partitionNames)
    {
        partitionNames = [];
        if (string.IsNullOrWhiteSpace(PartitionClauseSql))
            return false;

        var distinctPartitionNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var hasRange = false;
        foreach (var (low, high) in ranges)
        {
            hasRange = true;
            if (!TryInferRequestedPartitionNamesForRange(low, high, out var rangePartitionNames))
                return false;

            foreach (var partitionName in rangePartitionNames)
                distinctPartitionNames.Add(partitionName);
        }

        if (!hasRange || distinctPartitionNames.Count == 0)
            return false;

        partitionNames = [.. distinctPartitionNames];
        return true;
    }

    internal bool TryGetPartitionNameForValue(
        object? rawValue,
        out string partitionName)
    {
        partitionName = string.Empty;
        if (string.IsNullOrWhiteSpace(PartitionClauseSql))
            return false;

        var routingInfo = GetPartitionRoutingInfo();
        if (routingInfo is null)
            return false;

        if (rawValue is null || rawValue is DBNull)
            return false;

        if (!_columns.TryGetValue(routingInfo.PartitionedColumnName, out var partitionedColumn))
            return false;

        var probeRow = new Dictionary<int, object?>(1)
        {
            [partitionedColumn.Index] = rawValue
        };

        return routingInfo.TryGetPartitionName(probeRow, this, out partitionName);
    }

    private static bool TryGetYearForPartitionValue(object? rawValue, out int year)
    {
        switch (rawValue)
        {
            case DateTime dateTime:
                year = dateTime.Year;
                return true;
            case DateTimeOffset dateTimeOffset:
                year = dateTimeOffset.Year;
                return true;
            case int intValue:
                year = intValue;
                return true;
            case long longValue when longValue >= int.MinValue && longValue <= int.MaxValue:
                year = (int)longValue;
                return true;
            case string text when DateTime.TryParse(text, CultureInfo.InvariantCulture, DateTimeStyles.AllowWhiteSpaces, out var parsedDate):
                year = parsedDate.Year;
                return true;
            default:
                year = default;
                return false;
        }
    }

    internal bool TryGetPartitionedColumnName(out string partitionedColumnName)
    {
        partitionedColumnName = string.Empty;
        if (string.IsNullOrWhiteSpace(PartitionClauseSql))
            return false;

        var routingInfo = GetPartitionRoutingInfo();
        if (routingInfo is null)
            return false;

        partitionedColumnName = routingInfo.PartitionedColumnName;
        return true;
    }

    private PartitionRoutingInfo? GetPartitionRoutingInfo()
    {
        if (_partitionRoutingInfo is not null)
            return _partitionRoutingInfo;

        if (string.IsNullOrWhiteSpace(PartitionClauseSql))
            return null;

        _partitionRoutingInfo = PartitionRoutingInfo.TryParse(PartitionClauseSql!);
        return _partitionRoutingInfo;
    }

    /// <summary>
    /// EN: Sets the AllowIdentityInsert property.
    /// PT: Define a propriedade AllowIdentityInsert.
    /// </summary>
    /// <param name="allowIdentityInsert">
    /// EN: Enables explicit values for identity columns when building scenarios or executing inserts.
    /// PT: Habilita valores explícitos para colunas identity ao montar cenários ou executar inserções.
    /// </param>
    /// <returns></returns>
    public ITableMock SetAllowIdentityInsert(bool allowIdentityInsert)
    {
        AllowIdentityInsert = allowIdentityInsert;
        return this;
    }

    private readonly ColumnDictionary _columns = [];
    private readonly Dictionary<int, string> _columnsByIndex = [];

    /// <summary>
    /// EN: Table column dictionary.
    /// PT: Dicionário de colunas da tabela.
    /// </summary>
    public IReadOnlyDictionary<string, ColumnDef> Columns
        => new ReadOnlyDictionary<string, ColumnDef>(_columns);

    internal IReadOnlyDictionary<int, string> ColumnsByIndex => _columnsByIndex;

    private readonly List<Dictionary<int, object?>> _items = [];

    public IReadOnlyList<IReadOnlyDictionary<int, object?>> Items => new ItemsView(_items);

    private sealed class ItemsView(List<Dictionary<int, object?>> items) : IReadOnlyList<IReadOnlyDictionary<int, object?>>
    {
        public int Count => items.Count;
        public IReadOnlyDictionary<int, object?> this[int index] => items[index];
        public IEnumerator<IReadOnlyDictionary<int, object?>> GetEnumerator() => items.GetEnumerator();
        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();
    }

    internal HashSet<int> _primaryKeyIndexes = [];
    private int[] _pkIndexArray = [];

    /// <summary>
    /// EN: Fast lookup dictionary mapping serialized PK values to row positions.
    /// PT: Dicionário rápido que mapeia valores de PK serializados para posições de linha.
    /// </summary>
    private readonly Dictionary<IndexKey, int> _pkIndex = new();

    // ---------- Wave D : índices ---------------------------------
    /// <summary>
    /// EN: Indexes of columns that form the primary key.
    /// PT: Índices das colunas que formam a chave primária.
    /// </summary>
    public IReadOnlyHashSet<int> PrimaryKeyIndexes => new ReadOnlyHashSet<int>(_primaryKeyIndexes);

    public void AddPrimaryKeyIndexes(params string[] columns)
    {
        foreach (var colName in columns)
            _primaryKeyIndexes.Add(Columns[colName].Index);
        if (_primaryKeyIndexes.Count != columns.Length)
            throw new InvalidOperationException(SqlExceptionMessages.DuplicatePrimaryKeyColumns());
        _pkIndexArray = [.. _primaryKeyIndexes.OrderBy(static i => i)];
        RebuildPkIndex();
    }

    /// <summary>
    /// EN: Builds a composite key string from primary key column values of a row.
    /// PT: Constrói uma string de chave composta a partir dos valores das colunas PK de uma linha.
    /// </summary>
    internal IndexKey BuildPkKey(IReadOnlyDictionary<int, object?> row)
    {
        if (_pkIndexArray.Length == 0)
            return default;

        if (_pkIndexArray.Length == 1)
        {
            var pk0 = _pkIndexArray[0];
            return new IndexKey(row.TryGetValue(pk0, out var v) ? v : null);
        }

        if (_pkIndexArray.Length == 2)
        {
            var pk0 = _pkIndexArray[0];
            var pk1 = _pkIndexArray[1];
            var v0 = row.TryGetValue(pk0, out var vv0) ? vv0 : null;
            var v1 = row.TryGetValue(pk1, out var vv1) ? vv1 : null;
            return new IndexKey(v0, v1);
        }

        if (_pkIndexArray.Length == 3)
        {
            var pk0 = _pkIndexArray[0];
            var pk1 = _pkIndexArray[1];
            var pk2 = _pkIndexArray[2];
            var v0 = row.TryGetValue(pk0, out var vv0) ? vv0 : null;
            var v1 = row.TryGetValue(pk1, out var vv1) ? vv1 : null;
            var v2 = row.TryGetValue(pk2, out var vv2) ? vv2 : null;
            return new IndexKey(v0, v1, v2);
        }

        var values = new object?[_pkIndexArray.Length];
        for (int i = 0; i < _pkIndexArray.Length; i++)
        {
            values[i] = row.TryGetValue(_pkIndexArray[i], out var v) ? v : null;
        }
        return new IndexKey(values);
    }

    /// <summary>
    /// EN: Tries to find a row by its primary key using the fast PK index.
    /// PT: Tenta encontrar uma linha pela chave primária usando o índice PK rápido.
    /// </summary>
    /// <param name="row">EN: Row containing PK values. PT: Linha contendo valores de PK.</param>
    /// <param name="rowIndex">EN: Found row index. PT: Índice da linha encontrada.</param>
    /// <returns>EN: True if a matching row was found. PT: True se uma linha correspondente foi encontrada.</returns>
    public bool TryFindRowByPk(IReadOnlyDictionary<int, object?> row, out int rowIndex)
    {
        rowIndex = -1;
        if (_primaryKeyIndexes.Count == 0)
            return false;

        var key = BuildPkKey(row);
        return _pkIndex.TryGetValue(key, out rowIndex);
    }

    /// <summary>
    /// EN: Rebuilds the PK index from all current rows.
    /// PT: Reconstrói o índice PK a partir de todas as linhas atuais.
    /// </summary>
    private void RebuildPkIndex()
    {
        _pkIndex.Clear();
        if (_primaryKeyIndexes.Count == 0)
            return;

        for (int i = 0; i < _items.Count; i++)
        {
            var key = BuildPkKey(_items[i]);
            _pkIndex[key] = i;
        }
    }

    private readonly Dictionary<string, ForeignDef> _foreignKeys = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// EN: List of foreign keys defined in the table.
    /// PT: Lista de chaves estrangeiras definidas na tabela.
    /// </summary>
    public IReadOnlyDictionary<string, ForeignDef> ForeignKeys => _foreignKeys;

    internal IndexDictionary _indexes = [];

    /// <summary>
    /// EN: Indexes declared on the table.
    /// PT: Índices declarados na tabela.
    /// </summary>
    public IReadOnlyDictionary<string, IndexDef> Indexes
        => new ReadOnlyDictionary<string, IndexDef>(_indexes);

    private readonly Dictionary<TableTriggerEvent, List<Action<TableTriggerContext>>> _triggers = [];
    private readonly Dictionary<string, (TableTriggerEvent Event, Action<TableTriggerContext> Handler)> _namedTriggers = new(StringComparer.OrdinalIgnoreCase);
    internal event Action<TableMutationNotification>? MutationApplied;

    /// <summary>
    /// EN: Registers a trigger callback for the specified table event.
    /// PT: Registra um callback de trigger para o evento de tabela especificado.
    /// </summary>
    /// <param name="evt">EN: Trigger event. PT: Evento da trigger.</param>
    /// <param name="handler">EN: Callback handler. PT: Manipulador de callback.</param>
    public void AddTrigger(TableTriggerEvent evt, Action<TableTriggerContext> handler)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(handler, nameof(handler));
        if (!_triggers.TryGetValue(evt, out var handlers))
        {
            handlers = [];
            _triggers[evt] = handlers;
        }

        handlers.Add(handler);
    }

    internal void AddOrReplaceTrigger(
        string triggerName,
        TableTriggerEvent evt,
        Action<TableTriggerContext> handler,
        bool orReplace = false)
    {
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(triggerName, nameof(triggerName));
        ArgumentNullExceptionCompatible.ThrowIfNull(handler, nameof(handler));

        var normalizedName = triggerName.NormalizeName();
        if (_namedTriggers.TryGetValue(normalizedName, out var previous))
        {
            if (!orReplace)
                throw new InvalidOperationException($"Trigger '{normalizedName}' already exists.");

            if (_triggers.TryGetValue(previous.Event, out var previousHandlers))
            {
                previousHandlers.Remove(previous.Handler);
                if (previousHandlers.Count == 0)
                    _triggers.Remove(previous.Event);
            }
        }

        AddTrigger(evt, handler);
        _namedTriggers[normalizedName] = (evt, handler);
    }

    internal bool RemoveTrigger(string triggerName)
    {
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(triggerName, nameof(triggerName));

        var normalizedName = triggerName.NormalizeName();
        if (!_namedTriggers.TryGetValue(normalizedName, out var previous))
            return false;

        if (_triggers.TryGetValue(previous.Event, out var previousHandlers))
        {
            previousHandlers.Remove(previous.Handler);
            if (previousHandlers.Count == 0)
                _triggers.Remove(previous.Event);
        }

        _namedTriggers.Remove(normalizedName);
        return true;
    }

    internal void ExecuteTriggers(
        TableTriggerEvent evt,
        IReadOnlyDictionary<int, object?>? oldRow,
        IReadOnlyDictionary<int, object?>? newRow)
    {
        if (!_triggers.TryGetValue(evt, out var handlers) || handlers.Count == 0)
            return;

        var context = new TableTriggerContext(this, oldRow, newRow);
        foreach (var handler in handlers)
            handler(context);
    }

    /// <inheritdoc />
    public bool HasTriggers(TableTriggerEvent evt)
        => _triggers.TryGetValue(evt, out var handlers) && handlers.Count > 0;

    /// <summary>
    /// EN: Creates a detachable row snapshot without allocating a dictionary.
    /// PT: Cria um snapshot de linha sem alocar um dicionário.
    /// </summary>
    internal static IReadOnlyDictionary<int, object?> SnapshotRow(IReadOnlyDictionary<int, object?>? row)
    {
        var metrics = DbMetrics.Current;
        if (metrics is null)
            return LazyRowSnapshot.From(row);

        metrics.IncrementPerformancePhaseHit(DbPerformanceMetricKeys.RowSnapshot);
        var startedAt = Stopwatch.GetTimestamp();
        try
        {
            return LazyRowSnapshot.From(row);
        }
        finally
        {
            metrics.IncrementPerformancePhaseElapsedTicks(
                DbPerformanceMetricKeys.RowSnapshot,
                StopwatchCompatible.GetElapsedTicks(startedAt));
        }
    }

    private void NotifyMutationApplied(
        TableMutationKind kind,
        int rowIndex,
        Dictionary<int, object?> row,
        Dictionary<int, object?>? oldRowSnapshot = null,
        int previousNextIdentity = 0)
        => MutationApplied?.Invoke(new TableMutationNotification(
            this,
            kind,
            rowIndex,
            row,
            oldRowSnapshot,
            previousNextIdentity));

    internal bool HasRegisteredTriggers()
        => _triggers.Values.Any(static handlers => handlers.Count > 0);

    /// <summary>
    /// EN: Add new Vollumn to Table
    /// PT: Incluir nova coluna na tabela
    /// </summary>
    /// <param name="name"></param>
    /// <param name="dbType"></param>
    /// <param name="nullable"></param>
    /// <param name="size"></param>
    /// <param name="decimalPlaces"></param>
    /// <param name="identity"></param>
    /// <param name="defaultValue"></param>
    /// <param name="enumValues"></param>
    /// <returns></returns>
    public ColumnDef AddColumn(
        string name,
        DbType dbType,
        bool nullable,
        int? size = null,
        int? decimalPlaces = null,
        bool identity = false,
        object? defaultValue = null,
        IList<string>? enumValues = null)
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
            enumValues: enumValues);

        _columns.Add(normalizedName, col);
        _columnsByIndex[col.Index] = normalizedName;
        BackfillAddedColumn(col);
        return col;
    }

    /// <summary>
    /// EN: Add new Vollumn to Table
    /// PT: Incluir nova coluna na tabela
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
            column.enumValues);

    private void BackfillAddedColumn(ColumnDef column)
    {
        if (_items.Count == 0)
            return;

        foreach (var row in _items)
        {
            object? value = column.DefaultValue;

            if (column.Identity)
                value = NextIdentity++;

            row[column.Index] = value;

            if (column.GetGenValue != null && column.PersistComputedValue)
                row[column.Index] = column.GetGenValue(new ReadOnlyDictionary<int, object?>(row), this);

            if (!column.Nullable && row[column.Index] == null)
                throw ColumnCannotBeNull(column.Name);
        }
    }

    /// <summary>
    /// EN: Returns the ColumnDef for <paramref name="columnName"/> or throws UnknownColumn.
    /// PT: Retorna o ColumnDef para <paramref name="columnName"/>
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
    /// PT: Cria e registra um índice na tabela.
    /// </summary>
    public IndexDef CreateIndex(
        string name,
        IEnumerable<string> keyCols,
        string[]? include = null,
        bool unique = false)
    {
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(name, nameof(name));
        ArgumentNullExceptionCompatible.ThrowIfNull(keyCols, nameof(keyCols));
        name = name.NormalizeName();
        if (_indexes.ContainsKey(name))
            throw new InvalidOperationException(SqlExceptionMessages.IndexAlreadyExists(name));
        var normalizedKeyCols = keyCols
            .Select(static col => col.NormalizeName())
            .ToList();
        if (normalizedKeyCols.Count != normalizedKeyCols.Distinct(StringComparer.OrdinalIgnoreCase).Count())
            throw new InvalidOperationException($"Index '{name}' cannot contain duplicate key columns.");

        foreach (var keyColumn in normalizedKeyCols)
            GetColumn(keyColumn);

        List<string>? normalizedIncludeCols = null;
        if (include is not null)
        {
            normalizedIncludeCols = [.. include.Select(static col => col.NormalizeName())];

            if (normalizedIncludeCols.Count != normalizedIncludeCols.Distinct(StringComparer.OrdinalIgnoreCase).Count())
                throw new InvalidOperationException($"Index '{name}' cannot contain duplicate include columns.");

            if (normalizedIncludeCols.Any(includeCol => normalizedKeyCols.Contains(includeCol, StringComparer.OrdinalIgnoreCase)))
                throw new InvalidOperationException($"Index '{name}' cannot include key columns redundantly.");

            foreach (var includeColumn in normalizedIncludeCols)
                GetColumn(includeColumn);
        }

        var idx = new IndexDef(this, name, normalizedKeyCols, normalizedIncludeCols?.ToArray(), unique);
        _indexes.Add(name, idx);
        return idx;
    }

    internal void DropIndex(
        string name,
        bool ifExists = false)
    {
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(name, nameof(name));
        name = name.NormalizeName();

        if (_indexes.Remove(name))
            return;

        if (ifExists)
            return;

        throw new InvalidOperationException($"Index '{name}' does not exist.");
    }

    /// <summary>
    /// EN: Looks up values in the index using the given key.
    /// PT: Procura valores no índice usando a chave informada.
    /// </summary>
    /// <param name="def">EN: Index definition. PT: Definição do índice.</param>
    /// <param name="key">EN: Key to search. PT: Chave a buscar.</param>
    /// <returns>EN: List of positions or null. PT: Lista de posições ou null.</returns>
    public IReadOnlyDictionary<int, IReadOnlyDictionary<string, object?>>? Lookup(
        IndexDef def,
        IndexKey key)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(def, nameof(def));
        if (!_indexes.TryGetValue(def.Name.NormalizeName(), out var map))
            return null;

        return map.Lookup(key);
    }

    /// <summary>
    /// EN: Updates _indexes after inserting or changing a row.
    /// PT: Atualiza os índices após inserir ou alterar uma linha.
    /// </summary>
    /// <param name="rowIdx">EN: Changed row index. PT: Índice da linha alterada.</param>
    /// <param name="row">EN: Row to remove. PT: Linha a remover.</param>
    internal void RemoveRowFromIndexes(int rowIdx, IReadOnlyDictionary<int, object?> row)
    {
        foreach (var idx in _indexes.Values)
        {
            idx.RemoveRow(rowIdx, row);
        }
    }

    internal void ShiftIndexPositionsAfterDelete(int deletedIdx)
    {
        foreach (var idx in _indexes.Values)
        {
            idx.ShiftPositionsAfter(deletedIdx);
        }
    }

    public void UpdateIndexesWithRow(int rowIdx)
    {
        foreach (var it in _indexes)
            it.Value.UpdateIndexesWithRow(rowIdx, this[rowIdx]);
    }

    internal void UpdateIndexesWithRow(
        int rowIdx,
        IReadOnlyDictionary<int, object?>? oldRow,
        IReadOnlyDictionary<int, object?> newRow)
    {
        foreach (var it in _indexes)
            it.Value.UpdateIndexesWithRow(rowIdx, oldRow, newRow);
    }

    /// <summary>
    /// EN: Rebuilds all table _indexes.
    /// PT: Reconstrói todos os índices da tabela.
    /// </summary>
    public void RebuildAllIndexes()
    {
        if (_indexes.Count <= 1 || !Schema.Db.ThreadSafe)
        {
            foreach (var ix in _indexes)
                ix.Value.RebuildIndex();
            RebuildPkIndex();
            return;
        }

        Parallel.ForEach(_indexes.Values, ix => ix.RebuildIndex());
        RebuildPkIndex();
    }

    internal void MarkAllIndexesDirty()
    {
        foreach (var ix in _indexes.Values)
            ix.MarkDirty();
    }


    /// <summary>
    /// EN: Implements CreateForeignKey.
    /// PT: Implementa CreateForeignKey.
    /// </summary>
    public ForeignDef CreateForeignKey(
        string name,
        string refTable,
        HashSet<(string col, string refCol)> references)
    {
        var tbRef = ResolveReferencedTable(refTable);
        var fk = new ForeignDef(
            this,
            name,
            tbRef,
            [.. references.Select(_ => (col: Columns[_.col], refCol: tbRef.Columns[_.refCol]))]
            );

        _foreignKeys.Add(name, fk);
        return fk;
    }

    private ITableMock ResolveReferencedTable(string refTable)
    {
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(refTable, nameof(refTable));

        var separatorIndex = refTable.IndexOf('.');
        if (separatorIndex <= 0 || separatorIndex == refTable.Length - 1)
            return Schema[refTable];

        var schemaName = refTable[..separatorIndex].NormalizeName();
        var tableName = refTable[(separatorIndex + 1)..].NormalizeName();
        return Schema.Db.GetTable(tableName, schemaName);
    }

    internal void ValidateForeignKeysOnRow(IReadOnlyDictionary<int, object?> row)
    {
        foreach (var fk in _foreignKeys.Values)
        {
            var hasNull = false;
            foreach (var (col, _) in fk.References)
            {
                if (!row.TryGetValue(col.Index, out var val)
                    || val is null
                    || val is DBNull)
                {
                    hasNull = true;
                    break;
                }
            }

            if (hasNull)
                continue;

            if (!HasReferencedRow(fk, row))
            {
                var refCols = string.Join(",", fk.References.Select(_ => _.col.Name));
                throw ForeignKeyFails(refCols, fk.RefTable.TableName);
            }
        }
    }

    private bool HasReferencedRow(
        ForeignDef fk,
        IReadOnlyDictionary<int, object?> row)
    {
        var refTable = fk.RefTable;
        var matchingIndex = refTable.Indexes.Values
            .OrderByDescending(_ => _.KeyCols.Count)
            .FirstOrDefault(ix => fk.References.All(r =>
                ix.KeyCols.Contains(r.refCol.Name, StringComparer.OrdinalIgnoreCase)));

        if (matchingIndex is not null)
        {
            IndexKey key;
            if (fk.References.Count == 1)
            {
                var r = fk.References.First();
                key = new IndexKey(row[r.col.Index]);
            }
            else
            {
                var valuesByColumn = fk.References.ToDictionary(
                    _ => _.refCol.Name.NormalizeName(),
                    _ => row[_.col.Index],
                    StringComparer.OrdinalIgnoreCase);
                key = matchingIndex.BuildIndexKeyFromValues(valuesByColumn);
            }

            if (matchingIndex.LookupMutable(key)?.Count > 0)
                return true;
        }

        if (Schema.Db.ThreadSafe && refTable.Count >= 2048)
        {
            return refTable.AsParallel().Any(refRow => fk.References.All(r =>
                Equals(refRow[r.refCol.Index], row[r.col.Index])));
        }

        return refTable.Any(refRow => fk.References.All(r =>
            Equals(refRow[r.refCol.Index], row[r.col.Index])));
    }

    /// <summary>
    /// EN: Adds multiple items by converting them into rows.
    /// PT: Adiciona vários itens convertendo-os em linhas.
    /// </summary>
    /// <typeparam name="T">EN: Item type. PT: Tipo dos itens.</typeparam>
    /// <param name="items">EN: Items to insert. PT: Itens a inserir.</param>
    public ITableMock AddRangeItems<T>(IEnumerable<T> items)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(items, nameof(items));
        var rows = items.Select(MaterializeItem).ToList();
        return AddBatch(rows);
    }

    /// <summary>
    /// EN: Adds already materialized rows.
    /// PT: Adiciona linhas já materializadas.
    /// </summary>
    /// <param name="items">EN: Rows to insert. PT: Linhas a inserir.</param>
    public ITableMock AddRange(IEnumerable<Dictionary<int, object?>> items)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(items, nameof(items));
        if (items is IReadOnlyList<Dictionary<int, object?>> materializedRows)
            return AddBatch(materializedRows);

        return AddBatch([.. items]);
    }

    /// <summary>
    /// EN: Adds an item by converting it into a table row.
    /// PT: Adiciona um item convertendo-o em linha da tabela.
    /// </summary>
    /// <typeparam name="T">EN: Item type. PT: Tipo do item.</typeparam>
    /// <param name="item">EN: Item to insert. PT: Item a inserir.</param>
    public ITableMock AddItem<T>(T item)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(item, nameof(item));
        Add(MaterializeItem(item));
        return this;
    }

    private Dictionary<int, object?> MaterializeItem<T>(T item)
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

    private Dictionary<int, object?> MaterializeItemCore<T>(T item)
    {
        var row = new Dictionary<int, object?>();
        var accessors = GetItemAccessors(typeof(T));

        foreach (var p in Columns)
        {
            if (!accessors.TryGetValue(p.Key, out var getter))
            {
                row[p.Value.Index] = null;
                continue;
            }

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
    /// EN: Adds multiple rows in batch while validating uniqueness and updating indexes incrementally.
    /// PT: Adiciona multiplas linhas em lote validando unicidade e atualizando indices de forma incremental.
    /// </summary>
    /// <param name="values">EN: Rows to insert. PT: Linhas a inserir.</param>
    public ITableMock AddBatch(IReadOnlyList<Dictionary<int, object?>> values)
    {
        if (values.Count == 0)
            return this;

        if (values.Count == 1 || HasSelfReferencingForeignKey())
        {
            foreach (var value in values)
                Add(value);
            return this;
        }

        var uniqueIndexes = _indexes.GetUnique().ToArray();
        var allIndexes = _indexes.Values.ToArray();
        var batchPrimaryKeys = _primaryKeyIndexes.Count > 0
            ? new HashSet<IndexKey>()
            : null;
        
        var previousNextIdentities = new int[values.Count];
        var batchUniqueDicts = uniqueIndexes.ToDictionary(
            static index => index,
            static _ => new HashSet<IndexKey>());

        // Pre-calculate ALL keys for ALL indexes to reuse later
        var pkKeys = batchPrimaryKeys is null ? null : new IndexKey[values.Count];
        var indexKeysMatrix = new IndexKey[allIndexes.Length][];
        for (int i = 0; i < allIndexes.Length; i++) indexKeysMatrix[i] = new IndexKey[values.Count];

        for (var valueIndex = 0; valueIndex < values.Count; valueIndex++)
        {
            var row = values[valueIndex];
            previousNextIdentities[valueIndex] = NextIdentity;
            ApplyDefaultValues(row);
            RefreshPersistedComputedValues(row);
            ValidateForeignKeysOnRow(row);

            if (pkKeys is not null)
            {
                var pkKey = BuildPkKey(row);
                pkKeys[valueIndex] = pkKey;
                if (_pkIndex.ContainsKey(pkKey) || !batchPrimaryKeys!.Add(pkKey))
                {
                    var dupPk = _columns
                        .Where(_ => _primaryKeyIndexes.Contains(_.Value.Index))
                        .Select(_ => $"{_.Key}: {(row.TryGetValue(_.Value.Index, out var v) ? v : null)}");
                    throw DuplicateKey(TableName, PRIMARY, string.Join(",", dupPk));
                }
            }

            for(int i=0; i < allIndexes.Length; i++)
            {
                var index = allIndexes[i];
                var key = index.BuildIndexKey(row);
                indexKeysMatrix[i][valueIndex] = key;

                if (index.Unique)
                {
                    if (index.LookupMutable(key)?.Count > 0 || !batchUniqueDicts[index].Add(key))
                        throw DuplicateKey(TableName, index.Name, key);
                }
            }
        }

        var startIndex = _items.Count;
        _items.AddRange(values);
        
        // Update indexes using PRE-CALCULATED keys
        var hasPrimaryKey = _primaryKeyIndexes.Count > 0;
        for (var rowOffset = 0; rowOffset < values.Count; rowOffset++)
        {
            var rowIndex = startIndex + rowOffset;
            var row = values[rowOffset];

            for (int i = 0; i < allIndexes.Length; i++)
            {
                allIndexes[i].UpdateIndexesWithRow(rowIndex, row, indexKeysMatrix[i][rowOffset]);
            }

            if (hasPrimaryKey)
                _pkIndex[pkKeys![rowOffset]] = rowIndex;
        }

        for (var rowOffset = 0; rowOffset < values.Count; rowOffset++)
        {
            NotifyMutationApplied(
                TableMutationKind.Insert,
                startIndex + rowOffset,
                values[rowOffset],
                previousNextIdentity: previousNextIdentities[rowOffset]);
        }

        return this;
    }

    private bool HasSelfReferencingForeignKey()
        => _foreignKeys.Values.Any(fk => ReferenceEquals(fk.RefTable, this));

    /// <summary>
    /// EN: Adds a row ensuring default values and uniqueness.
    /// PT: Adiciona uma linha garantindo valores padrao e unicidade.
    /// </summary>
    /// <param name="value">EN: Row to insert. PT: Linha a inserir.</param>
    public ITableMock Add(Dictionary<int, object?> value)
    {
        var previousNextIdentity = NextIdentity;
        ApplyDefaultValues(value);
        RefreshPersistedComputedValues(value);
        ValidateForeignKeysOnRow(value);
        EnsureUniqueOnInsert(value);
        _items.Add(value);
        // Update _indexes with the new row
        int newIdx = Count - 1;
        UpdateIndexesWithRow(newIdx);
        // Update PK index
        if (_primaryKeyIndexes.Count > 0)
        {
            var pkKey = BuildPkKey(value);
            _pkIndex[pkKey] = newIdx;
        }
        NotifyMutationApplied(
            TableMutationKind.Insert,
            newIdx,
            value,
            previousNextIdentity: previousNextIdentity);
        return this;
    }

    internal void UpdateIndexesWithRows(
        int startIndex,
        IReadOnlyList<Dictionary<int, object?>> rows)
    {
        if (rows.Count == 0)
            return;

        var hasPrimaryKey = _primaryKeyIndexes.Count > 0;
        for (var rowOffset = 0; rowOffset < rows.Count; rowOffset++)
        {
            var rowIndex = startIndex + rowOffset;
            var row = rows[rowOffset];

            foreach (var index in _indexes.Values)
                index.UpdateIndexesWithRow(rowIndex, row);

            if (hasPrimaryKey)
                _pkIndex[BuildPkKey(row)] = rowIndex;
        }
    }

    private void ApplyDefaultValues(Dictionary<int, object?> value)
    {
        foreach (var it in Columns)
        {
            var col = it.Value;
            if (!value.ContainsKey(col.Index))
                value[col.Index] = null;

            if (!col.Identity)
            {
                if (col.DefaultValue != null && value[col.Index] == null)
                    value[col.Index] = col.DefaultValue;
            }
            else if (AllowIdentityInsert && value[col.Index] is not null)
                UpdateNextIdentityFromExplicitValue(value[col.Index]);
            else
                value[col.Index] = NextIdentity++;

            if (col.GetGenValue != null && col.PersistComputedValue)
                value[col.Index] = col.GetGenValue(value, this);

            if (!col.Nullable && value[col.Index] == null)
                throw ColumnCannotBeNull(it.Key);
        }
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

    private void RefreshPersistedComputedValues(IDictionary<int, object?> row)
    {
        foreach (var col in Columns.Values)
        {
            if (col.GetGenValue == null || !col.PersistComputedValue)
                continue;

            row[col.Index] = col.GetGenValue(new ReadOnlyDictionary<int, object?>(row), this);
        }
    }

    private bool TryFindPrimaryConflictByIndex(
        IReadOnlyDictionary<int, object?> newRow,
        out int rowIndex)
    {
        rowIndex = -1;
        if (_primaryKeyIndexes.Count == 0)
            return false;

        var pkColumnNames = _columns
            .Where(_ => _primaryKeyIndexes.Contains(_.Value.Index))
            .Select(_ => _.Key)
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

        var pkIndex = _indexes.GetUnique()
            .FirstOrDefault(ix =>
                ix.KeyCols.Count == pkColumnNames.Count
                && ix.KeyCols.All(pkColumnNames.Contains));

        if (pkIndex is null)
            return false;

        var valuesByColumn = _columns
            .Where(_ => _primaryKeyIndexes.Contains(_.Value.Index))
            .ToDictionary(
                _ => _.Key.NormalizeName(),
                _ => newRow.TryGetValue(_.Value.Index, out var val) ? val : null,
                StringComparer.OrdinalIgnoreCase);

        var key = pkIndex.BuildIndexKeyFromValues(valuesByColumn);
        var hits = pkIndex.LookupMutable(key);
        if (hits is not { Count: > 0 })
            return false;

        rowIndex = hits.First().Key;
        return true;
    }

    internal void EnsureUniqueOnInsert(Dictionary<int, object?> newRow)
    {
        CheckUniquePrimary(newRow);

        foreach (var idx in _indexes.GetUnique())
        {
            var key = idx.BuildIndexKey(newRow);
            var hits = idx.LookupMutable(key);
            if (hits?.Any() == true)
                throw DuplicateKey(TableName, idx.Name, key);
        }
    }

    private void CheckUniquePrimary(Dictionary<int, object?> newRow)
    {
        if (_primaryKeyIndexes.Count <= 0)
            return;

        // Fast path: use PK index dictionary for O(1) conflict detection
        if (_pkIndex.Count > 0)
        {
            var pkKey = BuildPkKey(newRow);
            if (_pkIndex.ContainsKey(pkKey))
            {
                var dupPk = _columns
                    .Where(_ => _primaryKeyIndexes.Contains(_.Value.Index))
                    .Select(_ => $"{_.Key}: {(newRow.TryGetValue(_.Value.Index, out var v) ? v : null)}");
                throw DuplicateKey(TableName, PRIMARY, string.Join(",", dupPk));
            }
            return;
        }

        // Fallback: use existing index-based detection
        if (TryFindPrimaryConflictByIndex(newRow, out _))
        {
            var dupPk = _columns
                .Where(_ => _primaryKeyIndexes.Contains(_.Value.Index))
                .Select(_ => $"{_.Key}: {(newRow.TryGetValue(_.Value.Index, out var v) ? v : null)}");
            throw DuplicateKey(TableName, PRIMARY, string.Join(",", dupPk));
        }

        var pkColumnNames = _columns.ToDictionary(_ => _.Value.Index, _ => _.Key);
        for (int i = 0; i < Count; i++)
        {
            var pks = new List<string>();
            foreach (var pkIdx in _primaryKeyIndexes)
            {
                if (newRow.TryGetValue(pkIdx, out var pkVal)
                    && Count > i
                    && this[i]?.TryGetValue(pkIdx, out var cur) == true
                    && Equals(cur, pkVal))
                {
                    pks.Add($"{pkColumnNames[pkIdx]}: {pkVal}");
                }
            }
            if (_primaryKeyIndexes.Count == pks.Count)
                throw DuplicateKey(TableName, PRIMARY, string.Join(",", pks));
        }
    }

    internal int? FindConflictingRowIndex(
        IReadOnlyDictionary<int, object?> newRow,
        out string? conflictIndexName,
        out object? conflictKey)
    {
        conflictIndexName = null;
        conflictKey = null;

        // Fast path: use PK dictionary for O(1) conflict detection
        if (_primaryKeyIndexes.Count > 0 && _pkIndex.Count > 0)
        {
            var pkKey = BuildPkKey(newRow);
            if (_pkIndex.TryGetValue(pkKey, out var pkConflict))
            {
                var dupPk = _columns
                    .Where(_ => _primaryKeyIndexes.Contains(_.Value.Index))
                    .Select(_ => $"{_.Key}: {(newRow.TryGetValue(_.Value.Index, out var v) ? v : null)}");
                conflictIndexName = PRIMARY;
                conflictKey = string.Join(",", dupPk);
                return pkConflict;
            }
        }

        if (TryFindPrimaryConflictByIndex(newRow, out var conflictByIndex))
        {
            var dupPk = _columns
                .Where(_ => _primaryKeyIndexes.Contains(_.Value.Index))
                .Select(_ => $"{_.Key}: {(newRow.TryGetValue(_.Value.Index, out var v) ? v : null)}");
            conflictIndexName = PRIMARY;
            conflictKey = string.Join(",", dupPk);
            return conflictByIndex;
        }

        if (!CheckPrimary(newRow, ref conflictIndexName, ref conflictKey, out var value))
            return value;

        foreach (var idx in _indexes.GetUnique())
        {
            var key = idx.BuildIndexKey(newRow);
            var hits = idx.LookupMutable(key);
            if (hits is { Count: > 0 })
            {
                conflictIndexName = idx.Name;
                conflictKey = key;
                return hits.First().Key;
            }
        }
        return null;
    }

    private bool CheckPrimary(
        IReadOnlyDictionary<int, object?> newRow,
        ref string? conflictIndexName,
        ref object? conflictKey,
        out int? value)
    {
        value = default;
        if (_primaryKeyIndexes.Count <= 0) return true;
        var pkColumnNames = _columns.ToDictionary(_ => _.Value.Index, _ => _.Key);
        for (int i = 0; i < Count; i++)
        {
            var pks = new List<string>();
            foreach (var pkIdx in _primaryKeyIndexes)
            {
                if (newRow.TryGetValue(pkIdx, out var pkVal)
                    && this[i].TryGetValue(pkIdx, out var cur)
                    && Equals(cur, pkVal))
                {
                    pks.Add($"{pkColumnNames[pkIdx]}: {pkVal}");
                }
            }
            if (_primaryKeyIndexes.Count != pks.Count)
                continue;

            conflictIndexName = PRIMARY;
            conflictKey = string.Join(",", pks);
            value = i;
            return false;
        }

        return true;
    }

    internal void EnsureUniqueBeforeUpdate(
        string tableName,
        IReadOnlyDictionary<int, object?> existingRow,
        IReadOnlyDictionary<int, object?> simulatedRow,
        int rowIdx,
        IReadOnlyCollection<string> changedCols)
    {
        foreach (var ix in _indexes.GetUnique())
            ix.EnsureUniqueBeforeUpdate(rowIdx, existingRow, simulatedRow, changedCols);

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

        w = w!.Trim();
        if (w.StartsWith("WHERE ", StringComparison.OrdinalIgnoreCase))
            w = w[6..].Trim();

        var parts = Regex.Split(w, @"\s+AND\s+", RegexOptions.IgnoreCase)
            .Where(p => !string.IsNullOrWhiteSpace(p));

        foreach (var p in parts)
        {
            var s = p.Trim();

            var min = Regex.Match(s, @"^(?<c>[\w`\.]+)\s+IN\s*(?<v>.+)$", RegexOptions.IgnoreCase);
            if (min.Success)
            {
                list.Add((min.Groups["c"].Value.Trim(), SqlConst.IN, min.Groups["v"].Value.Trim()));
                continue;
            }

            var comparison = Regex.Match(
                s,
                @"^(?<c>[\w`\.]+)\s*(?<op><=|>=|<>|!=|=|<|>)\s*(?<v>.+)$",
                RegexOptions.IgnoreCase);
            if (comparison.Success)
            {
                list.Add((
                    comparison.Groups["c"].Value.Trim(),
                    comparison.Groups["op"].Value.Trim(),
                    comparison.Groups["v"].Value.Trim()));
            }
        }

        return list;
    }

    /// <summary>
    /// EN: Attempts to find a single row using PK shortcut when WHERE conditions match an exact PK equality.
    /// PT: Tenta encontrar uma unica linha usando atalho PK quando as condicoes WHERE correspondem a uma igualdade exata de PK.
    /// </summary>
    /// <param name="table">EN: Target table. PT: Tabela alvo.</param>
    /// <param name="pars">EN: Query parameters. PT: Parametros da consulta.</param>
    /// <param name="conditions">EN: Parsed WHERE conditions. PT: Condicoes WHERE parseadas.</param>
    /// <param name="rowIndex">EN: Found row index. PT: Indice da linha encontrada.</param>
    /// <returns>EN: True if a single row was found via PK shortcut. PT: True se uma unica linha foi encontrada via atalho PK.</returns>
    internal static bool TryFindRowByPkConditions(
        ITableMock table,
        DbParameterCollection? pars,
        List<(string C, string Op, string V)> conditions,
        out int rowIndex)
    {
        rowIndex = -1;
        if (conditions.Count == 0 || table.PrimaryKeyIndexes.Count == 0)
            return false;

        var eqConditions = conditions.Where(c => c.Op == "=").ToList();
        if (eqConditions.Count < table.PrimaryKeyIndexes.Count)
            return false;

        var syntheticRow = new Dictionary<int, object?>();

        foreach (var pkIdx in table.PrimaryKeyIndexes)
        {
            var col = table.Columns.Values.First(c => c.Index == pkIdx);
            var matchingCond = eqConditions.FirstOrDefault(cond =>
                string.Equals(col.Name, cond.C.NormalizeName(), StringComparison.OrdinalIgnoreCase) ||
                string.Equals(col.Name, cond.C.Trim('`', '"', '[', ']').NormalizeName(), StringComparison.OrdinalIgnoreCase));

            if (matchingCond == default) return false;

            table.CurrentColumn = matchingCond.C;
            var resolved = table.Resolve(matchingCond.V, col.DbType, col.Nullable, pars, table.Columns);
            table.CurrentColumn = null;
            syntheticRow[pkIdx] = resolved is DBNull ? null : resolved;
        }

        if (!table.TryFindRowByPk(syntheticRow, out rowIndex))
            return false;

        var row = table[rowIndex];
        return IsMatchSimple(table, pars, conditions, row);
    }

    internal static bool IsMatchSimple(
        ITableMock table,
        DbParameterCollection? pars,
        List<(string C, string Op, string V)> conditions,
        IReadOnlyDictionary<int, object?> row)
    => conditions.All(cond =>
    {
        var info = table.GetColumn(cond.C);
        var actual = info.GetGenValue != null ? info.GetGenValue(row, table) : row[info.Index];

        if (TryMatchComparison(table, pars, cond, info, actual, out var value))
            return value;

        if (!cond.Op.Equals(SqlConst.IN, StringComparison.OrdinalIgnoreCase))
            return false;

        var rhs = cond.V.Trim();

        var candidates = rhs.StartsWith("(")
            && rhs.EndsWith(")")
            ? GetCandidatesFromSub(table, pars, cond, info, rhs)
            : GetCanditateFromTable(table, pars, cond, info, rhs);

        foreach (var cand in candidates)
        {
            if (Equals(actual, cand))
                return true;
        }

        return false;
    });

    private static bool TryMatchComparison(
        ITableMock table,
        DbParameterCollection? pars,
        (string C, string Op, string V) cond,
        ColumnDef info, object? actual,
        out bool value)
    {
        value = default;
        table.CurrentColumn = cond.C;
        var exp = table.Resolve(cond.V, info.DbType, info.Nullable, pars, table.Columns);
        table.CurrentColumn = null;
        exp = exp is DBNull ? null : exp;

        switch (cond.Op)
        {
            case "=":
                value = Equals(actual, exp);
                return true;
            case "<>":
            case "!=":
                value = !Equals(actual, exp);
                return true;
            case "<":
            case "<=":
            case ">":
            case ">=":
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
        => decimal.TryParse(
            Convert.ToString(value, CultureInfo.InvariantCulture),
            NumberStyles.Any,
            CultureInfo.InvariantCulture,
            out result);

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

        var tmp = new List<object?>();
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

        var parts = inner.Split(',')
            .Select(_ => _.Trim())
            .ToArray();

        var tmp = new List<object?>();
        foreach (var part in parts)
        {
            table.CurrentColumn = cond.C;
            var val = table.Resolve(part, info.DbType, info.Nullable, pars, table.Columns);
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
            @"^SELECT\s+(?<col>[A-Za-z0-9_`\.]+)\s+FROM\s+`?(?<table>[A-Za-z0-9_]+)`?(?:\s+WHERE\s+(?<where>[\s\S]+))?$",
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
                value = DbTypeParser.Parse(targetInfo.DbType, value.ToString()!);
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
    /// PT: Implementa RemoveAt.
    /// </summary>
    public Dictionary<int, object?> RemoveAt(int idx)
    {
        var it = _items[idx];
        RemoveRowFromIndexes(idx, it);
        // Remove from PK index and shift positions
        if (_primaryKeyIndexes.Count > 0)
        {
            var removedKey = BuildPkKey(it);
            _pkIndex.Remove(removedKey);
            // Shift positions for rows after the deleted one
            var keysToUpdate = _pkIndex.Where(kv => kv.Value > idx).Select(kv => kv.Key).ToList();
            foreach (var key in keysToUpdate)
                _pkIndex[key] = _pkIndex[key] - 1;
        }
        _items.RemoveAt(idx);
        ShiftIndexPositionsAfterDelete(idx);
        NotifyMutationApplied(TableMutationKind.Delete, idx, it);
        return it;
    }

    /// <summary>
    /// EN: Implements UpdateRowColumn.
    /// PT: Implementa UpdateRowColumn.
    /// </summary>
    public void UpdateRowColumn(
        int rowIdx,
        int colIdx,
        object? value)
    {
        var row = _items[rowIdx];
        var oldRow = row.ToDictionary(_ => _.Key, _ => _.Value);
        // If changing a PK column, update the PK index
        if (_primaryKeyIndexes.Count > 0 && _primaryKeyIndexes.Contains(colIdx))
        {
            var oldKey = BuildPkKey(row);
            _pkIndex.Remove(oldKey);
            row[colIdx] = value;
            RefreshPersistedComputedValues(row);
            var newKey = BuildPkKey(row);
            _pkIndex[newKey] = rowIdx;
        }
        else
        {
            row[colIdx] = value;
            RefreshPersistedComputedValues(row);
        }
        NotifyMutationApplied(TableMutationKind.Update, rowIdx, row, oldRow);
    }

    internal int FindRowIndexByReference(Dictionary<int, object?> row)
    {
        for (var i = 0; i < _items.Count; i++)
        {
            if (ReferenceEquals(_items[i], row))
                return i;
        }

        return -1;
    }

    internal void RemoveRowByReference(Dictionary<int, object?> row)
    {
        var rowIndex = FindRowIndexByReference(row);
        if (rowIndex >= 0)
            _items.RemoveAt(rowIndex);
    }

    internal void InsertRestoredRow(int rowIndex, Dictionary<int, object?> row)
        => _items.Insert(rowIndex, row);

    internal void RestoreRowSnapshot(
        Dictionary<int, object?> targetRow,
        IReadOnlyDictionary<int, object?> snapshot)
    {
        targetRow.Clear();
        foreach (var entry in snapshot)
            targetRow[entry.Key] = entry.Value;
    }

    internal void RestoreIndexesAfterJournalReplay()
    {
        RebuildPkIndex();
        MarkAllIndexesDirty();
    }

    private List<Dictionary<int, object?>>? _backup;

    /// <summary>
    /// EN: Backs up current rows.
    /// PT: Faz backup das linhas atuais.
    /// </summary>
    public void Backup() => _backup = [.. this.Select(row => row.ToDictionary(_ => _.Key, _ => _.Value))];

    /// <summary>
    /// EN: Restores the previous backup, if any.
    /// PT: Restaura o backup anterior, se existir.
    /// </summary>
    public void Restore()
    {
        if (_backup == null)
            return;

        _items.Clear();
        _pkIndex.Clear();
        foreach (var row in _backup)
            _items.Add(row.ToDictionary(_ => _.Key, _ => _.Value));

        RebuildPkIndex();
        MarkAllIndexesDirty();
    }

    /// <summary>
    /// EN: Clears the stored backup.
    /// PT: Limpa o backup armazenado.
    /// </summary>
    public void ClearBackup() => _backup = null;

    /// <summary>
    /// EN: Gets or sets the column currently being evaluated.
    /// PT: Obtém ou define a coluna atualmente em avaliação.
    /// </summary>
    public abstract string? CurrentColumn { get; set; }

    /// <summary>
    /// EN: Gets or sets Count.
    /// PT: Obtém ou define Count.
    /// </summary>
    public int Count => _items.Count;

    /// <summary>
    /// EN: Gets or sets an item in this collection.
    /// PT: Obtém ou define um item desta coleção.
    /// </summary>
    public IReadOnlyDictionary<int, object?> this[int index] => _items[index];

    /// <summary>
    /// EN: Resolves a token to a value in the table context.
    /// PT: Resolve um token para um valor no contexto da tabela.
    /// </summary>
    /// <param name="token">EN: Token to resolve. PT: Token a resolver.</param>
    /// <param name="dbType">EN: Expected type. PT: Tipo esperado.</param>
    /// <param name="isNullable">EN: Whether the value can be null. PT: Se o valor pode ser nulo.</param>
    /// <param name="pars">EN: Query parameters. PT: Parâmetros de consulta.</param>
    /// <param name="colDict">EN: Optional column dictionary. PT: Dicionário de colunas opcional.</param>
    /// <returns>EN: Resolved value. PT: Valor resolvido.</returns>
    public abstract object? Resolve(
        string token,
        DbType dbType,
        bool isNullable,
        IDataParameterCollection? pars = null,
        IReadOnlyDictionary<string, ColumnDef>? colDict = null);

    /// <summary>
    /// EN: Creates an exception for an unknown column.
    /// PT: Cria exceção para coluna inexistente.
    /// </summary>
    /// <param name="columnName">EN: Column name. PT: Nome da coluna.</param>
    public abstract Exception UnknownColumn(string columnName);
    /// <summary>
    /// EN: Creates an exception for a duplicate key.
    /// PT: Cria exceção para chave duplicada.
    /// </summary>
    /// <param name="tbl">EN: Affected table. PT: Tabela afetada.</param>
    /// <param name="key">EN: Key name. PT: Nome da chave.</param>
    /// <param name="val">EN: Duplicate value. PT: Valor duplicado.</param>
    public abstract Exception DuplicateKey(string tbl, string key, object? val);
    /// <summary>
    /// EN: Creates an exception for a non-nullable column.
    /// PT: Cria exceção para coluna que não aceita nulos.
    /// </summary>
    /// <param name="col">EN: Column name. PT: Nome da coluna.</param>
    public abstract Exception ColumnCannotBeNull(string col);
    /// <summary>
    /// EN: Creates an exception for a foreign key violation.
    /// PT: Cria exceção para violação de chave estrangeira.
    /// </summary>
    /// <param name="col">EN: Referencing column. PT: Coluna que referencia.</param>
    /// <param name="refTbl">EN: Referenced table. PT: Tabela referenciada.</param>
    public abstract Exception ForeignKeyFails(string col, string refTbl);
    /// <summary>
    /// EN: Creates an exception for deleting a referenced row.
    /// PT: Cria exceção para tentativa de remover linha referenciada.
    /// </summary>
    /// <param name="tbl">EN: Referenced table. PT: Tabela referenciada.</param>
    public abstract Exception ReferencedRow(string tbl);

    /// <summary>
    /// EN: Implements GetEnumerator.
    /// PT: Implementa GetEnumerator.
    /// </summary>
    public IEnumerator<IReadOnlyDictionary<int, object?>> GetEnumerator()
        => _items.GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator()
        => GetEnumerator();

    private sealed record PartitionRoutingInfo(
        string PartitionedColumnName,
        PartitionPartitionKind Kind,
        IReadOnlyList<PartitionPartitionItem> Partitions)
    {
        internal static PartitionRoutingInfo? TryParse(string partitionClauseSql)
        {
            var rangeMatch = Regex.Match(
                partitionClauseSql,
                @"PARTITION\s+BY\s+RANGE\s*\(\s*YEAR\s*\(\s*`?(?<column>[A-Za-z0-9_]+)`?\s*\)\s*\)\s*\((?<parts>[\s\S]+)\)\s*$",
                RegexOptions.IgnoreCase | RegexOptions.Singleline);

            if (rangeMatch.Success)
            {
                var partitionColumn = rangeMatch.Groups["column"].Value.NormalizeName();
                if (string.IsNullOrWhiteSpace(partitionColumn))
                    return null;

                var partitions = new List<PartitionPartitionItem>();
                foreach (var part in SplitTopLevelPartitions(rangeMatch.Groups["parts"].Value))
                {
                    var item = ParseRangePartitionItem(part);
                    if (item is null)
                        return null;

                    partitions.Add(item);
                }

                if (partitions.Count == 0)
                    return null;

                return new PartitionRoutingInfo(partitionColumn, PartitionPartitionKind.Range, partitions);
            }

            var listMatch = Regex.Match(
                partitionClauseSql,
                @"PARTITION\s+BY\s+LIST\s*\(\s*YEAR\s*\(\s*`?(?<column>[A-Za-z0-9_]+)`?\s*\)\s*\)\s*\((?<parts>[\s\S]+)\)\s*$",
                RegexOptions.IgnoreCase | RegexOptions.Singleline);

            if (!listMatch.Success)
                return null;

            var listPartitionColumn = listMatch.Groups["column"].Value.NormalizeName();
            if (string.IsNullOrWhiteSpace(listPartitionColumn))
                return null;

            var listPartitions = new List<PartitionPartitionItem>();
            foreach (var part in SplitTopLevelPartitions(listMatch.Groups["parts"].Value))
            {
                var item = ParseListPartitionItem(part);
                if (item is null)
                    return null;

                listPartitions.Add(item);
            }

            if (listPartitions.Count == 0)
                return null;

            return new PartitionRoutingInfo(listPartitionColumn, PartitionPartitionKind.List, listPartitions);
        }

        internal bool TryGetPartitionName(
            IReadOnlyDictionary<int, object?> row,
            TableMock table,
            out string partitionName)
        {
            partitionName = string.Empty;
            if (!table._columns.TryGetValue(PartitionedColumnName, out var column))
                return false;

            if (!row.TryGetValue(column.Index, out var rawValue) || rawValue is null || rawValue is DBNull)
                return false;

            if (!TryGetYear(rawValue, out var year))
                return false;

            foreach (var partition in Partitions)
            {
                if (Kind == PartitionPartitionKind.Range)
                {
                    if (partition.MaxValue)
                    {
                        partitionName = partition.Name;
                        return true;
                    }

                    if (year < partition.Value)
                    {
                        partitionName = partition.Name;
                        return true;
                    }

                    continue;
                }

                if (partition.ListValues is not null)
                {
                    if (partition.ListValues.Contains(year))
                    {
                        partitionName = partition.Name;
                        return true;
                    }

                    continue;
                }

                if (partition.Value == year)
                {
                    partitionName = partition.Name;
                    return true;
                }
            }

            return false;
        }

        private static IEnumerable<string> SplitTopLevelPartitions(string partsSql)
        {
            var start = 0;
            var depth = 0;
            for (var i = 0; i < partsSql.Length; i++)
            {
                var ch = partsSql[i];
                if (ch == '(')
                {
                    depth++;
                    continue;
                }

                if (ch == ')')
                {
                    if (depth > 0)
                        depth--;
                    continue;
                }

                if (ch == ',' && depth == 0)
                {
                    var slice = partsSql[start..i].Trim();
                    if (!string.IsNullOrWhiteSpace(slice))
                        yield return slice;
                    start = i + 1;
                }
            }

            var last = partsSql[start..].Trim();
            if (!string.IsNullOrWhiteSpace(last))
                yield return last;
        }

        private static PartitionPartitionItem? ParseRangePartitionItem(string partSql)
        {
            var match = Regex.Match(
                partSql,
                @"^\s*PARTITION\s+`?(?<name>[A-Za-z0-9_]+)`?\s+VALUES\s+LESS\s+THAN\s*(?:\(\s*(?<bound>MAXVALUE|-?\d+)\s*\)|(?<bound>MAXVALUE|-?\d+))\s*$",
                RegexOptions.IgnoreCase | RegexOptions.Singleline);
            if (!match.Success)
                return null;

            var name = match.Groups["name"].Value.NormalizeName();
            var boundRaw = match.Groups["bound"].Value.Trim();
            if (string.Equals(boundRaw, "MAXVALUE", StringComparison.OrdinalIgnoreCase))
                return new PartitionPartitionItem(name, 0, MaxValue: true);

            if (!int.TryParse(boundRaw, NumberStyles.Integer, CultureInfo.InvariantCulture, out var upperBound))
                return null;

            return new PartitionPartitionItem(name, upperBound, MaxValue: false);
        }

        private static PartitionPartitionItem? ParseListPartitionItem(string partSql)
        {
            var match = Regex.Match(
                partSql,
                @"^\s*PARTITION\s+`?(?<name>[A-Za-z0-9_]+)`?\s+VALUES\s+IN\s*\(\s*(?<values>[\s\S]+?)\s*\)\s*$",
                RegexOptions.IgnoreCase | RegexOptions.Singleline);
            if (!match.Success)
                return null;

            var name = match.Groups["name"].Value.NormalizeName();
            if (string.IsNullOrWhiteSpace(name))
                return null;

            var valuesSql = match.Groups["values"].Value;
            var values = SplitTopLevelCsv(valuesSql).ToList();
            if (values.Count == 0)
                return null;

            var parsedValues = new List<int>(values.Count);
            foreach (var rawValue in values)
            {
                if (!int.TryParse(rawValue.Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var value))
                    return null;

                parsedValues.Add(value);
            }

            return new PartitionPartitionItem(name, 0, MaxValue: false, parsedValues);
        }

        private static bool TryGetYear(object rawValue, out int year)
        {
            switch (rawValue)
            {
                case DateTime dateTime:
                    year = dateTime.Year;
                    return true;
                case DateTimeOffset dateTimeOffset:
                    year = dateTimeOffset.Year;
                    return true;
                case int intValue:
                    year = intValue;
                    return true;
                case long longValue when longValue >= int.MinValue && longValue <= int.MaxValue:
                    year = (int)longValue;
                    return true;
                case string text when DateTime.TryParse(text, CultureInfo.InvariantCulture, DateTimeStyles.AllowWhiteSpaces, out var parsedDate):
                    year = parsedDate.Year;
                    return true;
                default:
                    year = default;
                    return false;
            }
        }
    }

    private static IEnumerable<string> SplitTopLevelCsv(string partsSql)
    {
        var start = 0;
        var depth = 0;
        for (var i = 0; i < partsSql.Length; i++)
        {
            var ch = partsSql[i];
            if (ch == '(')
            {
                depth++;
                continue;
            }

            if (ch == ')')
            {
                if (depth > 0)
                    depth--;
                continue;
            }

            if (ch == ',' && depth == 0)
            {
                var slice = partsSql[start..i].Trim();
                if (!string.IsNullOrWhiteSpace(slice))
                    yield return slice;
                start = i + 1;
            }
        }

        var last = partsSql[start..].Trim();
        if (!string.IsNullOrWhiteSpace(last))
            yield return last;
    }

    private enum PartitionPartitionKind
    {
        Range,
        List
    }

    private sealed record PartitionPartitionItem(string Name, int Value, bool MaxValue, IReadOnlyList<int>? ListValues = null);
}
