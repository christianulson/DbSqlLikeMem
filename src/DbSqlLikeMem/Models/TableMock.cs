using DbSqlLikeMem.Interfaces;
using DbSqlLikeMem.Models;
using System.Collections.ObjectModel;

namespace DbSqlLikeMem;

/// <summary>
/// EN: Base for an in-memory table with data, columns, and _indexes.
/// PT: Base de uma tabela em memória com dados, colunas e índices.
/// </summary>
public abstract class TableMock
    : ITableMock
{

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

    private readonly ColumnDictionary _columns = [];
    
    /// <summary>
    /// EN: Table column dictionary.
    /// PT: Dicionário de colunas da tabela.
    /// </summary>
    public IReadOnlyDictionary<string, ColumnDef> Columns
        => new ReadOnlyDictionary<string, ColumnDef>(_columns);

    private readonly List<Dictionary<int, object?>> _items = [];

    public IReadOnlyList<IReadOnlyDictionary<int, object?>> Items => [.. _items
        .Select(_=> new ReadOnlyDictionary<int, object?>(_))];

    internal HashSet<int> _primaryKeyIndexes = [];

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
            throw new InvalidOperationException("Colunas da PK Duplicadas");
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
        var idx = _columns.Count;
        var col = new ColumnDef(
            table: this,
            name: name,
            index: idx,
            dbType: dbType,
            nullable: nullable,
            size: size,
            decimalPlaces: decimalPlaces,
            identity: identity,
            defaultValue: defaultValue,
            enumValues: enumValues);

        _columns.Add(name, col);
        return col;
    }

    /// <summary>
    /// EN: Add new Vollumn to Table
    /// PT: Incluir nova coluna na tabela
    /// </summary>
    /// <returns></returns>
    public ColumnDef AddColumn(
        Col column)
    {
        var idx = _columns.Count;
        var col = new ColumnDef(
            table: this,
            name: column.name,
            index: idx,
            dbType: column.dbType,
            nullable: column.nullable,
            size: column.size,
            decimalPlaces: column.decimalPlaces,
            identity: column.identity,
            defaultValue: column.defaultValue,
            enumValues: column.enumValues);

        _columns.Add(column.name, col);
        return col;
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
            throw new InvalidOperationException($"Índice '{name}' já existe.");
        var idx = new IndexDef(this, name, keyCols, include, unique);
        _indexes.Add(name, idx);
        return idx;
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
        string key)
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
    public void UpdateIndexesWithRow(int rowIdx)
    {
        foreach (var it in _indexes)
            it.Value.UpdateIndexesWithRow(rowIdx, this[rowIdx]);
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
            return;
        }

        Parallel.ForEach(_indexes.Values, ix => ix.RebuildIndex());
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
        var tbRef = Schema[refTable];
        var fk = new ForeignDef(
            this,
            name,
            tbRef,
            [.. references.Select(_ => (col: Columns[_.col], refCol: tbRef.Columns[_.refCol]))]
            );

        _foreignKeys.Add(name, fk);
        return fk;
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
            var valuesByColumn = fk.References.ToDictionary(
                _ => _.refCol.Name.NormalizeName(),
                _ => row[_.col.Index],
                StringComparer.OrdinalIgnoreCase);

            var key = matchingIndex.BuildIndexKeyFromValues(valuesByColumn);
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
    public void AddRangeItems<T>(IEnumerable<T> items)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(items, nameof(items));
        foreach (var item in items)
            AddItem(item);
    }

    /// <summary>
    /// EN: Adds already materialized rows.
    /// PT: Adiciona linhas já materializadas.
    /// </summary>
    /// <param name="items">EN: Rows to insert. PT: Linhas a inserir.</param>
    public void AddRange(IEnumerable<Dictionary<int, object?>> items)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(items, nameof(items));
        foreach (var item in items)
            Add(item);
    }

    /// <summary>
    /// EN: Adds an item by converting it into a table row.
    /// PT: Adiciona um item convertendo-o em linha da tabela.
    /// </summary>
    /// <typeparam name="T">EN: Item type. PT: Tipo do item.</typeparam>
    /// <param name="item">EN: Item to insert. PT: Item a inserir.</param>
    public void AddItem<T>(T item)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(item, nameof(item));

        var row = new Dictionary<int, object?>();

        // pega props públicas de instância (ignorando indexers)
        var t = typeof(T);

        foreach (var p in Columns)
        {
            var prop = t.GetProperty(p.Key);
            if (prop == null)
            {
                row[p.Value.Index] = null;
                continue;
            }
            object? value;
#pragma warning disable CA1031 // Do not catch general exception types
            try
            {
                value = prop.GetValue(item);
            }
            catch
            {
                value = null;
            }
#pragma warning restore CA1031 // Do not catch general exception types

            row[p.Value.Index] = value;
        }

        // reaproveita sua lógica de unique + index update
        Add(row);
    }

    /// <summary>
    /// EN: Adds a row ensuring default values and uniqueness.
    /// PT: Adiciona uma linha garantindo valores padrão e unicidade.
    /// </summary>
    /// <param name="value">EN: Row to insert. PT: Linha a inserir.</param>
    public void Add(Dictionary<int, object?> value)
    {
        ApplyDefaultValues(value);
        RefreshPersistedComputedValues(value);
        EnsureUniqueOnInsert(value);
        _items.Add(value);
        // Update _indexes with the new row
        int newIdx = Count - 1;
        UpdateIndexesWithRow(newIdx);
    }

    private void ApplyDefaultValues(Dictionary<int, object?> value)
    {
        foreach (var it in Columns)
        {
            var col = it.Value;
            if (!value.ContainsKey(col.Index))
                value[col.Index] = null;

            if (col.Identity)
                value[col.Index] = NextIdentity++;
            else if (col.DefaultValue != null && value[col.Index] == null)
                value[col.Index] = col.DefaultValue;

            if (col.GetGenValue != null && col.PersistComputedValue)
                value[col.Index] = col.GetGenValue(value, this);

            if (!col.Nullable && value[col.Index] == null)
                throw ColumnCannotBeNull(it.Key);
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
        if (_primaryKeyIndexes.Count > 0)
        {
            if (TryFindPrimaryConflictByIndex(newRow, out _))
            {
                var dupPk = _columns
                    .Where(_ => _primaryKeyIndexes.Contains(_.Value.Index))
                    .Select(_ => $"{_.Key}: {(newRow.TryGetValue(_.Value.Index, out var v) ? v : null)}");
                throw DuplicateKey(TableName, "PRIMARY", string.Join(",", dupPk));
            }
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
                if (_primaryKeyIndexes.Count == pks.Count)
                    throw DuplicateKey(TableName, "PRIMARY", string.Join(",", pks));
            }
        }

        foreach (var idx in _indexes.GetUnique())
        {
            var key = idx.BuildIndexKey(newRow);
            var hits = idx.LookupMutable(key);
            if (hits?.Any() == true)
                throw DuplicateKey(TableName, idx.Name, key);
        }
    }

    internal int? FindConflictingRowIndex(
        IReadOnlyDictionary<int, object?> newRow,
        out string? conflictIndexName,
        out object? conflictKey)
    {
        conflictIndexName = null;
        conflictKey = null;

        if (TryFindPrimaryConflictByIndex(newRow, out var conflictByIndex))
        {
            var dupPk = _columns
                .Where(_ => _primaryKeyIndexes.Contains(_.Value.Index))
                .Select(_ => $"{_.Key}: {(newRow.TryGetValue(_.Value.Index, out var v) ? v : null)}");
            conflictIndexName = "PRIMARY";
            conflictKey = string.Join(",", dupPk);
            return conflictByIndex;
        }

        if (_primaryKeyIndexes.Count > 0)
        {
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
                if (_primaryKeyIndexes.Count == pks.Count)
                {
                    conflictIndexName = "PRIMARY";
                    conflictKey = string.Join(",", pks);
                    return i;
                }
            }
        }

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
                list.Add((min.Groups["c"].Value.Trim(), "IN", min.Groups["v"].Value.Trim()));
                continue;
            }

            var kv = s.Split('=').Take(2).ToArray();
            if (kv.Length == 2)
            {
                list.Add((kv[0].Trim(), "=", kv[1].Trim()));
            }
        }

        return list;
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

            if (cond.Op.Equals("=", StringComparison.OrdinalIgnoreCase))
            {
                table.CurrentColumn = cond.C;
                var exp = table.Resolve(cond.V, info.DbType, info.Nullable, pars, table.Columns);
                table.CurrentColumn = null;
                return Equals(actual, exp is DBNull ? null : exp);
            }

            if (cond.Op.Equals("IN", StringComparison.OrdinalIgnoreCase))
            {
                var rhs = cond.V.Trim();

                IEnumerable<object?> candidates;

                if (rhs.StartsWith("(")
                && rhs.EndsWith(")"))
                {
                    var inner = rhs[1..^1].Trim();

                    if (Regex.IsMatch(inner, @"^(SELECT|WITH)\b", RegexOptions.IgnoreCase))
                    {
                        candidates = ResolveInSubqueryCandidates(table, info, inner, pars);
                    }
                    else
                    {
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

                            if (val is System.Collections.IEnumerable ie && val is not string)
                            {
                                foreach (var v in ie) tmp.Add(v);
                            }
                            else
                            {
                                tmp.Add(val);
                            }
                        }
                        candidates = tmp;
                    }
                }
                else
                {
                    table.CurrentColumn = cond.C;
                    var resolved = table.Resolve(rhs, info.DbType, info.Nullable, pars, table.Columns);
                    table.CurrentColumn = null;

                    resolved = resolved is DBNull ? null : resolved;

                    if (resolved is System.Collections.IEnumerable ie && resolved is not string)
                    {
                        var tmp = new List<object?>();
                        foreach (var v in ie) tmp.Add(v);
                        candidates = tmp;
                    }
                    else
                    {
                        candidates = [resolved];
                    }
                }

                foreach (var cand in candidates)
                {
                    if (Equals(actual, cand))
                        return true;
                }

                return false;
            }

            return false;
        });

    private static IEnumerable<object?> ResolveInSubqueryCandidates(
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

        var tmp = new List<object?>();
        foreach (var row in sourceTable)
        {
            if (whereConds.Count > 0 && !IsMatchSimple(sourceTable, pars, whereConds, row))
                continue;

            var v = sourceCol.GetGenValue != null ? sourceCol.GetGenValue(row, sourceTable) : row[sourceCol.Index];
            if (v is DBNull) v = null;

            if (v != null)
            {
                try
                {
                    v = DbTypeParser.Parse(targetInfo.DbType, v.ToString()!);
                }
                catch
                {
                    // best effort: keep original value when coercion is not possible
                }
            }

            tmp.Add(v);
        }

        return tmp;
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
        _items.RemoveAt(idx);
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
        _items[rowIdx][colIdx] = value;
        RefreshPersistedComputedValues(_items[rowIdx]);
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
        foreach (var row in _backup) Add(row);

        foreach (var ix in _indexes)
            ix.Value.RebuildIndex();
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
}
