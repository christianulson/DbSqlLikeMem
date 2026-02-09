using System.Collections;
using System.Collections.Concurrent;
using System.Text.RegularExpressions;

namespace DbSqlLikeMem;

/// <summary>
/// EN: Base for an in-memory table with data, columns, and indexes.
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
        IColumnDictionary columns,
        IEnumerable<Dictionary<int, object?>>? rows = null)
    {
        TableName = tableName.NormalizeName();
        Schema = schema;
        Columns = columns;
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
    /// EN: Table column dictionary.
    /// PT: Dicionário de colunas da tabela.
    /// </summary>
    public IColumnDictionary Columns { get; }

    private List<Dictionary<int, object?>> Items { get; } = [];

    // ---------- Wave D : índices ---------------------------------
    /// <summary>
    /// EN: Indexes of columns that form the primary key.
    /// PT: Índices das colunas que formam a chave primária.
    /// </summary>
    public HashSet<int> PrimaryKeyIndexes { get; } = [];

#pragma warning disable CA1002 // Do not expose generic lists
    private readonly List<(string Col, string RefTable, string RefCol)> _foreignKeys = [];
    /// <summary>
    /// EN: List of foreign keys defined in the table.
    /// PT: Lista de chaves estrangeiras definidas na tabela.
    /// </summary>
    public IReadOnlyList<(string Col, string RefTable, string RefCol)> ForeignKeys => _foreignKeys;
#pragma warning restore CA1002 // Do not expose generic lists

#pragma warning disable CA1002 // Do not expose generic lists
    /// <summary>
    /// EN: Indexes declared on the table.
    /// PT: Índices declarados na tabela.
    /// </summary>
    public IndexDictionary Indexes { get; } = [];
#pragma warning restore CA1002 // Do not expose generic lists

    // nome-do-índice → chave-derivada(string) → posições (List<int>)
    private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, List<int>>> _ix
        = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// EN: Returns the ColumnDef for <paramref name="columnName"/> or throws UnknownColumn.
    /// PT: Retorna o ColumnDef para <paramref name="columnName"/>
    /// ou lança UnknownColumn se não existir.
    /// </summary>
    public ColumnDef GetColumn(string columnName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(columnName);
        if (!Columns.TryGetValue(columnName.NormalizeName(), out var info))
            throw UnknownColumn(columnName);
        return info;
    }

    /// <summary>
    /// EN: Creates and registers an index on the table.
    /// PT: Cria e registra um índice na tabela.
    /// </summary>
    /// <param name="def">EN: Index definition. PT: Definição do índice.</param>
    public void CreateIndex(IndexDef def)
    {
        ArgumentNullException.ThrowIfNull(def);
        ArgumentException.ThrowIfNullOrWhiteSpace(def.Name);
        var name = def.Name.NormalizeName();
        if (Indexes.ContainsKey(name))
            throw new InvalidOperationException($"Índice '{name}' já existe.");
        Indexes.Add(name, def);
        RebuildIndex(def);
    }

    internal void RebuildIndex(IndexDef def)
    {
        ArgumentNullException.ThrowIfNull(def);
        ArgumentException.ThrowIfNullOrWhiteSpace(def.Name);
        var name = def.Name.NormalizeName();
        var map = new ConcurrentDictionary<string, List<int>>(StringComparer.OrdinalIgnoreCase);
        for (int i = 0; i < Count; i++)
        {
            var key = BuildIndexKey(def, Items[i]);
            map.AddOrUpdate(key, [i], (_, list) => { list.Add(i); return list; });
        }
        _ix[name] = map;
    }

    /// <summary>
    /// EN: Looks up values in the index using the given key.
    /// PT: Procura valores no índice usando a chave informada.
    /// </summary>
    /// <param name="def">EN: Index definition. PT: Definição do índice.</param>
    /// <param name="key">EN: Key to search. PT: Chave a buscar.</param>
    /// <returns>EN: List of positions or null. PT: Lista de posições ou null.</returns>
    public IEnumerable<int>? Lookup(IndexDef def, string key)
    {
        ArgumentNullException.ThrowIfNull(def);
        return _ix.TryGetValue(def.Name.NormalizeName(), out var map)
            && map.TryGetValue(key.NormalizeName(), out var list)
            ? list
            : null;
    }

    /// <summary>
    /// EN: Updates indexes after inserting or changing a row.
    /// PT: Atualiza os índices após inserir ou alterar uma linha.
    /// </summary>
    /// <param name="rowIdx">EN: Changed row index. PT: Índice da linha alterada.</param>
    public void UpdateIndexesWithRow(int rowIdx)
    {
        foreach (var (name, def) in Indexes)
        {
            var key = BuildIndexKey(def, Items[rowIdx]);
            _ix[name].AddOrUpdate(key, [rowIdx], (_, list) => { list.Add(rowIdx); return list; });
        }
    }

    /// <summary>
    /// EN: Rebuilds all table indexes.
    /// PT: Reconstrói todos os índices da tabela.
    /// </summary>
    public void RebuildAllIndexes()
    {
        foreach (var ix in Indexes)
            RebuildIndex(ix.Value);
    }

    internal string BuildIndexKey(
        IndexDef idx,
        IReadOnlyDictionary<int, object?> row)
    {
        ArgumentNullException.ThrowIfNull(idx);
        ArgumentNullException.ThrowIfNull(row);
        return string.Join('|', idx.KeyCols.Select(colName =>
        {
            var ci = Columns[colName];
            if (ci.GetGenValue != null)
                return ci.GetGenValue(row, this)?.ToString() ?? "<null>";
            return row.TryGetValue(ci.Index, out var v) ? (v?.ToString() ?? "<null>") : "<null>";
        }));
    }

    public void CreateForeignKey(
        string col,
        string refTable,
        string refCol)
    {
        _foreignKeys.Add((col, refTable, refCol));
    }

    /// <summary>
    /// EN: Adds multiple items by converting them into rows.
    /// PT: Adiciona vários itens convertendo-os em linhas.
    /// </summary>
    /// <typeparam name="T">EN: Item type. PT: Tipo dos itens.</typeparam>
    /// <param name="items">EN: Items to insert. PT: Itens a inserir.</param>
    public void AddRangeItems<T>(IEnumerable<T> items)
    {
        ArgumentNullException.ThrowIfNull(items);
        foreach (var item in items)
            AddItem(item);
    }

    /// <summary>
    /// EN: Adds already materialized rows.
    /// PT: Adiciona linhas já materializadas.
    /// </summary>
    /// <param name="items">EN: Rows to insert. PT: Linhas a inserir.</param>
    public new void AddRange(IEnumerable<Dictionary<int, object?>> items)
    {
        ArgumentNullException.ThrowIfNull(items);
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
        ArgumentNullException.ThrowIfNull(item);

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
        EnsureUniqueOnInsert(value);
        Items.Add(value);
        // Update indexes with the new row
        int newIdx = Count - 1;
        UpdateIndexesWithRow(newIdx);
    }

    private void ApplyDefaultValues(Dictionary<int, object?> value)
    {
        foreach (var (key, col) in Columns)
        {
            if (!value.ContainsKey(col.Index)) continue;
            if (col.Identity) value[col.Index] = NextIdentity++;
            else if (col.DefaultValue != null && value[col.Index] == null) value[col.Index] = col.DefaultValue;
            if (!col.Nullable && value[col.Index] == null) throw ColumnCannotBeNull(key);
        }
    }

    internal void EnsureUniqueOnInsert(Dictionary<int, object?> newRow)
    {
        if (PrimaryKeyIndexes.Count > 0)
        {
            for (int i = 0; i < Count; i++)
            {
                var pks = new List<string>();
                foreach (var pkIdx in PrimaryKeyIndexes)
                {
                    if (newRow.TryGetValue(pkIdx, out var pkVal)
                        && this[i].TryGetValue(pkIdx, out var cur)
                        && Equals(cur, pkVal))
                    {
                        pks.Add($"{Columns.First(_ => _.Value.Index == pkIdx).Key}: {pkVal}");
                    }
                }
                if (PrimaryKeyIndexes.Count == pks.Count)
                    throw DuplicateKey(TableName, "PRIMARY", string.Join(",", pks));
            }
        }

        foreach (var idx in Indexes.GetUnique())
        {
            var key = BuildIndexKey(idx, newRow);
            var hits = Lookup(idx, key);
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

        if (PrimaryKeyIndexes.Count > 0)
        {
            for (int i = 0; i < Count; i++)
            {
                var pks = new List<string>();
                foreach (var pkIdx in PrimaryKeyIndexes)
                {
                    if (newRow.TryGetValue(pkIdx, out var pkVal)
                        && this[i].TryGetValue(pkIdx, out var cur)
                        && Equals(cur, pkVal))
                    {
                        pks.Add($"{Columns.First(_ => _.Value.Index == pkIdx).Key}: {pkVal}");
                    }
                }
                if (PrimaryKeyIndexes.Count == pks.Count)
                {
                    conflictIndexName = "PRIMARY";
                    conflictKey = string.Join(",", pks);
                    return i;
                }
            }
        }

        foreach (var idx in Indexes.GetUnique())
        {
            var key = BuildIndexKey(idx, newRow);
            var hits = Lookup(idx, key);
            if (hits != null)
            {
                var hitsList = hits.ToList();
                if (hitsList.Count > 0)
                {
                    conflictIndexName = idx.Name;
                    conflictKey = key;
                    return hitsList[0];
                }
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
        foreach (var ix in Indexes.GetUnique())
        {
            if (!ix.KeyCols.Intersect(changedCols, StringComparer.OrdinalIgnoreCase).Any()) continue;

            var oldKey = BuildIndexKey(ix, existingRow);
            var newKey = BuildIndexKey(ix, simulatedRow);

            if (!oldKey.Equals(newKey, StringComparison.Ordinal) &&
                Lookup(ix, newKey)?.Any(i => i != rowIdx) == true)
            {
                throw DuplicateKey(tableName, ix.Name, newKey);
            }
        }
    }

    internal static string? ResolveWhereRaw(
        string? whereRaw,
        string? rawSql)
    {
        var w = whereRaw;
        if (string.IsNullOrWhiteSpace(w) && !string.IsNullOrWhiteSpace(rawSql))
        {
            w = TryExtractWhereRaw(rawSql);
        }

        return w;
    }

    internal static List<(string C, string Op, string V)> ParseWhereSimple(string? w)
    {
        var list = new List<(string C, string Op, string V)>();
        if (string.IsNullOrWhiteSpace(w)) return list;

        w = w.Trim();
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

            var kv = s.Split('=', 2);
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

                if (rhs.StartsWith("(", StringComparison.Ordinal) && rhs.EndsWith(")", StringComparison.Ordinal))
                {
                    var inner = rhs[1..^1];
                    var parts = inner.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

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

    public Dictionary<int, object?> RemoveAt(int idx)
    {
        var it = Items[idx];
        Items.RemoveAt(idx);
        return it;
    }

    public void UpdateRowColumn(
        int rowIdx,
        int colIdx,
        object? value)
        => Items[rowIdx][colIdx] = value;

    private List<Dictionary<int, object?>>? _backup;

    /// <summary>
    /// EN: Backs up current rows.
    /// PT: Faz backup das linhas atuais.
    /// </summary>
    public void Backup() => _backup = [.. this.Select(row => new Dictionary<int, object?>(row))];

    /// <summary>
    /// EN: Restores the previous backup, if any.
    /// PT: Restaura o backup anterior, se existir.
    /// </summary>
    public void Restore()
    {
        if (_backup == null)
            return;

        Items.Clear();
        foreach (var row in _backup) Add(row);

        foreach (var ix in Indexes)
            RebuildIndex(ix.Value);
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

    public int Count => Items.Count;

    public IReadOnlyDictionary<int, object?> this[int index] => Items[index];

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
        IColumnDictionary? colDict = null);

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

    public IEnumerator<IReadOnlyDictionary<int, object?>> GetEnumerator()
        => Items.Select(_=> _.AsReadOnly()).ToList().AsReadOnly().GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator()
        => GetEnumerator();
}
