using System.Collections.Concurrent;

namespace DbSqlLikeMem;

/// <summary>
/// EN: Base for an in-memory table with data, columns, and indexes.
/// PT: Base de uma tabela em memória com dados, colunas e índices.
/// </summary>
public abstract class TableMock
    : List<Dictionary<int, object?>>,
    ITableMock
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
            var key = BuildKey(this[i], def.KeyCols.Select(c => Columns[c].Index));
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
            var key = BuildKey(this[rowIdx], def.KeyCols.Select(c => Columns[c].Index));
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

    private static string BuildKey(
        Dictionary<int, object?> row, IEnumerable<int> ords) =>
        string.Join('|', ords.Select(o => row.TryGetValue(o, out var it) ? it?.ToString() ?? "<null>" : "<null>"));

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
    public new void Add(Dictionary<int, object?> value)
    {
        ApplyDefaultValues(value);
        // Before adding, enforce unique indexes
        foreach (var idx in Indexes.GetUnique())
        {
            var key = BuildKey(value, idx.KeyCols.Select(c => Columns[c].Index));
            if (_ix.TryGetValue(idx.Name, out var map)
                && map.TryGetValue(key, out var existing)
                && existing.Count != 0)
            {
                throw DuplicateKey(TableName, idx.Name, key);
            }
        }
        base.Add(value);
        // Update indexes with the new row
        int newIdx = Count - 1;
        foreach (var idx in Indexes)
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

        Clear();
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
}
