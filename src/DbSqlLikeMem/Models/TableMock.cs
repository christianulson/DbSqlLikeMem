using System.Collections.Concurrent;

namespace DbSqlLikeMem;

/// <summary>
/// Base de uma tabela em memória com dados, colunas e índices.
/// </summary>
public abstract class TableMock
    : List<Dictionary<int, object?>>,
    ITableMock
{

    /// <summary>
    /// Inicializa a tabela com nome, schema e colunas, com linhas opcionais.
    /// </summary>
    /// <param name="tableName">Nome da tabela.</param>
    /// <param name="schema">Schema pai.</param>
    /// <param name="columns">Colunas da tabela.</param>
    /// <param name="rows">Linhas iniciais.</param>
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
    /// Nome normalizado da tabela.
    /// </summary>
    public string TableName { get; }
    /// <summary>
    /// Schema ao qual a tabela pertence.
    /// </summary>
    public SchemaMock Schema { get; }
    /// <summary>
    /// Próximo valor de identidade para colunas auto incrementais.
    /// </summary>
    public int NextIdentity { get; set; } = 1;

    /// <summary>
    /// Dicionário de colunas da tabela.
    /// </summary>
    public IColumnDictionary Columns { get; }

    // ---------- Wave D : índices ---------------------------------
    /// <summary>
    /// Índices das colunas que formam a chave primária.
    /// </summary>
    public HashSet<int> PrimaryKeyIndexes { get; } = [];

#pragma warning disable CA1002 // Do not expose generic lists
    private readonly List<(string Col, string RefTable, string RefCol)> _foreignKeys = [];
    /// <summary>
    /// Lista de chaves estrangeiras definidas na tabela.
    /// </summary>
    public IReadOnlyList<(string Col, string RefTable, string RefCol)> ForeignKeys => _foreignKeys;
#pragma warning restore CA1002 // Do not expose generic lists

#pragma warning disable CA1002 // Do not expose generic lists
    /// <summary>
    /// Índices declarados na tabela.
    /// </summary>
    public IndexDictionary Indexes { get; } = [];
#pragma warning restore CA1002 // Do not expose generic lists

    // nome-do-índice → chave-derivada(string) → posições (List<int>)
    private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, List<int>>> _ix
        = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// Retorna o ColumnDef para <paramref name="columnName"/>
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
    /// Cria e registra um índice na tabela.
    /// </summary>
    /// <param name="def">Definição do índice.</param>
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
    /// Procura valores no índice usando a chave informada.
    /// </summary>
    /// <param name="def">Definição do índice.</param>
    /// <param name="key">Chave a buscar.</param>
    /// <returns>Lista de posições ou null.</returns>
    public IEnumerable<int>? Lookup(IndexDef def, string key)
    {
        ArgumentNullException.ThrowIfNull(def);
        return _ix.TryGetValue(def.Name.NormalizeName(), out var map)
            && map.TryGetValue(key.NormalizeName(), out var list)
            ? list
            : null;
    }

    /// <summary>
    /// Atualiza os índices após inserir ou alterar uma linha.
    /// </summary>
    /// <param name="rowIdx">Índice da linha alterada.</param>
    public void UpdateIndexesWithRow(int rowIdx)
    {
        foreach (var (name, def) in Indexes)
        {
            var key = BuildKey(this[rowIdx], def.KeyCols.Select(c => Columns[c].Index));
            _ix[name].AddOrUpdate(key, [rowIdx], (_, list) => { list.Add(rowIdx); return list; });
        }
    }

    /// <summary>
    /// Reconstrói todos os índices da tabela.
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
    /// Adiciona vários itens convertendo-os em linhas.
    /// </summary>
    /// <typeparam name="T">Tipo dos itens.</typeparam>
    /// <param name="items">Itens a inserir.</param>
    public void AddRangeItems<T>(IEnumerable<T> items)
    {
        ArgumentNullException.ThrowIfNull(items);
        foreach (var item in items)
            AddItem(item);
    }

    /// <summary>
    /// Adiciona linhas já materializadas.
    /// </summary>
    /// <param name="items">Linhas a inserir.</param>
    public new void AddRange(IEnumerable<Dictionary<int, object?>> items)
    {
        ArgumentNullException.ThrowIfNull(items);
        foreach (var item in items)
            Add(item);
    }

    /// <summary>
    /// Adiciona um item convertendo-o em linha da tabela.
    /// </summary>
    /// <typeparam name="T">Tipo do item.</typeparam>
    /// <param name="item">Item a inserir.</param>
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
    /// Adiciona uma linha garantindo valores padrão e unicidade.
    /// </summary>
    /// <param name="value">Linha a inserir.</param>
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
    /// Faz backup das linhas atuais.
    /// </summary>
    public void Backup() => _backup = [.. this.Select(row => new Dictionary<int, object?>(row))];

    /// <summary>
    /// Restaura o backup anterior, se existir.
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
    /// Limpa o backup armazenado.
    /// </summary>
    public void ClearBackup() => _backup = null;

    /// <summary>
    /// Obtém ou define a coluna atualmente em avaliação.
    /// </summary>
    public abstract string? CurrentColumn { get; set; }
    /// <summary>
    /// Resolve um token para um valor no contexto da tabela.
    /// </summary>
    /// <param name="token">Token a resolver.</param>
    /// <param name="dbType">Tipo esperado.</param>
    /// <param name="isNullable">Se o valor pode ser nulo.</param>
    /// <param name="pars">Parâmetros de consulta.</param>
    /// <param name="colDict">Dicionário de colunas opcional.</param>
    /// <returns>Valor resolvido.</returns>
    public abstract object? Resolve(
        string token,
        DbType dbType,
        bool isNullable,
        IDataParameterCollection? pars = null,
        IColumnDictionary? colDict = null);

    /// <summary>
    /// Cria exceção para coluna inexistente.
    /// </summary>
    /// <param name="columnName">Nome da coluna.</param>
    public abstract Exception UnknownColumn(string columnName);
    /// <summary>
    /// Cria exceção para chave duplicada.
    /// </summary>
    /// <param name="tbl">Tabela afetada.</param>
    /// <param name="key">Nome da chave.</param>
    /// <param name="val">Valor duplicado.</param>
    public abstract Exception DuplicateKey(string tbl, string key, object? val);
    /// <summary>
    /// Cria exceção para coluna que não aceita nulos.
    /// </summary>
    /// <param name="col">Nome da coluna.</param>
    public abstract Exception ColumnCannotBeNull(string col);
    /// <summary>
    /// Cria exceção para violação de chave estrangeira.
    /// </summary>
    /// <param name="col">Coluna que referencia.</param>
    /// <param name="refTbl">Tabela referenciada.</param>
    public abstract Exception ForeignKeyFails(string col, string refTbl);
    /// <summary>
    /// Cria exceção para tentativa de remover linha referenciada.
    /// </summary>
    /// <param name="tbl">Tabela referenciada.</param>
    public abstract Exception ReferencedRow(string tbl);
}
