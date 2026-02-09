using System.Collections;

namespace DbSqlLikeMem;

/// <summary>
/// Base de um schema em memória, responsável por tabelas e procedimentos.
/// </summary>
public abstract class SchemaMock 
    : ISchemaMock
    , IEnumerable<KeyValuePair<string, ITableMock>>
{
    /// <summary>
    /// Inicializa o schema com nome, banco e coleções opcionais.
    /// </summary>
    /// <param name="schemaName">Nome do schema.</param>
    /// <param name="db">Instância do banco pai.</param>
    /// <param name="tables">Configuração inicial de tabelas.</param>
    /// <param name="procedures">Procedimentos iniciais.</param>
    protected SchemaMock(
        string schemaName,
        DbMock db,
        IDictionary<string, (IColumnDictionary columns, IEnumerable<Dictionary<int, object?>>? rows)>? tables = null,
        IDictionary<string, ProcedureDef>? procedures = null/*,
        IDictionary<string, SqlSelectQuery>? views = null*/
    )
    {
        SchemaName = schemaName.NormalizeName();
        Db = db;
        if (tables != null)
            foreach (var (tableName, config) in tables)
                CreateTable(tableName, config.columns, config.rows);
        if (procedures != null)
            foreach (var (procedureName, config) in procedures)
                Procedures.Add(procedureName, config);
        //if (views != null)
        //    foreach (var (viewName, config) in views)
        //        Views.AddTable(viewName, config);
    }

    /// <summary>
    /// Nome normalizado do schema.
    /// </summary>
    public string SchemaName { get; }

    /// <summary>
    /// Banco ao qual o schema pertence.
    /// </summary>
    public DbMock Db { get; }

    /// <summary>
    /// Mapa interno de tabelas, com nomes normalizados no acesso.
    /// </summary>
    public TableDictionary tables = new TableDictionary();
    /// <summary>
    /// Exposição das tabelas do schema.
    /// </summary>
    public ITableDictionary Tables => tables;

    /// <summary>
    /// Contratos de procedimentos armazenados (apenas assinatura).
    /// </summary>
    public IDictionary<string, ProcedureDef> Procedures { get; } =
        new Dictionary<string, ProcedureDef>(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// Views não materializadas (somente definição) avaliadas sob demanda.
    /// </summary>
    internal IDictionary<string, SqlSelectQuery> Views { get; } =
        new Dictionary<string, SqlSelectQuery>(StringComparer.OrdinalIgnoreCase);

    protected abstract TableMock NewTable(
        string tableName,
        IColumnDictionary columns,
        IEnumerable<Dictionary<int, object?>>? rows = null);

    /// <summary>
    /// Cria uma tabela e a registra no schema.
    /// </summary>
    /// <param name="tableName">Nome da tabela.</param>
    /// <param name="columns">Colunas da tabela.</param>
    /// <param name="rows">Linhas iniciais.</param>
    /// <returns>Tabela criada.</returns>
    public TableMock CreateTable(
        string tableName,
        IColumnDictionary columns,
        IEnumerable<Dictionary<int, object?>>? rows = null)
    {
        var t = NewTable(tableName, columns, rows);
        tables.Add(tableName, t);
        return t;
    }

    #region Tables

    /// <summary>
    /// Adiciona uma tabela ao schema.
    /// </summary>
    /// <param name="key">Nome da tabela.</param>
    /// <param name="table">Tabela a adicionar.</param>
    public void Add(string key, ITableMock table)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(table);

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
    /// Tenta obter uma tabela pelo nome.
    /// </summary>
    /// <param name="key">Nome da tabela.</param>
    /// <param name="value">Tabela encontrada, se houver.</param>
    /// <returns>True se a tabela existir.</returns>
    public bool TryGetTable(string key, out ITableMock? value)
        => tables.TryGetValue(key.NormalizeName(), out value);

    /// <summary>
    /// Obtém ou define uma tabela pelo nome.
    /// </summary>
    public ITableMock this[string key]
    {
        get => tables[key.NormalizeName()];
        set => tables[key.NormalizeName()] = value;
    }

    /// <summary>
    /// Retorna os nomes das tabelas do schema.
    /// </summary>
    public IEnumerable<string> Keys => tables.Keys.ToHashSet(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// Retorna as tabelas do schema.
    /// </summary>
    public IEnumerable<ITableMock> Values => tables.Values;

    /// <summary>
    /// Retorna enumerador das tabelas do schema.
    /// </summary>
    public IEnumerator<KeyValuePair<string, ITableMock>> GetEnumerator()
        => tables.GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator()
        => tables.GetEnumerator();

    #region Backup / Restore (best-effort)

    /// <summary>
    /// Faz backup de todas as tabelas do schema.
    /// </summary>
    public virtual void BackupAllTablesBestEffort()
    {
        foreach (var table in tables.Values)
            table.Backup();
    }

    /// <summary>
    /// Restaura backup de todas as tabelas do schema.
    /// </summary>
    public virtual void RestoreAllTablesBestEffort()
    {
        foreach (var table in tables.Values)
            table.Restore();
    }

    /// <summary>
    /// Limpa backup de todas as tabelas do schema.
    /// </summary>
    public virtual void ClearBackupAllTablesBestEffort()
    {
        foreach (var table in tables.Values)
            table.ClearBackup();
    }

    #endregion

    #endregion
}
