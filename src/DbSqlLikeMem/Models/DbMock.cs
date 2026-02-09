using System.Diagnostics.CodeAnalysis;

namespace DbSqlLikeMem;

/// <summary>
/// Base de um banco em memória com schemas, tabelas e procedimentos.
/// </summary>
public abstract class DbMock
    : Dictionary<string, SchemaMock>
    , ISchemaDictionary
{
    /// <summary>
    /// Versão do banco simulada.
    /// </summary>
    public int Version { get; }

    /// <summary>
    /// Indica se operações devem aplicar bloqueio para segurança de threads.
    /// </summary>
    public bool ThreadSafe { get; set; }

    internal object SyncRoot { get; } = new();

    internal abstract SqlDialectBase Dialect { get; set; }

    IEnumerable<string> IReadOnlyDictionary<string, ISchemaMock>.Keys => Keys;

    IEnumerable<ISchemaMock> IReadOnlyDictionary<string, ISchemaMock>.Values => Values;

    ISchemaMock IReadOnlyDictionary<string, ISchemaMock>.this[string key] => throw new NotImplementedException();

    /// <summary>
    /// Inicializa o banco com a versão informada e um schema padrão.
    /// </summary>
    /// <param name="version">Versão simulada do banco.</param>
    protected DbMock(
        int version)
        : base(StringComparer.OrdinalIgnoreCase)
    {
        Version = version;
        CreateSchema("DefaultSchema");
    }

    //protected DbMock(
    //    IDictionary<string, ISchemaMock>? schemas
    //    ) : base(StringComparer.OrdinalIgnoreCase)
    //{
    //    ArgumentNullException.ThrowIfNull(schemas);
    //    foreach (var (k, v) in schemas)
    //        AddTable(k, v);
    //}

    #region Schema

    protected abstract SchemaMock NewSchema(
        string schemaName,
        IDictionary<string, (IColumnDictionary columns, IEnumerable<Dictionary<int, object?>>? rows)>? tables = null);

    /// <summary>
    /// Cria um schema e o registra no banco.
    /// </summary>
    /// <param name="schemaName">Nome do schema.</param>
    /// <param name="tables">Tabelas iniciais do schema.</param>
    /// <returns>Schema criado.</returns>
    public ISchemaMock CreateSchema(
        string schemaName,
        IDictionary<string, (IColumnDictionary columns, IEnumerable<Dictionary<int, object?>>? rows)>? tables = null)
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
                throw new Exception($"Existe mais de um Schema ({string.Join(',', this.Keys)}), escolha um e passe como parâmetro");
            if (Count == 0)
                throw new Exception("Schema não existe cadastrado");
            schemaName = this.Keys.First();
        }
        return schemaName.NormalizeName();
    }

    /// <summary>
    /// Tenta obter um schema pelo nome.
    /// </summary>
    /// <param name="key">Nome do schema.</param>
    /// <param name="value">Schema encontrado, se houver.</param>
    /// <returns>True se o schema existir.</returns>
    public bool TryGetValue(
        string key,
        [MaybeNullWhen(false)] out ISchemaMock value
    )
    {
        if (base.TryGetValue(key, out var v) && v != null)
        {
            value = (ISchemaMock)v;
            return true;
        }
        value = null;
        return false;
    }

    #endregion

    #region Table

    /// <summary>
    /// Cria e adiciona uma tabela ao schema indicado.
    /// </summary>
    /// <param name="tableName">Nome da tabela.</param>
    /// <param name="columns">Definição das colunas.</param>
    /// <param name="rows">Linhas iniciais.</param>
    /// <param name="schemaName">Schema alvo.</param>
    /// <returns>Tabela criada.</returns>
    public ITableMock AddTable(
        string tableName,
        IColumnDictionary? columns = null,
        IEnumerable<Dictionary<int, object?>>? rows = null,
        string? schemaName = null)
    {
        var sc = GetSchemaName(schemaName);
        if (!this.TryGetValue(sc, out var s))
            s = CreateSchema(sc);
        return s.CreateTable(tableName, columns ?? new ColumnDictionary(), rows);
    }

    /// <summary>
    /// Obtém uma tabela pelo nome, lançando erro se não existir.
    /// </summary>
    /// <param name="tableName">Nome da tabela.</param>
    /// <param name="schemaName">Schema alvo.</param>
    /// <returns>Tabela encontrada.</returns>
    public ITableMock GetTable(
        string tableName,
        string? schemaName = null)
    {
        ArgumentNullException.ThrowIfNull(tableName);
        var sc = GetSchemaName(schemaName);
        if (!this[sc].TryGetTable(tableName, out var tb)
            || tb == null)
            throw new Exception($"Tabela não existe cadastrada {tableName}");
        return tb;
    }

    /// <summary>
    /// Tenta obter uma tabela pelo nome.
    /// </summary>
    /// <param name="tableName">Nome da tabela.</param>
    /// <param name="tb">Tabela encontrada, se houver.</param>
    /// <param name="schemaName">Schema alvo.</param>
    /// <returns>True se a tabela existir.</returns>
    public bool TryGetTable(
        string tableName,
        out ITableMock? tb,
        string? schemaName = null)
    {
        ArgumentNullException.ThrowIfNull(tableName);
        var sc = GetSchemaName(schemaName);
        return this[sc].TryGetTable(tableName, out tb)
            && tb != null;
    }

    /// <summary>
    /// Verifica se uma tabela existe no schema informado.
    /// </summary>
    /// <param name="tableName">Nome da tabela.</param>
    /// <param name="schemaName">Schema alvo.</param>
    /// <returns>True se existir.</returns>
    public bool ContainsTable(
        string tableName,
        string? schemaName = null)
    {
        ArgumentNullException.ThrowIfNull(tableName);
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

    #endregion

    #region View

    internal void AddView(
        SqlCreateViewQuery query,
        string? schemaName = null)
    {
        var name = query.Table?.Name?.NormalizeName();
        ArgumentException.ThrowIfNullOrWhiteSpace(name);

        var sc = GetSchemaName(schemaName);
        var schema = (SchemaMock)this[sc];

        if (schema.Views.ContainsKey(name))
        {
            if (query.OrReplace)
            {
                schema.Views[name] = query.Select;
                return;
            }

            if (query.IfNotExists)
            {
                return; // não cria, não dá erro
            }

            throw new InvalidOperationException($"View '{name}' already exists.");
        }

        schema.Views[name] = query.Select;
    }


    internal SqlSelectQuery GetView(
        string viewName,
        string? schemaName = null)
    {
        ArgumentNullException.ThrowIfNull(viewName);
        var sc = GetSchemaName(schemaName);
        if (!((SchemaMock)this[sc]).Views.TryGetValue(viewName, out var vw)
            || vw == null)
            throw new Exception($"View não existe cadastrada {viewName}");
        return vw;
    }

    internal bool TryGetView(
        string viewName,
        out SqlSelectQuery? vw,
        string? schemaName = null)
    {
        ArgumentNullException.ThrowIfNull(viewName);
        var sc = GetSchemaName(schemaName);
        return ((SchemaMock)this[sc]).Views.TryGetValue(viewName, out vw)
            && vw != null;
    }

    #endregion

    #region Procedures

    /// <summary>
    /// Registra um procedimento armazenado no schema informado.
    /// </summary>
    /// <param name="procName">Nome do procedimento.</param>
    /// <param name="pr">Definição do procedimento.</param>
    /// <param name="schemaName">Schema alvo.</param>
    public void AddProdecure(
        string procName,
        ProcedureDef pr,
        string? schemaName = null)
    {
        ArgumentNullException.ThrowIfNull(procName);
        var sc = GetSchemaName(schemaName);
        if (!this.TryGetValue(sc, out var s))
            s = CreateSchema(sc);
        this[sc].Procedures[procName] = pr;
    }

    /// <summary>
    /// Tenta obter um procedimento armazenado pelo nome.
    /// </summary>
    /// <param name="procName">Nome do procedimento.</param>
    /// <param name="pr">Procedimento encontrado, se houver.</param>
    /// <param name="schemaName">Schema alvo.</param>
    /// <returns>True se existir.</returns>
    public bool TryGetProcedure(
        string procName,
        out ProcedureDef? pr,
        string? schemaName = null)
    {
        ArgumentNullException.ThrowIfNull(procName);
        var sc = GetSchemaName(schemaName);
        return this[sc].Procedures.TryGetValue(procName, out pr)
            && pr != null;
    }

    #endregion

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
        foreach (var (key, value) in this)
            yield return new KeyValuePair<string, ISchemaMock>(key, value);
    }
}
