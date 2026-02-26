namespace DbSqlLikeMem.Sqlite;

/// <summary>
/// EN: Table mock specialized for SQLite schema operations.
/// PT: Tabela simulada especializada para operações de esquema no SQLite.
/// </summary>
internal class SqliteTableMock(
        string tableName,
        SqliteSchemaMock schema,
        IEnumerable<Col> columns,
        IEnumerable<Dictionary<int, object?>>? rows = null
        ) : TableMock(tableName, schema, columns, rows)
{

    /// <summary>
    /// EN: Gets or sets the column name currently being resolved by value conversion helpers.
    /// PT: Obtém ou define o nome da coluna que está sendo resolvida pelos auxiliares de conversão de valor.
    /// </summary>
    public override string? CurrentColumn
    {
        get { return SqliteValueHelper.CurrentColumn; }
        set { SqliteValueHelper.CurrentColumn = value; }
    }

    /// <summary>
    /// EN: Resolves a SQL token to a typed value according to SQLite conversion rules.
    /// PT: Resolve um token SQL para um valor tipado conforme as regras de conversão do SQLite.
    /// </summary>
    public override object? Resolve(
        string token,
        DbType dbType,
        bool isNullable,
        IDataParameterCollection? pars = null,
        IReadOnlyDictionary<string, ColumnDef>? colDict = null)
    {
        var exp = SqliteValueHelper.Resolve(token, dbType, isNullable, pars, colDict);
        return exp;
    }

    /// <summary>
    /// EN: Creates the provider-specific exception used when a referenced column does not exist.
    /// PT: Cria a exceção específica do provedor quando a coluna referenciada não existe.
    /// </summary>
    public override Exception UnknownColumn(string columnName)
        => SqliteExceptionFactory.UnknownColumn(columnName);

    /// <summary>
    /// EN: Creates the provider-specific exception used for duplicate key violations.
    /// PT: Cria a exceção específica do provedor para violações de chave duplicada.
    /// </summary>
    public override Exception DuplicateKey(string tbl, string key, object? val)
        => SqliteExceptionFactory.DuplicateKey(tbl, key, val);

    /// <summary>
    /// EN: Creates the provider-specific exception used when a non-nullable column receives null.
    /// PT: Cria a exceção específica do provedor quando uma coluna obrigatória recebe valor nulo.
    /// </summary>
    public override Exception ColumnCannotBeNull(string col)
        => SqliteExceptionFactory.ColumnCannotBeNull(col);

    /// <summary>
    /// EN: Creates the provider-specific exception used for foreign key violations.
    /// PT: Cria a exceção específica do provedor para violações de chave estrangeira.
    /// </summary>
    public override Exception ForeignKeyFails(string col, string refTbl)
        => SqliteExceptionFactory.ForeignKeyFails(col, refTbl);

    /// <summary>
    /// EN: Creates the provider-specific exception used when a referenced row prevents deletion.
    /// PT: Cria a exceção específica do provedor quando uma linha referenciada impede a exclusão.
    /// </summary>
    public override Exception ReferencedRow(string tbl)
        => SqliteExceptionFactory.ReferencedRow(tbl);
}
