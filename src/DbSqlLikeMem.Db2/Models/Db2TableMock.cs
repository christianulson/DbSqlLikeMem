namespace DbSqlLikeMem.Db2;

/// <summary>
/// EN: Table mock specialized for DB2 schema operations.
/// PT: Tabela simulada especializada para operações de esquema no Db2.
/// </summary>
internal class Db2TableMock(
        string tableName,
        Db2SchemaMock schema,
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
        get { return Db2ValueHelper.CurrentColumn; }
        set { Db2ValueHelper.CurrentColumn = value; }
    }

    /// <summary>
    /// EN: Resolves a SQL token to a typed value according to Db2 conversion rules.
    /// PT: Resolve um token SQL para um valor tipado conforme as regras de conversão do Db2.
    /// </summary>
    public override object? Resolve(
        string token,
        DbType dbType,
        bool isNullable,
        IDataParameterCollection? pars = null,
        IReadOnlyDictionary<string, ColumnDef>? colDict = null)
    {
        var exp = Db2ValueHelper.Resolve(token, dbType, isNullable, pars, colDict);
        return exp;
    }

    /// <summary>
    /// EN: Creates the provider-specific exception used when a referenced column does not exist.
    /// PT: Cria a exceção específica do provedor quando a coluna referenciada não existe.
    /// </summary>
    public override Exception UnknownColumn(string columnName)
        => Db2ExceptionFactory.UnknownColumn(columnName);

    /// <summary>
    /// EN: Creates the provider-specific exception used for duplicate key violations.
    /// PT: Cria a exceção específica do provedor para violações de chave duplicada.
    /// </summary>
    public override Exception DuplicateKey(string tbl, string key, object? val)
        => Db2ExceptionFactory.DuplicateKey(tbl, key, val);

    /// <summary>
    /// EN: Creates the provider-specific exception used when a non-nullable column receives null.
    /// PT: Cria a exceção específica do provedor quando uma coluna obrigatória recebe valor nulo.
    /// </summary>
    public override Exception ColumnCannotBeNull(string col)
        => Db2ExceptionFactory.ColumnCannotBeNull(col);

    /// <summary>
    /// EN: Creates the provider-specific exception used for foreign key violations.
    /// PT: Cria a exceção específica do provedor para violações de chave estrangeira.
    /// </summary>
    public override Exception ForeignKeyFails(string col, string refTbl)
        => Db2ExceptionFactory.ForeignKeyFails(col, refTbl);

    /// <summary>
    /// EN: Creates the provider-specific exception used when a referenced row prevents deletion.
    /// PT: Cria a exceção específica do provedor quando uma linha referenciada impede a exclusão.
    /// </summary>
    public override Exception ReferencedRow(string tbl)
        => Db2ExceptionFactory.ReferencedRow(tbl);
}
