namespace DbSqlLikeMem.Firebird;

/// <summary>
/// EN: In-memory table mock configured for Firebird.
/// PT: Tabela simulada em memória configurada para Firebird.
/// </summary>
public class FirebirdTableMock(
    string tableName,
    SchemaMock schema,
    IEnumerable<Col> columns,
    IEnumerable<Dictionary<int, object?>>? rows = null
) : TableMock(tableName, schema, columns, rows)
{
    /// <summary>
    /// EN: Gets or sets the column name currently being resolved by Firebird value helpers.
    /// PT: Obtém ou define o nome da coluna que está sendo resolvida pelos auxiliares de valor do Firebird.
    /// </summary>
    public override string? CurrentColumn
    {
        get => FirebirdValueHelper.CurrentColumn;
        set => FirebirdValueHelper.CurrentColumn = value;
    }

    /// <summary>
    /// EN: Resolves a SQL token to a typed value using Firebird conversion rules.
    /// PT: Resolve um token SQL para um valor tipado usando regras de conversão do Firebird.
    /// </summary>
    public override object? Resolve(
        string token,
        DbType dbType,
        bool isNullable,
        IDataParameterCollection? pars = null,
        IReadOnlyDictionary<string, ColumnDef>? colDict = null)
        => FirebirdValueHelper.Resolve(token, dbType, isNullable, pars, colDict);

    /// <summary>
    /// EN: Creates the provider-specific exception used when a referenced column does not exist.
    /// PT: Cria a exceção específica do provedor quando a coluna referenciada não existe.
    /// </summary>
    public override Exception UnknownColumn(string columnName)
        => FirebirdExceptionFactory.UnknownColumn(columnName);

    /// <summary>
    /// EN: Creates the provider-specific exception used for duplicate key violations.
    /// PT: Cria a exceção específica do provedor para violações de chave duplicada.
    /// </summary>
    public override Exception DuplicateKey(string tbl, string key, object? val)
        => FirebirdExceptionFactory.DuplicateKey(tbl, key, val);

    /// <summary>
    /// EN: Creates the provider-specific exception used when a non-nullable column receives null.
    /// PT: Cria a exceção específica do provedor quando uma coluna obrigatória recebe valor nulo.
    /// </summary>
    public override Exception ColumnCannotBeNull(string col)
        => FirebirdExceptionFactory.ColumnCannotBeNull(col);

    /// <summary>
    /// EN: Creates the provider-specific exception used for foreign key violations.
    /// PT: Cria a exceção específica do provedor para violações de chave estrangeira.
    /// </summary>
    public override Exception ForeignKeyFails(string col, string refTbl)
        => FirebirdExceptionFactory.ForeignKeyFails(col, refTbl);

    /// <summary>
    /// EN: Creates the provider-specific exception used when a referenced row prevents deletion.
    /// PT: Cria a exceção específica do provedor quando uma linha referenciada impede a exclusão.
    /// </summary>
    public override Exception ReferencedRow(string tbl)
        => FirebirdExceptionFactory.ReferencedRow(tbl);
}

