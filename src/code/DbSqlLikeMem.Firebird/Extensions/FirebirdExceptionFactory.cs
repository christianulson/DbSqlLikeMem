namespace DbSqlLikeMem.Firebird;

internal static class FirebirdExceptionFactory
{
    /// <summary>
    /// EN: Creates the provider-specific exception used for duplicate key violations.
    /// PT: Cria a exceção específica do provedor para violações de chave duplicada.
    /// </summary>
    public static Exception DuplicateKey(string tbl, string key, object? val)
        => new FirebirdMockException(SqlExceptionMessages.DuplicateKey(val, key), 1062, "23000");

    /// <summary>
    /// EN: Creates the provider-specific exception used when a referenced column does not exist.
    /// PT: Cria a exceção específica do provedor quando a coluna referenciada não existe.
    /// </summary>
    public static Exception UnknownColumn(string columnName)
        => new FirebirdMockException(SqlExceptionMessages.UnknownColumn(columnName), 1054);

    /// <summary>
    /// EN: Creates the provider-specific exception used when a non-nullable column receives null.
    /// PT: Cria a exceção específica do provedor quando uma coluna obrigatória recebe valor nulo.
    /// </summary>
    public static Exception ColumnCannotBeNull(string col)
        => new FirebirdMockException(SqlExceptionMessages.ColumnCannotBeNull(col), 1048, "23000");

    /// <summary>
    /// EN: Creates the provider-specific exception used for foreign key violations.
    /// PT: Cria a exceção específica do provedor para violações de chave estrangeira.
    /// </summary>
    public static Exception ForeignKeyFails(string col, string refTbl)
        => new FirebirdMockException(SqlExceptionMessages.ForeignKeyFails(col, refTbl), 1452, "23000");

    /// <summary>
    /// EN: Creates the provider-specific exception used when a referenced row prevents deletion.
    /// PT: Cria a exceção específica do provedor quando uma linha referenciada impede a exclusão.
    /// </summary>
    public static Exception ReferencedRow(string tbl)
        => new FirebirdMockException(SqlExceptionMessages.ReferencedRow(tbl), 1451, "23000");
}
