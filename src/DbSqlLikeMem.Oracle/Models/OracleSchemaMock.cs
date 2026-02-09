namespace DbSqlLikeMem.Oracle;

/// <summary>
/// EN: Schema mock for Oracle databases.
/// PT: Mock de esquema para bancos Oracle.
/// </summary>
public class OracleSchemaMock(
    string schemaName,
    OracleDbMock db,
    IDictionary<string, (IColumnDictionary columns, IEnumerable<Dictionary<int, object?>>? rows)>? tables = null
    ) : SchemaMock(schemaName, db, tables)
{
    /// <summary>
    /// EN: Creates an Oracle table mock for this schema.
    /// PT: Cria um mock de tabela Oracle para este schema.
    /// </summary>
    /// <param name="tableName">EN: Table name. PT: Nome da tabela.</param>
    /// <param name="columns">EN: Table columns. PT: Colunas da tabela.</param>
    /// <param name="rows">EN: Initial rows. PT: Linhas iniciais.</param>
    /// <returns>EN: Table mock. PT: Mock de tabela.</returns>
    protected override TableMock NewTable(
        string tableName,
        IColumnDictionary columns,
        IEnumerable<Dictionary<int, object?>>? rows = null)
        => new OracleTableMock(tableName, this, columns, rows);
}
