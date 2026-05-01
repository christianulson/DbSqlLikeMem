namespace DbSqlLikeMem.Oracle;

/// <summary>
/// EN: Schema mock for Oracle databases.
/// PT-br: simulado de esquema para bancos Oracle.
/// </summary>
public class OracleSchemaMock(
    string schemaName,
    OracleDbMock db,
    IDictionary<string, (IEnumerable<Col> columns, IEnumerable<Dictionary<int, object?>>? rows)>? tables = null
    ) : SchemaMock(schemaName, db, tables)
{
    /// <summary>
    /// EN: Creates an Oracle table mock for this schema.
    /// PT-br: Cria um simulado de tabela Oracle para este schema.
    /// </summary>
    /// <param name="tableName">EN: Table name. PT-br: Nome da tabela.</param>
    /// <param name="columns">EN: Table columns. PT-br: Colunas da tabela.</param>
    /// <param name="rows">EN: Initial rows. PT-br: Linhas iniciais.</param>
    /// <returns>EN: Table mock. PT-br: Mock de tabela.</returns>
    protected override TableMock NewTable(
        string tableName,
        IEnumerable<Col> columns,
        IEnumerable<Dictionary<int, object?>>? rows = null)
        => new OracleTableMock(tableName, this, columns, rows);
}
