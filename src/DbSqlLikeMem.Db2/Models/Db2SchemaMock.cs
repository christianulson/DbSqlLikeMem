namespace DbSqlLikeMem.Db2;

/// <summary>
/// EN: Schema mock for DB2 databases.
/// PT: Mock de esquema para bancos DB2.
/// </summary>
public class Db2SchemaMock(
    string schemaName,
    Db2DbMock db,
    IDictionary<string, (IEnumerable<Col> columns, IEnumerable<Dictionary<int, object?>>? rows)>? tables = null
    ) : SchemaMock(schemaName, db, tables)
{
    /// <summary>
    /// EN: Creates a DB2 table mock for this schema.
    /// PT: Cria um mock de tabela DB2 para este schema.
    /// </summary>
    /// <param name="tableName">EN: Table name. PT: Nome da tabela.</param>
    /// <param name="columns">EN: Table columns. PT: Colunas da tabela.</param>
    /// <param name="rows">EN: Initial rows. PT: Linhas iniciais.</param>
    /// <returns>EN: Table mock. PT: Mock de tabela.</returns>
    protected override TableMock NewTable(
        string tableName,
        IEnumerable<Col> columns,
        IEnumerable<Dictionary<int, object?>>? rows = null)
        => new Db2TableMock(tableName, this, columns, rows);
}
