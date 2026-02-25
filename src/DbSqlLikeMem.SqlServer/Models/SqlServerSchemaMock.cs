namespace DbSqlLikeMem.SqlServer;

/// <summary>
/// EN: Schema mock for SQL Server databases.
/// PT: Esquema simulado para bancos SQL Server.
/// </summary>
public class SqlServerSchemaMock(
    string schemaName,
    SqlServerDbMock db,
    IDictionary<string, (IEnumerable<Col> columns, IEnumerable<Dictionary<int, object?>>? rows)>? tables = null
    ) : SchemaMock(schemaName, db, tables)
{
    /// <summary>
    /// EN: Creates a SQL Server table mock for this schema.
    /// PT: Cria um simulado de tabela SQL Server para este schema.
    /// </summary>
    /// <param name="tableName">EN: Table name. PT: Nome da tabela.</param>
    /// <param name="columns">EN: Table columns. PT: Colunas da tabela.</param>
    /// <param name="rows">EN: Initial rows. PT: Linhas iniciais.</param>
    /// <returns>EN: Table mock. PT: Mock de tabela.</returns>
    protected override TableMock NewTable(
        string tableName,
        IEnumerable<Col> columns,
        IEnumerable<Dictionary<int, object?>>? rows = null)
        => new SqlServerTableMock(tableName, this, columns, rows);
}
