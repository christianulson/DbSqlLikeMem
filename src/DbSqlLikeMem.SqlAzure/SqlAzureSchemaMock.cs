using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using DbSqlLikeMem.SqlServer;
namespace DbSqlLikeMem.SqlAzure;

public class SqlAzureSchemaMock(
    string schemaName,
    SqlAzureDbMock db,
    IDictionary<string, (IEnumerable<Col> columns, IEnumerable<Dictionary<int, object?>>? rows)>? tables = null
    ) : SqlServerSchemaMock(schemaName, db, tables)
{
    /// <summary>
    /// EN: Creates a SQL Azure table mock inside this schema.
    /// PT: Cria uma tabela simulada do SQL Azure dentro deste esquema.
    /// </summary>
    protected override TableMock NewTable(
        string tableName,
        IEnumerable<Col> columns,
        IEnumerable<Dictionary<int, object?>>? rows = null)
        => new SqlAzureTableMock(tableName, this, columns, rows);
}