using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using DbSqlLikeMem.SqlServer;
namespace DbSqlLikeMem.SqlAzure;

public class SqlAzureTableMock(
    string tableName,
    SqlAzureSchemaMock schema,
    IEnumerable<Col> columns,
    IEnumerable<Dictionary<int, object?>>? rows = null
    ) : SqlServerTableMock(tableName, schema, columns, rows)
{
    /// <summary>
    /// EN: Creates the SQL Azure unknown-column exception for invalid column access.
    /// PT: Cria a excecao de coluna desconhecida do SQL Azure para acesso de coluna invalida.
    /// </summary>
    public override Exception UnknownColumn(string columnName)
        => SqlAzureExceptionFactory.UnknownColumn(columnName);

    /// <summary>
    /// EN: Creates the SQL Azure duplicate-key exception for unique key violations.
    /// PT: Cria a excecao de chave duplicada do SQL Azure para violacoes de chave unica.
    /// </summary>
    public override Exception DuplicateKey(string tbl, string key, object? val)
        => SqlAzureExceptionFactory.DuplicateKey(tbl, key, val);

    /// <summary>
    /// EN: Creates the SQL Azure exception for null values in non-nullable columns.
    /// PT: Cria a excecao do SQL Azure para valores nulos em colunas nao anulaveis.
    /// </summary>
    public override Exception ColumnCannotBeNull(string col)
        => SqlAzureExceptionFactory.ColumnCannotBeNull(col);

    /// <summary>
    /// EN: Creates the SQL Azure foreign-key failure exception.
    /// PT: Cria a excecao de falha de chave estrangeira do SQL Azure.
    /// </summary>
    public override Exception ForeignKeyFails(string col, string refTbl)
        => SqlAzureExceptionFactory.ForeignKeyFails(col, refTbl);

    /// <summary>
    /// EN: Creates the SQL Azure referenced-row exception for delete/update restrictions.
    /// PT: Cria a excecao de linha referenciada do SQL Azure para restricoes de exclusao/atualizacao.
    /// </summary>
    public override Exception ReferencedRow(string tbl)
        => SqlAzureExceptionFactory.ReferencedRow(tbl);
}