using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using DbSqlLikeMem.SqlServer;
namespace DbSqlLikeMem.SqlAzure;

public class SqlAzureDbMock : SqlServerDbMock
{
    /// <summary>
    /// EN: Creates an in-memory SQL Azure database mock for the provided compatibility version.
    /// PT: Cria um banco simulado em memoria do SQL Azure para a versao de compatibilidade informada.
    /// </summary>
    public SqlAzureDbMock(int? version = null) : base(version ?? SqlAzureDbCompatibilityLevels.Default)
    {
        Dialect = new SqlServerDialect(SqlAzureDbCompatibilityLevels.ToSqlServerDialectVersion(Version));
    }

    /// <summary>
    /// EN: Creates a SQL Azure schema mock attached to this database.
    /// PT: Cria um esquema simulado do SQL Azure associado a este banco.
    /// </summary>
    protected override SchemaMock NewSchema(
        string schemaName,
        IDictionary<string, (IEnumerable<Col> columns, IEnumerable<Dictionary<int, object?>>? rows)>? tables = null)
        => new SqlAzureSchemaMock(schemaName, this, tables);
}