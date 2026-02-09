namespace DbSqlLikeMem.SqlServer;

public class SqlServerDbMock : DbMock
{
    internal override SqlDialectBase Dialect { get; set; }

    public SqlServerDbMock(
        int? version = null
        ) : base(version ?? 2022)
    {
        Dialect = new SqlServerDialect(Version);
    }
    /// <summary>
    /// EN: Creates a SQL Server schema mock instance.
    /// PT: Cria uma instância de mock de schema do SQL Server.
    /// </summary>
    /// <param name="schemaName">EN: Schema name. PT: Nome do schema.</param>
    /// <param name="tables">EN: Initial tables. PT: Tabelas iniciais.</param>
    /// <returns>EN: Schema mock. PT: Mock de schema.</returns>
    protected override SchemaMock NewSchema(
        string schemaName,
        IDictionary<string, (IColumnDictionary columns, IEnumerable<Dictionary<int, object?>>? rows)>? tables = null
        ) => new SqlServerSchemaMock(schemaName, this, tables);
}
