namespace DbSqlLikeMem.SqlServer;

/// <summary>
/// EN: In-memory database mock configured for SQL Server.
/// PT: Banco de dados simulado em memória configurado para SQL Server.
/// </summary>
public class SqlServerDbMock : DbMock
{
    internal override SqlDialectBase Dialect { get; set; }

    /// <summary>
    /// EN: Initializes an in-memory SQL Server mock database with the requested version.
    /// PT: Inicializa um banco SQL Server simulado em memória com a versão informada.
    /// </summary>
    public SqlServerDbMock(
        int? version = null
        ) : base(version ?? 2022)
    {
        Dialect = new SqlServerDialect(Version);
    }
    /// <summary>
    /// EN: Creates a SQL Server schema mock instance.
    /// PT: Cria uma instância de simulado de schema do SQL Server.
    /// </summary>
    /// <param name="schemaName">EN: Schema name. PT: Nome do schema.</param>
    /// <param name="tables">EN: Initial tables. PT: Tabelas iniciais.</param>
    /// <returns>EN: Schema mock. PT: Mock de schema.</returns>
    protected override SchemaMock NewSchema(
        string schemaName,
        IDictionary<string, (IEnumerable<Col> columns, IEnumerable<Dictionary<int, object?>>? rows)>? tables = null
        ) => new SqlServerSchemaMock(schemaName, this, tables);
}
