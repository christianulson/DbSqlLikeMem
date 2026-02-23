namespace DbSqlLikeMem.Db2;

/// <summary>
/// EN: In-memory database mock configured for DB2.
/// PT: Banco de dados simulado em memória configurado para Db2.
/// </summary>
public class Db2DbMock
    : DbMock
{
    internal override SqlDialectBase Dialect { get; set; }

    /// <summary>
    /// EN: Initializes an in-memory Db2 mock database with the requested version.
    /// PT: Inicializa um banco Db2 simulado em memória com a versão informada.
    /// </summary>
    public Db2DbMock(
        int? version = null
        ): base(version ?? 11)
    {
        Dialect = new Db2Dialect(Version);
    }

    /// <summary>
    /// EN: Creates a DB2 schema mock instance.
    /// PT: Cria uma instância de simulado de schema DB2.
    /// </summary>
    /// <param name="schemaName">EN: Schema name. PT: Nome do schema.</param>
    /// <param name="tables">EN: Initial tables. PT: Tabelas iniciais.</param>
    /// <returns>EN: Schema mock. PT: Mock de schema.</returns>
    protected override SchemaMock NewSchema(
        string schemaName,
        IDictionary<string, (IEnumerable<Col> columns, IEnumerable<Dictionary<int, object?>>? rows)>? tables = null
        ) => new Db2SchemaMock(schemaName, this, tables);
}
