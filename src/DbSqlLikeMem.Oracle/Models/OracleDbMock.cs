namespace DbSqlLikeMem.Oracle;

/// <summary>
/// EN: In-memory database mock configured for Oracle.
/// PT: simulado de banco em memória configurado para Oracle.
/// </summary>
public class OracleDbMock : DbMock
{
    internal override SqlDialectBase Dialect { get; set; }

    /// <summary>
    /// EN: Implements OracleDbMock.
    /// PT: Implementa OracleDbMock.
    /// </summary>
    public OracleDbMock(
        int? version = null
        ) : base(version ?? 23)
    {
        Dialect = new OracleDialect(Version);
    }

    /// <summary>
    /// EN: Creates an Oracle schema mock instance.
    /// PT: Cria uma instância de simulado de schema Oracle.
    /// </summary>
    /// <param name="schemaName">EN: Schema name. PT: Nome do schema.</param>
    /// <param name="tables">EN: Initial tables. PT: Tabelas iniciais.</param>
    /// <returns>EN: Schema mock. PT: Mock de schema.</returns>
    protected override SchemaMock NewSchema(
        string schemaName,
        IDictionary<string, (IEnumerable<Col> columns, IEnumerable<Dictionary<int, object?>>? rows)>? tables = null
        ) => new OracleSchemaMock(schemaName, this, tables);
}
