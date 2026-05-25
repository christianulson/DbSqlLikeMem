namespace DbSqlLikeMem.Oracle;

/// <summary>
/// EN: In-memory database mock configured for Oracle.
/// PT-br: simulado de banco em memória configurado para Oracle.
/// </summary>
public class OracleDbMock : DbMock
{
    internal override SqlDialectBase Dialect { get; set; }

    /// <inheritdoc />
    public override bool GlobalTemporaryTablesShareDefinitionAcrossConnections => true;

    /// <inheritdoc />
    public override bool GlobalTemporaryTablesShareRowsAcrossConnections => false;

    /// <summary>
    /// EN: Implements OracleDbMock.
    /// PT-br: Implementa OracleDbMock.
    /// </summary>
    public OracleDbMock(
        int? version = null
        ) : base(version ?? 23)
    {
        Dialect = new OracleDialect(Version);
    }

    /// <summary>
    /// EN: Creates an Oracle schema mock instance.
    /// PT-br: Cria uma instância de simulado de schema Oracle.
    /// </summary>
    /// <param name="schemaName">EN: Schema name. PT-br: Nome do schema.</param>
    /// <param name="tables">EN: Initial tables. PT-br: Tabelas iniciais.</param>
    /// <returns>EN: Schema mock. PT-br: Mock de schema.</returns>
    protected override SchemaMock NewSchema(
        string schemaName,
        IDictionary<string, (IEnumerable<Col> columns, IEnumerable<Dictionary<int, object?>>? rows)>? tables = null
        ) => new OracleSchemaMock(schemaName, this, tables);
}
