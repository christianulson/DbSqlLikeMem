namespace DbSqlLikeMem.Firebird;

/// <summary>
/// EN: In-memory database mock configured for Firebird.
/// PT-br: Banco de dados simulado em memória configurado para Firebird.
/// </summary>
public class FirebirdDbMock : DbMock
{
    internal override SqlDialectBase Dialect { get; set; }

    /// <inheritdoc />
    public override bool GlobalTemporaryTablesShareDefinitionAcrossConnections => true;

    /// <inheritdoc />
    public override bool GlobalTemporaryTablesShareRowsAcrossConnections => false;

    /// <summary>
    /// EN: Initializes an in-memory Firebird mock database with the requested version.
    /// PT-br: Inicializa um banco Firebird simulado em memória com a versão informada.
    /// </summary>
    public FirebirdDbMock(
        int? version = null
        ) : base(version ?? FirebirdDbVersions.Default)
    {
        Dialect = new FirebirdDialect(Version);
        AddTable(
            "RDB$DATABASE",
            [
                new("DUMMY", DbType.Int32, false)
            ],
            [
                new Dictionary<int, object?> { [0] = 1 }
            ]);
    }

    /// <summary>
    /// EN: Creates a Firebird schema mock instance.
    /// PT-br: Cria uma instância de simulado de schema Firebird.
    /// </summary>
    /// <param name="schemaName">EN: Schema name. PT-br: Nome do schema.</param>
    /// <param name="tables">EN: Initial tables. PT-br: Tabelas iniciais.</param>
    /// <returns>EN: Schema mock. PT-br: Mock de schema.</returns>
    protected override SchemaMock NewSchema(
        string schemaName,
        IDictionary<string, (IEnumerable<Col> columns, IEnumerable<Dictionary<int, object?>>? rows)>? tables = null
        ) => new FirebirdSchemaMock(schemaName, this, tables);
}

