namespace DbSqlLikeMem.Firebird;

/// <summary>
/// EN: In-memory database mock configured for Firebird.
/// PT: Banco de dados simulado em memória configurado para Firebird.
/// </summary>
public class FirebirdDbMock : DbMock
{
    internal override SqlDialectBase Dialect { get; set; }

    /// <summary>
    /// EN: Initializes an in-memory Firebird mock database with the requested version.
    /// PT: Inicializa um banco Firebird simulado em memória com a versão informada.
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
    /// PT: Cria uma instância de simulado de schema Firebird.
    /// </summary>
    /// <param name="schemaName">EN: Schema name. PT: Nome do schema.</param>
    /// <param name="tables">EN: Initial tables. PT: Tabelas iniciais.</param>
    /// <returns>EN: Schema mock. PT: Mock de schema.</returns>
    protected override SchemaMock NewSchema(
        string schemaName,
        IDictionary<string, (IEnumerable<Col> columns, IEnumerable<Dictionary<int, object?>>? rows)>? tables = null
        ) => new FirebirdSchemaMock(schemaName, this, tables);
}

