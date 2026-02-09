namespace DbSqlLikeMem.Npgsql;

/// <summary>
/// EN: In-memory database mock configured for Npgsql.
/// PT: Mock de banco em memória configurado para Npgsql.
/// </summary>
public class NpgsqlDbMock : DbMock
{
    internal override SqlDialectBase Dialect { get; set; }

    public NpgsqlDbMock(
    int? version = null
    ) : base(version ?? 17)
    {
        Dialect = new NpgsqlDialect(Version);
    }

    /// <summary>
    /// EN: Creates a PostgreSQL schema mock instance.
    /// PT: Cria uma instância de mock de schema PostgreSQL.
    /// </summary>
    /// <param name="schemaName">EN: Schema name. PT: Nome do schema.</param>
    /// <param name="tables">EN: Initial tables. PT: Tabelas iniciais.</param>
    /// <returns>EN: Schema mock. PT: Mock de schema.</returns>
    protected override SchemaMock NewSchema(
        string schemaName,
        IDictionary<string, (IColumnDictionary columns, IEnumerable<Dictionary<int, object?>>? rows)>? tables = null
        ) => new NpgsqlSchemaMock(schemaName, this, tables);
}
