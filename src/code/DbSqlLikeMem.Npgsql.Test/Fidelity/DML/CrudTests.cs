using DbSqlLikeMem.Npgsql.TestTools;
using DbSqlLikeMem.TestTools.Tests.DML;

namespace DbSqlLikeMem.Npgsql.Test.Fidelity.DML;

/// <summary>
/// EN: Runs PostgreSQL fidelity tests for the shared CRUD workflows.
/// PT: Executa testes de fidelidade PostgreSQL para os fluxos compartilhados de CRUD.
/// </summary>
public class CrudTests(
    ITestOutputHelper helper
    ) : CrudTestsBase<NpgsqlConnectionMock, NpgsqlConnection>(
    helper,
    new NpgsqlProviderSqlDialect(),
    () => new NpgsqlConnectionMock(),
    s => new NpgsqlConnection(s)
    )
{
    /// <inheritdoc />
    protected override DateTime NormalizeParameterDateTimeInput(DateTime value)
        => value.Kind == DateTimeKind.Unspecified
            ? DateTime.SpecifyKind(value, DateTimeKind.Utc)
            : value;
}
