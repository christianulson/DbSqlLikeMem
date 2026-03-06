namespace DbSqlLikeMem.Npgsql.LinqToDb.Test;

/// <summary>
/// EN: Executes shared LinqToDB smoke contract tests using the Npgsql provider connection factory.
/// PT: Executa testes compartilhados de contrato smoke de LinqToDB usando a fábrica de conexão do provedor Npgsql.
/// </summary>
public sealed class LinqToDbSmokeTests(
    ITestOutputHelper helper
) : LinqToDbSmokeTestsBase(helper, static () => new NpgsqlLinqToDbConnectionFactory());
