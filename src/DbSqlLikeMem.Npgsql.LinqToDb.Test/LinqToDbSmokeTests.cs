namespace DbSqlLikeMem.Npgsql.LinqToDb.Test;

/// <summary>
/// EN: Executes shared LinqToDB smoke contract tests using the Npgsql provider connection factory.
/// PT: Executa testes compartilhados de contrato smoke de LinqToDB usando a fábrica de conexão do provedor Npgsql.
/// </summary>
public sealed class LinqToDbSmokeTests : LinqToDbSupportTestsBase
{
    /// <summary>
    /// EN: Creates the Npgsql LinqToDB connection factory used by shared contract tests.
    /// PT: Cria a fábrica de conexão LinqToDB de Npgsql usada pelos testes de contrato compartilhados.
    /// </summary>
    protected override IDbSqlLikeMemLinqToDbConnectionFactory CreateFactory()
        => new NpgsqlLinqToDbConnectionFactory();
}
