namespace DbSqlLikeMem.Npgsql.EfCore.Test;

/// <summary>
/// EN: Executes shared EF Core smoke contract tests using the Npgsql provider connection factory.
/// PT: Executa testes compartilhados de contrato smoke de EF Core usando a fábrica de conexão do provedor Npgsql.
/// </summary>
public sealed class EfCoreSmokeTests : EfCoreSupportTestsBase
{
    /// <summary>
    /// EN: Creates the Npgsql EF Core connection factory used by the shared contract tests.
    /// PT: Cria a fábrica de conexão EF Core de Npgsql usada pelos testes de contrato compartilhados.
    /// </summary>
    protected override IDbSqlLikeMemEfCoreConnectionFactory CreateFactory()
        => new NpgsqlEfCoreConnectionFactory();
}
