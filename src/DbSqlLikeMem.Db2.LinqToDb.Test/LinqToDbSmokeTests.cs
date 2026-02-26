namespace DbSqlLikeMem.Db2.LinqToDb.Test;

/// <summary>
/// EN: Executes shared LinqToDB smoke contract tests using the Db2 provider connection factory.
/// PT: Executa testes compartilhados de contrato smoke de LinqToDB usando a fábrica de conexão do provedor Db2.
/// </summary>
public sealed class LinqToDbSmokeTests(
    ITestOutputHelper helper
) : LinqToDbSupportTestsBase(helper)
{
    /// <summary>
    /// EN: Creates the Db2 LinqToDB connection factory used by shared contract tests.
    /// PT: Cria a fábrica de conexão LinqToDB de Db2 usada pelos testes de contrato compartilhados.
    /// </summary>
    protected override IDbSqlLikeMemLinqToDbConnectionFactory CreateFactory()
        => new Db2LinqToDbConnectionFactory();
}
