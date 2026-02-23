namespace DbSqlLikeMem.SqlServer.LinqToDb.Test;

/// <summary>
/// EN: Executes shared LinqToDB smoke contract tests using the SqlServer provider connection factory.
/// PT: Executa testes compartilhados de contrato smoke de LinqToDB usando a fábrica de conexão do provedor SqlServer.
/// </summary>
public sealed class LinqToDbSmokeTests(
    ITestOutputHelper helper
) : LinqToDbSupportTestsBase(helper)
{
    /// <summary>
    /// EN: Creates the SqlServer LinqToDB connection factory used by shared contract tests.
    /// PT: Cria a fábrica de conexão LinqToDB de SqlServer usada pelos testes de contrato compartilhados.
    /// </summary>
    protected override IDbSqlLikeMemLinqToDbConnectionFactory CreateFactory()
        => new SqlServerLinqToDbConnectionFactory();
}
