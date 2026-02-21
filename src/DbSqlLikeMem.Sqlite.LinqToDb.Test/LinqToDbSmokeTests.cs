namespace DbSqlLikeMem.Sqlite.LinqToDb.Test;

/// <summary>
/// EN: Executes shared LinqToDB smoke contract tests using the Sqlite provider connection factory.
/// PT: Executa testes compartilhados de contrato smoke de LinqToDB usando a fábrica de conexão do provedor Sqlite.
/// </summary>
public sealed class LinqToDbSmokeTests : LinqToDbSupportTestsBase
{
    /// <summary>
    /// EN: Creates the Sqlite LinqToDB connection factory used by shared contract tests.
    /// PT: Cria a fábrica de conexão LinqToDB de Sqlite usada pelos testes de contrato compartilhados.
    /// </summary>
    protected override IDbSqlLikeMemLinqToDbConnectionFactory CreateFactory()
        => new SqliteLinqToDbConnectionFactory();
}
