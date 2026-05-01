namespace DbSqlLikeMem.LinqToDb;

/// <summary>
/// EN: Defines a provider-specific factory that creates opened mock ADO.NET connections for LinqToDB scenarios.
/// PT-br: Define uma fábrica específica de provedor que cria conexões ADO.NET simulado abertas para cenários com LinqToDB.
/// </summary>
public interface IDbSqlLikeMemLinqToDbConnectionFactory
{
    /// <summary>
    /// EN: Creates and opens a provider mock connection that can be consumed by LinqToDB data connections.
    /// PT-br: Cria e abre uma conexão simulada do provedor que pode ser consumida por conexões de dados do LinqToDB.
    /// </summary>
    DbConnection CreateOpenConnection();
}
