namespace DbSqlLikeMem.EfCore;

/// <summary>
/// EN: Defines a provider-specific factory that creates opened mock ADO.NET connections for EF Core scenarios.
/// PT-br: Define uma fábrica específica de provedor que cria conexões ADO.NET simulado abertas para cenários de EF Core.
/// </summary>
public interface IDbSqlLikeMemEfCoreConnectionFactory
{
    /// <summary>
    /// EN: Creates and opens a provider mock connection that can be plugged into EF Core relational providers.
    /// PT-br: Cria e abre uma conexão simulada do provedor que pode ser conectada a providers relacionais do EF Core.
    /// </summary>
    DbConnection CreateOpenConnection();
}
