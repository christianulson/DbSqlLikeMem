namespace DbSqlLikeMem;

/// <summary>
/// EN: Creates ADO.NET connections already wrapped by the interception pipeline.
/// PT: Cria conexoes ADO.NET ja encapsuladas pelo pipeline de interceptacao.
/// </summary>
public interface IDbInterceptionConnectionFactory
{
    /// <summary>
    /// EN: Creates a wrapped connection.
    /// PT: Cria uma conexao encapsulada.
    /// </summary>
    /// <returns>EN: Wrapped connection. PT: Conexao encapsulada.</returns>
    DbConnection CreateConnection();

    /// <summary>
    /// EN: Creates and opens a wrapped connection.
    /// PT: Cria e abre uma conexao encapsulada.
    /// </summary>
    /// <returns>EN: Open wrapped connection. PT: Conexao encapsulada aberta.</returns>
    DbConnection CreateOpenConnection();
}
