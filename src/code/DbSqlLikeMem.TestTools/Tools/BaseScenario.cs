namespace DbSqlLikeMem.TestTools;

/// <summary>
/// Provides a base class for scenario implementations, supplying access to repository services, SQL dialect
/// information, and a parameter collection for use within derived scenarios.
/// </summary>
/// <param name="repo">The repository service used to execute SQL commands and queries within the scenario. Cannot be null.</param>
/// <param name="context">The context for the fidelity test, providing access to a collection of parameters that can be used to configure and execute the test scenario. Cannot be null.</param>
public class BaseScenario(
    RepoService repo,
    FidelityTestContext context)
{
    /// <summary>
    /// EN: Provides access to the repository service for executing SQL commands and queries within the scenario.
    /// PT-br: Fornece acesso ao serviço de repositório para executar comandos e consultas SQL dentro do cenário.
    /// </summary>
    public RepoService Repo => repo;

    /// <summary>
    /// EN: Provides access to the SQL dialect used by the provider for formatting and executing SQL statements within the scenario.
    /// PT-br: Fornece acesso ao dialeto SQL usado pelo provedor para formatar e executar instruções SQL dentro do cenário.
    /// </summary>
    public FidelityTestContext Context => context;

    ///// <summary>
    ///// EN: Generates a new unique token that can be used for naming tables or other objects in the scenario. The token is an 8-character uppercase string derived from a GUID, ensuring uniqueness across different runs of the scenario.
    ///// PT-br: Gera um novo token único que pode ser usado para nomear tabelas ou outros objetos no cenário. O token é uma string maiúscula de 8 caracteres derivada de um GUID, garantindo unicidade em diferentes execuções do cenário.
    ///// </summary>
    ///// <returns>EN: A new unique token. PT-br: Um novo token único.</returns>
    //protected static string NewToken()
    //    => Guid.NewGuid().ToString("N")[..8].ToUpperInvariant();
}
