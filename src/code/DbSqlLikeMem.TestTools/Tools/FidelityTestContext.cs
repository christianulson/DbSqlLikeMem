namespace DbSqlLikeMem.TestTools;

/// <summary>
/// EN: Represents the context for a fidelity test, providing access to a collection of parameters that can be used to configure and execute the test scenario.
/// PT: Representa o contexto para um teste de fidelidade, fornecendo acesso a uma coleção de parâmetros que podem ser usados para configurar e executar o cenário de teste.
/// </summary>
public class FidelityTestContext
{
    /// <summary>
    /// Initializes a new instance of the FidelityTestContext class with a unique identifier.
    /// </summary>
    /// <remarks>The unique identifier is generated as a lowercase, 8-character string derived from a new
    /// GUID. This identifier can be used to distinguish between different test context instances.</remarks>
    public FidelityTestContext()
    {
        UId = Guid.NewGuid().ToString("N")[..8].ToLowerInvariant();
    }

    /// <summary>
    /// UId
    /// </summary>
    public string UId { get; private set; }

    /// <summary>
    /// Users Table Name
    /// </summary>
    public string TbUsers = "users";

    /// <summary>
    /// Gets the full name of the user by combining the user name and unique identifier.
    /// </summary>
    public string TbUsersFullName => $"{TbUsers}_{UId}";

    /// <summary>
    /// Gets or sets the value associated with the TbOrders property.
    /// </summary>
    public string TbOrders = "orders";

    /// <summary>
    /// Gets the fully qualified name of the orders table for the current user or context.
    /// </summary>
    public string TbOrdersFullName => $"{TbOrders}_{UId}";

    /// <summary>
    /// Gets or sets the value associated with the TempTb property, which may represent a temporary table name or identifier used in the test context.
    /// </summary>
    public string TempTb = "TempTable";

    /// <summary>
    /// Gets the fully qualified name of the temporary table for the current user or context by combining the TempTb property with the unique identifier (UId).
    /// </summary>
    public string TempTbFullName => $"{TempTb}_{UId}";

    /// <summary>
    /// Sequence Name
    /// </summary>
    public string Seq => $"Seq_{UId}";

    /// <summary>
    /// Sequence Name
    /// </summary>
    public string SavepointName => $"Savepoint_{UId}";
}
