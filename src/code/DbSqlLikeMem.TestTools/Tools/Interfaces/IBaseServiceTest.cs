
namespace DbSqlLikeMem.TestTools;

/// <summary>
/// EN: Represents a scenario runner that returns a value.
/// PT: Representa um executor de cenario que retorna um valor.
/// </summary>
public interface IBaseServiceTest
{
    /// <summary>
    /// EN: Executes the scenario and returns the computed value.
    /// PT: Executa o cenario e retorna o valor calculado.
    /// </summary>
    Task<object?> RunTestAsync(params object[] args);
}
