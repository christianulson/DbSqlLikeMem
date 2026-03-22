
namespace DbSqlLikeMem.TestTools;

/// <summary>
/// EN: Represents a scenario runner that does not return a value.
/// PT: Representa um executor de cenario que nao retorna valor.
/// </summary>
public interface IBaseServiceTest
{
    /// <summary>
    /// EN: Executes the scenario with the supplied parameters.
    /// PT: Executa o cenario com os parametros informados.
    /// </summary>
    public abstract void RunTest(params object[] pars);
}

/// <summary>
/// EN: Represents a scenario runner that returns a value.
/// PT: Representa um executor de cenario que retorna um valor.
/// </summary>
public interface IBaseServiceWithReturnTest<T2>
{
    /// <summary>
    /// EN: Executes the scenario and returns the computed value.
    /// PT: Executa o cenario e retorna o valor calculado.
    /// </summary>
    public abstract T2 RunTest(params object[] pars);
}
