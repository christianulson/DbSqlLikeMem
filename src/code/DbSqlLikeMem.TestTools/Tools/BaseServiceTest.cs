namespace DbSqlLikeMem.TestTools;

/// <summary>
/// EN: Describes shared connection and SQL execution helpers for scenario-based tests.
/// PT: Descreve helpers compartilhados de conexao e execucao SQL para testes baseados em cenarios.
/// </summary>
public abstract class BaseServiceTest(
        RepoService repo,
        FidelityTestContext context)
{
    /// <summary>
    /// EN: Gets the connection used by the current scenario.
    /// PT: Obtem a conexao usada pelo cenario atual.
    /// </summary>
    public RepoService Repo => repo;

    /// <summary>
    /// EN: Provides access to the SQL dialect used by the provider for formatting and executing SQL statements within the scenario.
    /// PT: Fornece acesso ao dialeto SQL usado pelo provedor para formatar e executar instruções SQL dentro do cenário.
    /// </summary>
    public FidelityTestContext Context = context;

    /// <summary>
    /// EN: Adds a parameter through the current provider dialect.
    /// PT: Adiciona um parametro atraves do dialeto atual do provedor.
    /// </summary>
    /// <param name="command"></param>
    /// <param name="name"></param>
    /// <param name="dbType"></param>
    /// <param name="value"></param>
    protected virtual void AddParameter(
        DbCommand command,
        string name,
        DbType dbType,
        object? value)
        => Repo.Dialect.AddParameter(command, name, dbType, value);
}
