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
    /// EN: Adds a parameter to the provided command with the appropriate formatting for the current provider's SQL dialect.
    /// PT: Adiciona um parâmetro ao comando fornecido com a formatação apropriada para o dialeto SQL do provedor atual.
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
    {
        var parameter = command.CreateParameter();
        parameter.ParameterName = Repo.Dialect.Parameter(name);
        parameter.DbType = dbType;
        parameter.Value = value ?? DBNull.Value;
        if (Repo.Dialect.Provider == ProviderId.Db2
            && value is string stringValue)
            parameter.Size = stringValue.Length;
        command.Parameters.Add(parameter);
    }
}
