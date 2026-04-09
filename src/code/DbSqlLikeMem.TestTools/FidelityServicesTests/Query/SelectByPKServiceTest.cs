namespace DbSqlLikeMem.TestTools.Query;

/// <summary>
/// EN: Executes the primary-key select command for the shared query scenario.
/// PT: Executa o comando de selecao por chave primaria para o cenario de consulta compartilhado.
/// </summary>
public class SelectByPKServiceTest<T>(
        T connection,
        ITestScenario<T> testScenario,
        ProviderSqlDialect dialect
    ) : BaseServiceTest<T>(connection, testScenario, dialect),
        IBaseServiceWithReturnTest<string>
    where T : DbConnection
{
    /// <summary>
    /// EN: Reads the seeded row by primary key and validates the returned value.
    /// PT: Lê a linha inserida pela chave primaria e valida o valor retornado.
    /// </summary>
    /// <param name="pars">EN: Scenario arguments that include the users table name. PT: Argumentos do cenario que incluem o nome da tabela de usuarios.</param>
    public string RunTest(params object[] pars)
    {
        var users = (string)pars[0];
        var tableName = ResolveScenarioTableName(users);
        string? value;

        var sql = Dialect.SelectUserNameById(tableName, 1);
        var rawValue = ExecuteScalar(sql);
        value = Convert.ToString(rawValue);

        if (!string.Equals(value, "Alice", StringComparison.Ordinal)
            && Dialect.Provider == ProviderId.Oracle)
        {
            using var command = Connection.CreateCommand();
            command.CommandText = NormalizeScenarioSql(sql);

            using var reader = command.ExecuteReader();
            if (reader.Read())
            {
                var nameValue = reader.GetValue(0);
                value = Convert.ToString(nameValue);
            }
        }

        if (!string.Equals(value, "Alice", StringComparison.Ordinal))
            throw new InvalidOperationException($"Unexpected select result for {Dialect.DisplayName}: {value ?? "<null>"}.");
        return value!;
    }
}
