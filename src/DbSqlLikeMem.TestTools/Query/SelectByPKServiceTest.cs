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
    /// <param name="pars"></param>
    public string RunTest(params object[] pars)
    {
        var users = (string)pars[0];
        var uId = (string)pars[1];
        var tableName = Dialect.Provider == ProviderId.Oracle
            ? users.ToLowerInvariant()
            : $"{users}_{uId}";
        string? value;

        if (Dialect.Provider == ProviderId.Oracle)
        {
            var sql = Dialect.SelectUserNameById(tableName, 1);
            Console.WriteLine($"[SelectByPk][Oracle] SQL: {sql}");
            var rawValue = ExecuteScalar(sql);
            Console.WriteLine($"[SelectByPk][Oracle] Scalar raw value: {rawValue ?? "<null>"} ({rawValue?.GetType().FullName ?? "<null>"})");
            value = Convert.ToString(rawValue);

            if (!string.Equals(value, "Alice", StringComparison.Ordinal))
            {
                using var command = Connection.CreateCommand();
                command.CommandText = sql;

                using var reader = command.ExecuteReader();
                if (reader.Read())
                {
                    var nameValue = reader.GetValue(0);
                    Console.WriteLine($"[SelectByPk][Oracle] Reader fallback row: Name={nameValue ?? "<null>"}");
                    value = Convert.ToString(nameValue);
                }
                else
                {
                    Console.WriteLine($"[SelectByPk][Oracle] Reader fallback returned no rows for {sql}");
                }
            }
        }
        else
        {
            var sql = Dialect.SelectUserNameById(tableName, 1);
            value = Convert.ToString(ExecuteScalar(sql));
        }

        if (Dialect.Provider == ProviderId.Oracle)
        {
            Console.WriteLine($"[SelectByPk][Oracle] Final value: {value ?? "<null>"}");
        }

        if (!string.Equals(value, "Alice", StringComparison.Ordinal))
            throw new InvalidOperationException($"Unexpected select result for {Dialect.DisplayName}: {value ?? "<null>"}.");
        return value!;
    }
}
