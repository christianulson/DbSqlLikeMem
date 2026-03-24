namespace DbSqlLikeMem.TestTools.DML;

public partial class DmlMutationServiceTest<T>
{
    /// <summary>
    /// EN: Executes the provider-specific upsert path and validates the updated value.
    /// PT: Executa o caminho de upsert específico do provedor e valida o valor atualizado.
    /// </summary>
    public string RunUpsert(params object[] pars)
    {
        if (!Dialect.SupportsUpsert)
        {
            throw new NotSupportedException($"{Dialect.DisplayName} does not support the upsert benchmark.");
        }

        var users = (string)pars[0];
        ExecuteNonQuery(Dialect.Upsert(users, 1, "Alice-v2"));
        var value = Convert.ToString(ExecuteScalar(Dialect.SelectUserNameById(users, 1)), CultureInfo.InvariantCulture);
        if (!string.Equals(value, "Alice-v2", StringComparison.Ordinal))
        {
            throw new InvalidOperationException($"Unexpected upsert result for {Dialect.DisplayName}: {value ?? "<null>"}.");
        }

        return value!;
    }

    /// <summary>
    /// EN: Inserts a row with upsert and then updates the same row with a second upsert.
    /// PT: Insere uma linha com upsert e depois atualiza a mesma linha com um segundo upsert.
    /// </summary>
    public string RunUpsertInsertThenUpdate(params object[] pars)
    {
        if (!Dialect.SupportsUpsert)
        {
            throw new NotSupportedException($"{Dialect.DisplayName} does not support the upsert benchmark.");
        }

        var users = (string)pars[0];
        ExecuteNonQuery(Dialect.Upsert(users, 1, "Alice"));

        var inserted = Convert.ToString(ExecuteScalar(Dialect.SelectUserNameById(users, 1)), CultureInfo.InvariantCulture);
        if (!string.Equals(inserted, "Alice", StringComparison.Ordinal))
        {
            throw new InvalidOperationException($"Unexpected upsert insert result for {Dialect.DisplayName}: {inserted ?? "<null>"}.");
        }

        ExecuteNonQuery(Dialect.Upsert(users, 1, "Alice-v2"));

        var updated = Convert.ToString(ExecuteScalar(Dialect.SelectUserNameById(users, 1)), CultureInfo.InvariantCulture);
        if (!string.Equals(updated, "Alice-v2", StringComparison.Ordinal))
        {
            throw new InvalidOperationException($"Unexpected upsert update result for {Dialect.DisplayName}: {updated ?? "<null>"}.");
        }

        return updated!;
    }
}
