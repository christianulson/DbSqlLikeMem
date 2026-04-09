namespace DbSqlLikeMem.TestTools.DML;

public partial class BatchServiceTest<T>
{
    /// <summary>
    /// EN: Inserts ten user rows in a batch and validates the final count.
    /// PT: Insere dez linhas de usuario em lote e valida a contagem final.
    /// </summary>
    public int RunBatchInsert10(params object[] pars)
    {
        var users = (string)pars[0];
        using var transaction = Connection.BeginTransaction();

        var values = new (int id, string name)[10];
        for (var i = 1; i <= 10; i++)
        {
            values[i - 1] = (i, $"User-{i}");
        }

        ExecuteNonQuery(Dialect.InsertUsers(users, values), transaction);
        transaction.Commit();

        var count = Convert.ToInt32(ExecuteScalar(Dialect.CountRows(users)), CultureInfo.InvariantCulture);
        if (count != 10)
        {
            throw new InvalidOperationException($"Expected 10 rows for {Dialect.DisplayName}, got {count}.");
        }

        return count;
    }

    /// <summary>
    /// EN: Inserts one hundred user rows in a batch and validates the final count.
    /// PT: Insere cem linhas de usuario em lote e valida a contagem final.
    /// </summary>
    public int RunBatchInsert100(params object[] pars)
    {
        var users = (string)pars[0];
        using var transaction = Connection.BeginTransaction();

        var values = new (int id, string name)[100];
        for (var i = 1; i <= 100; i++)
        {
            values[i - 1] = (i, $"User-{i}");
        }

        ExecuteNonQuery(Dialect.InsertUsers(users, values), transaction);
        transaction.Commit();

        var count = Convert.ToInt32(ExecuteScalar(Dialect.CountRows(users)), CultureInfo.InvariantCulture);
        if (count != 100)
        {
            throw new InvalidOperationException($"Expected 100 rows for {Dialect.DisplayName}, got {count}.");
        }

        return count;
    }

    /// <summary>
    /// EN: Reads the provider row count after a batch insert workflow.
    /// PT: Lê a contagem de linhas do provedor após um fluxo de insert em lote.
    /// </summary>
    public int RunRowCountInBatch(params object[] pars)
    {
        var users = (string)pars[0];
        ExecuteNonQuery(Dialect.InsertUser(users, 1, "Alice"));
        ExecuteNonQuery(Dialect.InsertUser(users, 2, "Bob"));

        var count = Convert.ToInt32(ExecuteScalar(Dialect.CountRows(users)), CultureInfo.InvariantCulture);
        if (count != 2)
        {
            throw new InvalidOperationException($"Unexpected batch rowcount for {Dialect.DisplayName}: {count}.");
        }

        return count;
    }
}
