namespace DbSqlLikeMem.TestTools.DML;

public partial class BatchServiceTest<T>
{
    /// <summary>
    /// EN: Executes a mixed read/write batch and validates the read result.
    /// PT: Executa um lote misto de leitura e escrita e valida o resultado lido.
    /// </summary>
    public string RunBatchMixedReadWrite(params object[] pars)
    {
        var users = (string)pars[0];
        using var transaction = Connection.BeginTransaction();
        ExecuteNonQuery(Dialect.InsertUser(users, 1, "Alice"), transaction);
        ExecuteNonQuery(Dialect.InsertUser(users, 2, "Bob"), transaction);
        var value = Convert.ToString(ExecuteScalar(Dialect.SelectUserNameById(users, 1), transaction), CultureInfo.InvariantCulture);
        ExecuteNonQuery(Dialect.UpdateUserNameById(users, 2, "Bob-v2"), transaction);
        transaction.Commit();

        return value!;
    }

    /// <summary>
    /// EN: Executes a scalar batch workflow and validates the count and second row value.
    /// PT: Executa um fluxo de lote escalar e valida a contagem e o valor da segunda linha.
    /// </summary>
    public string RunBatchScalar(params object[] pars)
    {
        var users = (string)pars[0];
        using var transaction = Connection.BeginTransaction();
        ExecuteNonQuery(Dialect.InsertUser(users, 1, "Alice"), transaction);
        ExecuteNonQuery(Dialect.InsertUser(users, 2, "Bob"), transaction);
        transaction.Commit();

        var count = Convert.ToInt32(ExecuteScalar(Dialect.CountRows(users)), CultureInfo.InvariantCulture);
        var second = Convert.ToString(ExecuteScalar(Dialect.SelectUserNameById(users, 2)), CultureInfo.InvariantCulture);

        return second!;
    }

    /// <summary>
    /// EN: Executes a non-query batch workflow and validates the final row count.
    /// PT: Executa um fluxo de lote sem resultado e valida a contagem final de linhas.
    /// </summary>
    public int RunBatchNonQuery(params object[] pars)
    {
        var users = (string)pars[0];
        using var transaction = Connection.BeginTransaction();
        ExecuteNonQuery(Dialect.InsertUser(users, 1, "Alice"), transaction);
        ExecuteNonQuery(Dialect.InsertUser(users, 2, "Bob"), transaction);
        ExecuteNonQuery(Dialect.UpdateUserNameById(users, 2, "Bob-v2"), transaction);
        ExecuteNonQuery(Dialect.DeleteUserById(users, 1), transaction);
        transaction.Commit();

        var count = Convert.ToInt32(ExecuteScalar(Dialect.CountRows(users)), CultureInfo.InvariantCulture);
        if (count != 1)
        {
            throw new InvalidOperationException($"Unexpected non-query batch count for {Dialect.DisplayName}: {count}.");
        }

        return count;
    }

    /// <summary>
    /// EN: Reads the provider row count and first value after a batch reader workflow.
    /// PT: Lê a contagem de linhas e o primeiro valor do provedor após um fluxo de leitura em lote.
    /// </summary>
    public object? RunBatchReaderMultiResult(params object[] pars)
    {
        var users = (string)pars[0];
        ExecuteNonQuery(Dialect.InsertUser(users, 1, "Alice"));
        ExecuteNonQuery(Dialect.InsertUser(users, 2, "Bob"));
        var first = Convert.ToInt32(ExecuteScalar(Dialect.CountRows(users)), CultureInfo.InvariantCulture);
        var second = ExecuteScalar(Dialect.SelectUserNameById(users, 1));

        GC.KeepAlive(first);
        GC.KeepAlive(second);
        return second;
    }
}
