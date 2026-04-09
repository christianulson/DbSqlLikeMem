namespace DbSqlLikeMem.TestTools.DML;

public partial class BatchServiceTest<T>
{
    /// <summary>
    /// EN: Executes the MariaDB RETURNING insert workflow and validates the returned row count.
    /// PT: Executa o fluxo INSERT RETURNING do MariaDB e valida a contagem de linhas retornadas.
    /// </summary>
    public int RunReturningInsert(params object[] pars)
    {
        var users = (string)pars[0];
        var rows = CountReaderRows(Dialect.InsertUserReturning(users, 1, "Alice"));
        if (rows != 1)
        {
            throw new InvalidOperationException($"Unexpected RETURNING rowcount for {Dialect.DisplayName}: {rows}.");
        }

        var count = Convert.ToInt32(ExecuteScalar(Dialect.CountRows(users)), CultureInfo.InvariantCulture);
        if (count != 1)
        {
            throw new InvalidOperationException($"Unexpected RETURNING insert persistence for {Dialect.DisplayName}: {count}.");
        }

        GC.KeepAlive(rows);
        GC.KeepAlive(count);
        return rows;
    }

    private int CountReaderRows(string sql, DbTransaction? transaction = null)
    {
        using var command = Connection.CreateCommand();
        command.CommandText = sql;
        if (transaction is not null)
        {
            command.Transaction = transaction;
        }

        using var reader = command.ExecuteReader();
        var count = 0;
        while (reader.Read())
        {
            count++;
        }

        return count;
    }
}
