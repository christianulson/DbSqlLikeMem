namespace DbSqlLikeMem.TestTools.DML;

public partial class BatchServiceTest<T>
{
    /// <summary>
    /// EN: Executes a batch inside a transaction and keeps the table name alive after commit.
    /// PT: Executa um lote dentro de uma transação e mantém o nome da tabela vivo após o commit.
    /// </summary>
    public string RunBatchTransactionControl(params object[] pars)
    {
        var users = (string)pars[0];
        using var tx = Connection.BeginTransaction();
        ExecuteNonQuery(Dialect.InsertUser(users, 1, "Alice"), tx);
        ExecuteNonQuery(Dialect.InsertUser(users, 2, "Bob"), tx);
        tx.Commit();
        GC.KeepAlive(users);
        return users;
    }
}
