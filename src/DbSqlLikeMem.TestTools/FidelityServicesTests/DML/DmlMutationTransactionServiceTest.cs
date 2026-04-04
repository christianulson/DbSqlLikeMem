namespace DbSqlLikeMem.TestTools.DML;

public partial class DmlMutationServiceTest<T>
{
    /// <summary>
    /// EN: Inserts a row inside a transaction, commits it, and validates the persisted count.
    /// PT: Insere uma linha dentro de uma transação, confirma a operação e valida a contagem persistida.
    /// </summary>
    public int RunTransactionCommit(params object[] pars)
        => RunTransactionCommit((string)GetScenarioTableName(pars));

    /// <summary>
    /// EN: Inserts a row inside a transaction, commits it, and validates the persisted count.
    /// PT: Insere uma linha dentro de uma transação, confirma a operação e valida a contagem persistida.
    /// </summary>
    public int RunTransactionCommit(string tableName)
    {
        using var transaction = Connection.BeginTransaction();
        ExecuteNonQuery(Dialect.InsertUser(tableName, 1, "Alice"), transaction);
        transaction.Commit();

        var count = Convert.ToInt32(ExecuteScalar(Dialect.CountRows(tableName)), CultureInfo.InvariantCulture);
        if (count != 1)
        {
            throw new InvalidOperationException($"Unexpected commit count for {Dialect.DisplayName}: {count}.");
        }

        return count;
    }

    /// <summary>
    /// EN: Inserts a row inside a transaction, rolls it back, and validates that no rows remain.
    /// PT: Insere uma linha dentro de uma transação, desfaz a operação e valida que nenhuma linha permaneceu.
    /// </summary>
    public int RunTransactionRollback(params object[] pars)
        => RunTransactionRollback((string)GetScenarioTableName(pars));

    /// <summary>
    /// EN: Inserts a row inside a transaction, rolls it back, and validates that no rows remain.
    /// PT: Insere uma linha dentro de uma transação, desfaz a operação e valida que nenhuma linha permaneceu.
    /// </summary>
    public int RunTransactionRollback(string tableName)
    {
        using var transaction = Connection.BeginTransaction();
        ExecuteNonQuery(Dialect.InsertUser(tableName, 1, "Alice"), transaction);
        transaction.Rollback();

        var count = Convert.ToInt32(ExecuteScalar(Dialect.CountRows(tableName)), CultureInfo.InvariantCulture);
        if (count != 0)
        {
            throw new InvalidOperationException($"Unexpected rollback count for {Dialect.DisplayName}: {count}.");
        }

        return count;
    }

    /// <summary>
    /// EN: Creates a savepoint inside a transaction when the provider supports it.
    /// PT: Cria um savepoint dentro de uma transação quando o provedor suporta isso.
    /// </summary>
    public void RunSavepointCreate()
    {
        if (!Dialect.SupportsSavepoints)
        {
            throw new NotSupportedException($"{Dialect.DisplayName} does not support the savepoint benchmark.");
        }

        using var transaction = Connection.BeginTransaction();
        var savepoint = $"sp_{Guid.NewGuid():N}"[..11];
        ExecuteSavepoint(transaction, savepoint);
        transaction.Rollback();
    }

    /// <summary>
    /// EN: Rolls back to a savepoint and validates the remaining row count.
    /// PT: Faz rollback para um savepoint e valida a contagem de linhas restante.
    /// </summary>
    public int RunRollbackToSavepoint(params object[] pars)
        => RunRollbackToSavepoint((string)GetScenarioTableName(pars));

    /// <summary>
    /// EN: Rolls back to a savepoint and validates the remaining row count.
    /// PT: Faz rollback para um savepoint e valida a contagem de linhas restante.
    /// </summary>
    public int RunRollbackToSavepoint(string tableName)
    {
        if (!Dialect.SupportsSavepoints)
        {
            throw new NotSupportedException($"{Dialect.DisplayName} does not support the savepoint benchmark.");
        }

        using var transaction = Connection.BeginTransaction();
        ExecuteNonQuery(Dialect.InsertUser(tableName, 1, "Alice"), transaction);
        var savepoint = $"sp_{Guid.NewGuid():N}"[..11];
        ExecuteSavepoint(transaction, savepoint);
        ExecuteNonQuery(Dialect.InsertUser(tableName, 2, "Bob"), transaction);
        ExecuteRollbackToSavepoint(transaction, savepoint);
        transaction.Commit();

        var count = Convert.ToInt32(ExecuteScalar(Dialect.CountRows(tableName)), CultureInfo.InvariantCulture);
        if (count != 1)
        {
            throw new InvalidOperationException($"Unexpected rollback-to-savepoint count for {Dialect.DisplayName}: {count}.");
        }

        return count;
    }

    /// <summary>
    /// EN: Releases a savepoint inside a transaction when the provider supports it.
    /// PT: Libera um savepoint dentro de uma transação quando o provedor suporta isso.
    /// </summary>
    public void RunReleaseSavepoint()
    {
        if (!SupportsReleaseSavepointWorkflow())
        {
            throw new NotSupportedException($"{Dialect.DisplayName} does not support releasing savepoints.");
        }

        using var transaction = Connection.BeginTransaction();
        var savepoint = $"sp_{Guid.NewGuid():N}"[..11];
        ExecuteNonQuery(Dialect.Savepoint(savepoint), transaction);
        ExecuteDialectCommand(Dialect.ReleaseSavepoint(savepoint), transaction);
        transaction.Rollback();
    }

    /// <summary>
    /// EN: Executes a nested savepoint flow and validates the resulting row count.
    /// PT: Executa um fluxo aninhado de savepoints e valida a contagem final de linhas.
    /// </summary>
    public int RunNestedSavepointFlow(params object[] pars)
        => RunNestedSavepointFlow((string)GetScenarioTableName(pars));

    /// <summary>
    /// EN: Executes a nested savepoint flow and validates the resulting row count.
    /// PT: Executa um fluxo aninhado de savepoints e valida a contagem final de linhas.
    /// </summary>
    public int RunNestedSavepointFlow(string tableName)
    {
        if (!Dialect.SupportsSavepoints)
        {
            throw new NotSupportedException($"{Dialect.DisplayName} does not support the savepoint benchmark.");
        }

        using var transaction = Connection.BeginTransaction();
        ExecuteNonQuery(Dialect.InsertUser(tableName, 1, "Alice"), transaction);
        var sp1 = $"sp_{Guid.NewGuid():N}"[..11];
        ExecuteSavepoint(transaction, sp1);
        ExecuteNonQuery(Dialect.InsertUser(tableName, 2, "Bob"), transaction);
        var sp2 = $"sp_{Guid.NewGuid():N}"[..11];
        ExecuteSavepoint(transaction, sp2);
        ExecuteNonQuery(Dialect.InsertUser(tableName, 3, "Charlie"), transaction);
        ExecuteRollbackToSavepoint(transaction, sp2);
        if (SupportsReleaseSavepointWorkflow())
        {
            ExecuteDialectCommand(Dialect.ReleaseSavepoint(sp1), transaction);
        }
        transaction.Commit();

        var count = Convert.ToInt32(ExecuteScalar(Dialect.CountRows(tableName)), CultureInfo.InvariantCulture);
        if (count != 2)
        {
            throw new InvalidOperationException($"Unexpected nested-savepoint count for {Dialect.DisplayName}: {count}.");
        }

        return count;
    }

    private static string GetScenarioTableName(IReadOnlyList<object> pars)
        => pars.Count >= 3
            ? (string)pars[2]
            : $"{(string)pars[0]}_{(string)pars[1]}";

    private void ExecuteDialectCommand(string sql, DbTransaction? transaction = null)
    {
        var trimmed = sql.TrimStart();
        if (trimmed.StartsWith("SELECT", StringComparison.OrdinalIgnoreCase))
        {
            ExecuteScalar(sql, transaction);
            return;
        }

        ExecuteNonQuery(sql, transaction);
    }

    private void ExecuteSavepoint(DbTransaction transaction, string savepoint)
    {
        ExecuteNonQuery(Dialect.Savepoint(savepoint), transaction);
    }

    private void ExecuteRollbackToSavepoint(DbTransaction transaction, string savepoint)
    {
        ExecuteNonQuery(Dialect.RollbackToSavepoint(savepoint), transaction);
    }

    private bool SupportsReleaseSavepointWorkflow()
        => Dialect.Provider is not ProviderId.SqlServer
            and not ProviderId.SqlAzure
            and not ProviderId.Oracle;
}
