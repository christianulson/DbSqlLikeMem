namespace DbSqlLikeMem.TestTools.DML;

/// <summary>
/// EN: Executes transaction and savepoint workflows over the shared test infrastructure.
/// PT-br: Executa fluxos de transacao e savepoint sobre a infraestrutura compartilhada de testes.
/// </summary>
public partial class DmlMutationServiceTest : BaseServiceTest
{
    /// <summary>
    /// EN: Creates a transaction service wrapper for the current fidelity test run.
    /// PT-br: Cria um wrapper de servico de transacao para a execucao atual do teste de fidelidade.
    /// </summary>
    /// <param name="repo">EN: Repository used to execute SQL commands. PT-br: Repositorio usado para executar comandos SQL.</param>
    /// <param name="context">EN: Scenario context with the current parameters. PT-br: Contexto do cenario com os parametros atuais.</param>
    public DmlMutationServiceTest(RepoService repo, FidelityTestContext context)
        : base(repo, context)
    {
    }

    /// <summary>
    /// EN: Gets the active database connection for the current run.
    /// PT-br: Obtem a conexao de banco de dados ativa da execucao atual.
    /// </summary>
    protected DbConnection Connection
    {
        get
        {
            if (Repo.Cnn.State != ConnectionState.Open)
            {
                Repo.Cnn.Open();
            }

            return Repo.Cnn;
        }
    }

    /// <summary>
    /// EN: Gets the active SQL dialect for the current run.
    /// PT-br: Obtem o dialeto SQL ativo da execucao atual.
    /// </summary>
    protected ProviderSqlDialect Dialect => Repo.Dialect;

    /// <summary>
    /// EN: Executes a non-query SQL command synchronously against the active connection.
    /// PT-br: Executa um comando SQL sem resultado de forma sincronizada na conexao ativa.
    /// </summary>
    /// <param name="sql">EN: SQL command text. PT-br: Texto do comando SQL.</param>
    /// <param name="transaction">EN: Optional transaction to enlist. PT-br: Transacao opcional para associar.</param>
    /// <returns>EN: Affected row count. PT-br: Quantidade de linhas afetadas.</returns>
    protected int ExecuteNonQuery(string sql, DbTransaction? transaction = null)
    {
        using var command = Connection.CreateCommand();
        command.CommandText = sql;
        command.Transaction = transaction;
        return command.ExecuteNonQuery();
    }

    /// <summary>
    /// EN: Executes a scalar SQL command synchronously against the active connection.
    /// PT-br: Executa um comando SQL escalar de forma sincronizada na conexao ativa.
    /// </summary>
    /// <param name="sql">EN: SQL command text. PT-br: Texto do comando SQL.</param>
    /// <param name="transaction">EN: Optional transaction to enlist. PT-br: Transacao opcional para associar.</param>
    /// <returns>EN: Scalar value returned by the command. PT-br: Valor escalar retornado pelo comando.</returns>
    protected object? ExecuteScalar(string sql, DbTransaction? transaction = null)
    {
        using var command = Connection.CreateCommand();
        command.CommandText = sql;
        command.Transaction = transaction;
        return command.ExecuteScalar();
    }

    /// <summary>
    /// EN: Inserts a row inside a transaction, commits it, and validates the persisted count.
    /// PT-br: Insere uma linha dentro de uma transação, confirma a operação e valida a contagem persistida.
    /// </summary>
    public int RunTransactionCommit(params object[] pars)
    {
        using var transaction = Connection.BeginTransaction();
        ExecuteNonQuery(Dialect.InsertUser(Context, 1, "Alice"), transaction);
        transaction.Commit();

        var count = Convert.ToInt32(ExecuteScalar(Dialect.CountRows(Context.TbUsersFullName)), CultureInfo.InvariantCulture);
        if (count != 1)
        {
            throw new InvalidOperationException($"Unexpected commit count for {Dialect.DisplayName}: {count}.");
        }

        return count;
    }

    /// <summary>
    /// EN: Inserts a row inside a transaction, rolls it back, and validates that no rows remain.
    /// PT-br: Insere uma linha dentro de uma transação, desfaz a operação e valida que nenhuma linha permaneceu.
    /// </summary>
    public int RunTransactionRollback(params object[] pars)
    {
        using var transaction = Connection.BeginTransaction();
        ExecuteNonQuery(Dialect.InsertUser(Context, 1, "Alice"), transaction);
        transaction.Rollback();

        var count = Convert.ToInt32(ExecuteScalar(Dialect.CountRows(Context.TbUsersFullName)), CultureInfo.InvariantCulture);
        if (count != 0)
        {
            throw new InvalidOperationException($"Unexpected rollback count for {Dialect.DisplayName}: {count}.");
        }

        return count;
    }

    /// <summary>
    /// EN: Creates a savepoint inside a transaction when the provider supports it.
    /// PT-br: Cria um savepoint dentro de uma transação quando o provedor suporta isso.
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
    /// PT-br: Faz rollback para um savepoint e valida a contagem de linhas restante.
    /// </summary>
    public int RunRollbackToSavepoint(params object[] pars)
    {
        if (!Dialect.SupportsSavepoints)
        {
            throw new NotSupportedException($"{Dialect.DisplayName} does not support the savepoint benchmark.");
        }

        using var transaction = Connection.BeginTransaction();
        ExecuteNonQuery(Dialect.InsertUser(Context, 1, "Alice"), transaction);
        var savepoint = $"sp_{Guid.NewGuid():N}"[..11];
        ExecuteSavepoint(transaction, savepoint);
        ExecuteNonQuery(Dialect.InsertUser(Context, 2, "Bob"), transaction);
        ExecuteRollbackToSavepoint(transaction, savepoint);
        transaction.Commit();

        var count = Convert.ToInt32(ExecuteScalar(Dialect.CountRows(Context.TbUsersFullName)), CultureInfo.InvariantCulture);
        if (count != 1)
        {
            throw new InvalidOperationException($"Unexpected rollback-to-savepoint count for {Dialect.DisplayName}: {count}.");
        }

        return count;
    }

    /// <summary>
    /// EN: Releases a savepoint inside a transaction when the provider supports it.
    /// PT-br: Libera um savepoint dentro de uma transação quando o provedor suporta isso.
    /// </summary>
    public void RunReleaseSavepoint()
    {
        if (!Dialect.SupportsReleaseSavepoints)
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
    /// PT-br: Executa um fluxo aninhado de savepoints e valida a contagem final de linhas.
    /// </summary>
    public int RunNestedSavepointFlow(params object[] pars)
    {
        if (!Dialect.SupportsSavepoints)
        {
            throw new NotSupportedException($"{Dialect.DisplayName} does not support the savepoint benchmark.");
        }

        using var transaction = Connection.BeginTransaction();
        ExecuteNonQuery(Dialect.InsertUser(Context, 1, "Alice"), transaction);
        var sp1 = $"sp_{Guid.NewGuid():N}"[..11];
        ExecuteSavepoint(transaction, sp1);
        ExecuteNonQuery(Dialect.InsertUser(Context, 2, "Bob"), transaction);
        var sp2 = $"sp_{Guid.NewGuid():N}"[..11];
        ExecuteSavepoint(transaction, sp2);
        ExecuteNonQuery(Dialect.InsertUser(Context, 3, "Charlie"), transaction);
        ExecuteRollbackToSavepoint(transaction, sp2);
        if (Dialect.SupportsReleaseSavepoints)
        {
            ExecuteDialectCommand(Dialect.ReleaseSavepoint(sp1), transaction);
        }
        transaction.Commit();

        var count = Convert.ToInt32(ExecuteScalar(Dialect.CountRows(Context.TbUsersFullName)), CultureInfo.InvariantCulture);
        if (count != 2)
        {
            throw new InvalidOperationException($"Unexpected nested-savepoint count for {Dialect.DisplayName}: {count}.");
        }

        return count;
    }

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
        if (UseNativeSavepointTransactionApi()
            && TryInvokeTransactionSavepointMethod(transaction, "Save", savepoint))
        {
            return;
        }

        ExecuteNonQuery(Dialect.Savepoint(savepoint), transaction);
    }

    private void ExecuteRollbackToSavepoint(DbTransaction transaction, string savepoint)
    {
        if (UseNativeSavepointTransactionApi()
            && TryInvokeTransactionSavepointMethod(transaction, "Rollback", savepoint))
        {
            return;
        }

        ExecuteNonQuery(Dialect.RollbackToSavepoint(savepoint), transaction);
    }

    private bool UseNativeSavepointTransactionApi()
        => Dialect.Provider is ProviderId.SqlServer
            or ProviderId.SqlAzure;

    private static bool TryInvokeTransactionSavepointMethod(
        DbTransaction transaction,
        string methodName,
        string savepoint)
    {
        var method = transaction.GetType().GetMethod(methodName, [typeof(string)]);
        if (method is null)
            return false;

        method.Invoke(transaction, [savepoint]);
        return true;
    }
}
