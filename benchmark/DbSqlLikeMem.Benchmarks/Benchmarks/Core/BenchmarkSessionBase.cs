using System.Globalization;

namespace DbSqlLikeMem.Benchmarks.Core;

/// <summary>
/// EN: Provides the common benchmark workflow shared by provider-specific benchmark sessions.
/// PT-br: Fornece o fluxo de benchmark comum compartilhado pelas sessões de benchmark específicas de cada provedor.
/// </summary>
/// <remarks>
/// EN: Derived types supply the connection factory and, when needed, override individual benchmark routines.
/// PT-br: Tipos derivados fornecem a fábrica de conexões e, quando necessário, sobrescrevem rotinas individuais de benchmark.
/// </remarks>
/// <param name="dialect">EN: The provider-specific SQL dialect used to generate benchmark commands. PT-br: O dialeto SQL específico do provedor usado para gerar os comandos de benchmark.</param>
/// <param name="engine">EN: The benchmark engine that identifies the runtime behind the session. PT-br: O mecanismo de benchmark que identifica o runtime por trás da sessão.</param>
public abstract class BenchmarkSessionBase(
    ProviderSqlDialect dialect, 
    BenchmarkEngine engine
    ) : IBenchmarkSession
{
    private static int _objectCounter;

    /// <summary>
    /// EN: Gets the SQL dialect abstraction used to generate provider-specific statements for the current session.
    /// PT-br: Obtém a abstração de dialeto SQL usada para gerar comandos específicos do provedor para a sessão atual.
    /// </summary>
    protected ProviderSqlDialect Dialect { get; } = dialect;

    /// <summary>
    /// EN: Gets the provider identifier exposed by the current dialect.
    /// PT-br: Obtém o identificador do provedor exposto pelo dialeto atual.
    /// </summary>
    public BenchmarkProviderId Provider => Dialect.Provider;

    /// <summary>
    /// EN: Gets the benchmark engine used by the current session.
    /// PT-br: Obtém o mecanismo de benchmark usado pela sessão atual.
    /// </summary>
    public BenchmarkEngine Engine { get; } = engine;

    /// <summary>
    /// EN: Performs any session initialization required before the benchmarks start.
    /// PT-br: Executa a inicialização necessária da sessão antes do início dos benchmarks.
    /// </summary>
    public virtual void Initialize()
    {
    }

    /// <summary>
    /// EN: Dispatches the requested benchmark feature to the corresponding benchmark routine.
    /// PT-br: Encaminha o recurso de benchmark solicitado para a rotina de benchmark correspondente.
    /// </summary>
    /// <param name="feature">EN: The benchmark feature to execute. PT-br: O recurso de benchmark a ser executado.</param>
    /// <exception cref="NotSupportedException"></exception>
    /// <exception cref="ArgumentOutOfRangeException"></exception>
    public void Execute(BenchmarkFeatureId feature)
    {
        switch (feature)
        {
            case BenchmarkFeatureId.ConnectionOpen:
                RunConnectionOpen();
                break;
            case BenchmarkFeatureId.CreateSchema:
                RunCreateSchema();
                break;
            case BenchmarkFeatureId.InsertSingle:
                RunInsertSingle();
                break;
            case BenchmarkFeatureId.InsertBatch100:
                RunInsertBatch100();
                break;
            case BenchmarkFeatureId.SelectByPk:
                RunSelectByPk();
                break;
            case BenchmarkFeatureId.SelectJoin:
                RunSelectJoin();
                break;
            case BenchmarkFeatureId.UpdateByPk:
                RunUpdateByPk();
                break;
            case BenchmarkFeatureId.DeleteByPk:
                RunDeleteByPk();
                break;
            case BenchmarkFeatureId.TransactionCommit:
                RunTransactionCommit();
                break;
            case BenchmarkFeatureId.TransactionRollback:
                RunTransactionRollback();
                break;
            case BenchmarkFeatureId.Upsert:
                RunUpsert();
                break;
            case BenchmarkFeatureId.SequenceNextValue:
                RunSequenceNextValue();
                break;
            case BenchmarkFeatureId.StringAggregate:
                RunStringAggregate();
                break;
            case BenchmarkFeatureId.DateScalar:
                RunDateScalar();
                break;
            case BenchmarkFeatureId.ExecutionPlan:
                throw new NotSupportedException("ExecutionPlan ficou só no catálogo desta primeira malha benchmarkável.");
            default:
                throw new ArgumentOutOfRangeException(nameof(feature), feature, null);
        }
    }

    /// <summary>
    /// EN: Releases any resources allocated by the benchmark session.
    /// PT-br: Libera os recursos alocados pela sessão de benchmark.
    /// </summary>
    public virtual void Dispose()
    {
    }

    /// <summary>
    /// EN: Creates a new provider-specific connection instance for the current benchmark session.
    /// PT-br: Cria uma nova instância de conexão específica do provedor para a sessão de benchmark atual.
    /// </summary>
    /// <returns>EN: A new provider-specific connection instance. PT-br: Uma nova instância de conexão específica do provedor.</returns>
    protected abstract DbConnection CreateConnection();

    /// <summary>
    /// EN: Generates a unique temporary table name for the users table used by a benchmark run.
    /// PT-br: Gera um nome único de tabela temporária para a tabela de usuários usada em uma execução de benchmark.
    /// </summary>
    /// <returns>EN: A unique temporary users table name. PT-br: Um nome único de tabela temporária de usuários.</returns>
    protected virtual string NewUsersTableName() => $"USR_{NextToken()}";

    /// <summary>
    /// EN: Generates a unique temporary table name for the orders table used by a benchmark run.
    /// PT-br: Gera um nome único de tabela temporária para a tabela de pedidos usada em uma execução de benchmark.
    /// </summary>
    /// <returns>EN: A unique temporary orders table name. PT-br: Um nome único de tabela temporária de pedidos.</returns>
    protected virtual string NewOrdersTableName() => $"ORD_{NextToken()}";

    /// <summary>
    /// EN: Generates a unique temporary sequence name for sequence-based benchmark operations.
    /// PT-br: Gera um nome único de sequência temporária para operações de benchmark baseadas em sequência.
    /// </summary>
    /// <returns>EN: A unique temporary sequence name. PT-br: Um nome único de sequência temporária.</returns>
    protected virtual string NewSequenceName() => $"SEQ_{NextToken()}";

    /// <summary>
    /// EN: Generates a unique hexadecimal token that can be appended to temporary object names.
    /// PT-br: Gera um token hexadecimal único que pode ser anexado aos nomes de objetos temporários.
    /// </summary>
    /// <returns>EN: A unique hexadecimal token for temporary object naming. PT-br: Um token hexadecimal único para nomeação de objetos temporários.</returns>
    protected static string NextToken() => Interlocked.Increment(ref _objectCounter).ToString("x8", CultureInfo.InvariantCulture).ToUpperInvariant();

    /// <summary>
    /// EN: Measures the cost of opening a new database connection.
    /// PT-br: Mede o custo de abrir uma nova conexão de banco de dados.
    /// </summary>
    protected virtual void RunConnectionOpen()
    {
        using var connection = CreateConnection();
        connection.Open();
        GC.KeepAlive(connection.State);
    }

    /// <summary>
    /// EN: Creates the benchmark tables and then removes them as part of the schema creation measurement.
    /// PT-br: Cria as tabelas de benchmark e depois as remove como parte da medição de criação de esquema.
    /// </summary>
    protected virtual void RunCreateSchema()
    {
        var users = NewUsersTableName();
        var orders = NewOrdersTableName();

        using var connection = CreateConnection();
        connection.Open();

        try
        {
            ExecuteNonQuery(connection, Dialect.CreateUsersTable(users));
            ExecuteNonQuery(connection, Dialect.CreateOrdersTable(orders));
        }
        finally
        {
            SafeDropTable(connection, orders);
            SafeDropTable(connection, users);
        }
    }

    /// <summary>
    /// EN: Inserts a single user row and validates that the row was persisted.
    /// PT-br: Insere uma única linha de usuário e valida que a linha foi persistida.
    /// </summary>
    /// <exception cref="InvalidOperationException"></exception>
    protected virtual void RunInsertSingle()
    {
        var users = NewUsersTableName();
        using var connection = CreateConnection();
        connection.Open();

        try
        {
            ExecuteNonQuery(connection, Dialect.CreateUsersTable(users));
            ExecuteNonQuery(connection, Dialect.InsertUser(users, 1, "Alice"));
            var count = Convert.ToInt32(ExecuteScalar(connection, Dialect.CountRows(users)), CultureInfo.InvariantCulture);
            if (count != 1)
            {
                throw new InvalidOperationException($"Expected 1 row for {Dialect.DisplayName}, got {count}.");
            }
            GC.KeepAlive(count);
        }
        finally
        {
            SafeDropTable(connection, users);
        }
    }

    /// <summary>
    /// EN: Inserts one hundred user rows and validates the final row count.
    /// PT-br: Insere cem linhas de usuário e valida a contagem final de linhas.
    /// </summary>
    /// <exception cref="InvalidOperationException"></exception>
    protected virtual void RunInsertBatch100()
    {
        var users = NewUsersTableName();
        using var connection = CreateConnection();
        connection.Open();

        try
        {
            ExecuteNonQuery(connection, Dialect.CreateUsersTable(users));
            for (var i = 1; i <= 100; i++)
            {
                ExecuteNonQuery(connection, Dialect.InsertUser(users, i, $"User-{i}"));
            }

            var count = Convert.ToInt32(ExecuteScalar(connection, Dialect.CountRows(users)), CultureInfo.InvariantCulture);
            if (count != 100)
            {
                throw new InvalidOperationException($"Expected 100 rows for {Dialect.DisplayName}, got {count}.");
            }
            GC.KeepAlive(count);
        }
        finally
        {
            SafeDropTable(connection, users);
        }
    }

    /// <summary>
    /// EN: Reads a user name by primary key and validates the returned value.
    /// PT-br: Lê um nome de usuário pela chave primária e valida o valor retornado.
    /// </summary>
    /// <exception cref="InvalidOperationException"></exception>
    protected virtual void RunSelectByPk()
    {
        var users = NewUsersTableName();
        using var connection = CreateConnection();
        connection.Open();

        try
        {
            ExecuteNonQuery(connection, Dialect.CreateUsersTable(users));
            ExecuteNonQuery(connection, Dialect.InsertUser(users, 1, "Alice"));
            var value = Convert.ToString(ExecuteScalar(connection, Dialect.SelectUserNameById(users, 1)), CultureInfo.InvariantCulture);
            if (!string.Equals(value, "Alice", StringComparison.Ordinal))
            {
                throw new InvalidOperationException($"Unexpected select result for {Dialect.DisplayName}: {value ?? "<null>"}.");
            }
            GC.KeepAlive(value);
        }
        finally
        {
            SafeDropTable(connection, users);
        }
    }

    /// <summary>
    /// EN: Executes a join query between users and orders and validates the resulting count.
    /// PT-br: Executa uma consulta com junção entre usuários e pedidos e valida a contagem resultante.
    /// </summary>
    /// <exception cref="InvalidOperationException"></exception>
    protected virtual void RunSelectJoin()
    {
        var users = NewUsersTableName();
        var orders = NewOrdersTableName();
        using var connection = CreateConnection();
        connection.Open();

        try
        {
            ExecuteNonQuery(connection, Dialect.CreateUsersTable(users));
            ExecuteNonQuery(connection, Dialect.CreateOrdersTable(orders));
            ExecuteNonQuery(connection, Dialect.InsertUser(users, 1, "Alice"));
            ExecuteNonQuery(connection, Dialect.InsertOrder(orders, 10, 1, "A"));
            ExecuteNonQuery(connection, Dialect.InsertOrder(orders, 11, 1, "B"));
            var value = Convert.ToInt32(ExecuteScalar(connection, Dialect.CountJoinForUser(users, orders, 1)), CultureInfo.InvariantCulture);
            if (value != 2)
            {
                throw new InvalidOperationException($"Unexpected join count for {Dialect.DisplayName}: {value}.");
            }
            GC.KeepAlive(value);
        }
        finally
        {
            SafeDropTable(connection, orders);
            SafeDropTable(connection, users);
        }
    }

    /// <summary>
    /// EN: Updates a user row by primary key and validates the stored value.
    /// PT-br: Atualiza uma linha de usuário pela chave primária e valida o valor armazenado.
    /// </summary>
    /// <exception cref="InvalidOperationException"></exception>
    protected virtual void RunUpdateByPk()
    {
        var users = NewUsersTableName();
        using var connection = CreateConnection();
        connection.Open();

        try
        {
            ExecuteNonQuery(connection, Dialect.CreateUsersTable(users));
            ExecuteNonQuery(connection, Dialect.InsertUser(users, 1, "Alice"));
            ExecuteNonQuery(connection, Dialect.UpdateUserNameById(users, 1, "Alice-v2"));
            var value = Convert.ToString(ExecuteScalar(connection, Dialect.SelectUserNameById(users, 1)), CultureInfo.InvariantCulture);
            if (!string.Equals(value, "Alice-v2", StringComparison.Ordinal))
            {
                throw new InvalidOperationException($"Unexpected update result for {Dialect.DisplayName}: {value ?? "<null>"}.");
            }
            GC.KeepAlive(value);
        }
        finally
        {
            SafeDropTable(connection, users);
        }
    }

    /// <summary>
    /// EN: Deletes a user row by primary key and validates the remaining row count.
    /// PT-br: Exclui uma linha de usuário pela chave primária e valida a contagem de linhas restante.
    /// </summary>
    /// <exception cref="InvalidOperationException"></exception>
    protected virtual void RunDeleteByPk()
    {
        var users = NewUsersTableName();
        using var connection = CreateConnection();
        connection.Open();

        try
        {
            ExecuteNonQuery(connection, Dialect.CreateUsersTable(users));
            ExecuteNonQuery(connection, Dialect.InsertUser(users, 1, "Alice"));
            ExecuteNonQuery(connection, Dialect.InsertUser(users, 2, "Bob"));
            ExecuteNonQuery(connection, Dialect.DeleteUserById(users, 1));
            var count = Convert.ToInt32(ExecuteScalar(connection, Dialect.CountRows(users)), CultureInfo.InvariantCulture);
            if (count != 1)
            {
                throw new InvalidOperationException($"Unexpected delete count for {Dialect.DisplayName}: {count}.");
            }
            GC.KeepAlive(count);
        }
        finally
        {
            SafeDropTable(connection, users);
        }
    }

    /// <summary>
    /// EN: Executes an insert inside a transaction, commits it, and validates the committed result.
    /// PT-br: Executa uma inserção dentro de uma transação, confirma a operação e valida o resultado confirmado.
    /// </summary>
    /// <exception cref="InvalidOperationException"></exception>
    protected virtual void RunTransactionCommit()
    {
        var users = NewUsersTableName();
        using var connection = CreateConnection();
        connection.Open();

        try
        {
            ExecuteNonQuery(connection, Dialect.CreateUsersTable(users));
            using var transaction = connection.BeginTransaction();
            ExecuteNonQuery(connection, Dialect.InsertUser(users, 1, "Alice"), transaction);
            transaction.Commit();
            var count = Convert.ToInt32(ExecuteScalar(connection, Dialect.CountRows(users)), CultureInfo.InvariantCulture);
            if (count != 1)
            {
                throw new InvalidOperationException($"Unexpected commit count for {Dialect.DisplayName}: {count}.");
            }
            GC.KeepAlive(count);
        }
        finally
        {
            SafeDropTable(connection, users);
        }
    }

    /// <summary>
    /// EN: Executes an insert inside a transaction, rolls it back, and validates that no rows remain.
    /// PT-br: Executa uma inserção dentro de uma transação, desfaz a operação e valida que nenhuma linha permaneceu.
    /// </summary>
    /// <exception cref="InvalidOperationException"></exception>
    protected virtual void RunTransactionRollback()
    {
        var users = NewUsersTableName();
        using var connection = CreateConnection();
        connection.Open();

        try
        {
            ExecuteNonQuery(connection, Dialect.CreateUsersTable(users));
            using var transaction = connection.BeginTransaction();
            ExecuteNonQuery(connection, Dialect.InsertUser(users, 1, "Alice"), transaction);
            transaction.Rollback();
            var count = Convert.ToInt32(ExecuteScalar(connection, Dialect.CountRows(users)), CultureInfo.InvariantCulture);
            if (count != 0)
            {
                throw new InvalidOperationException($"Unexpected rollback count for {Dialect.DisplayName}: {count}.");
            }
            GC.KeepAlive(count);
        }
        finally
        {
            SafeDropTable(connection, users);
        }
    }

    /// <summary>
    /// EN: Executes the provider-specific upsert path and validates the updated value.
    /// PT-br: Executa o caminho de upsert específico do provedor e valida o valor atualizado.
    /// </summary>
    /// <exception cref="NotSupportedException"></exception>
    /// <exception cref="InvalidOperationException"></exception>
    protected virtual void RunUpsert()
    {
        if (!Dialect.SupportsUpsert)
        {
            throw new NotSupportedException($"{Dialect.DisplayName} does not support the upsert benchmark.");
        }

        var users = NewUsersTableName();
        using var connection = CreateConnection();
        connection.Open();

        try
        {
            ExecuteNonQuery(connection, Dialect.CreateUsersTable(users));
            ExecuteNonQuery(connection, Dialect.InsertUser(users, 1, "Alice"));
            ExecuteNonQuery(connection, Dialect.Upsert(users, 1, "Alice-v2"));
            var value = Convert.ToString(ExecuteScalar(connection, Dialect.SelectUserNameById(users, 1)), CultureInfo.InvariantCulture);
            if (!string.Equals(value, "Alice-v2", StringComparison.Ordinal))
            {
                throw new InvalidOperationException($"Unexpected upsert result for {Dialect.DisplayName}: {value ?? "<null>"}.");
            }
            GC.KeepAlive(value);
        }
        finally
        {
            SafeDropTable(connection, users);
        }
    }

    /// <summary>
    /// EN: Creates a temporary sequence and reads its next value.
    /// PT-br: Cria uma sequência temporária e lê o seu próximo valor.
    /// </summary>
    /// <exception cref="NotSupportedException"></exception>
    protected virtual void RunSequenceNextValue()
    {
        if (!Dialect.SupportsSequence)
        {
            throw new NotSupportedException($"{Dialect.DisplayName} does not support the sequence benchmark.");
        }

        var sequence = NewSequenceName();
        using var connection = CreateConnection();
        connection.Open();

        try
        {
            ExecuteNonQuery(connection, Dialect.CreateSequence(sequence));
            var value = ExecuteScalar(connection, Dialect.NextSequenceValue(sequence));
            GC.KeepAlive(value);
        }
        finally
        {
            SafeDropSequence(connection, sequence);
        }
    }

    /// <summary>
    /// EN: Executes the provider-specific string aggregation query over sample user names.
    /// PT-br: Executa a consulta de agregação de strings específica do provedor sobre nomes de usuários de exemplo.
    /// </summary>
    /// <exception cref="NotSupportedException"></exception>
    protected virtual void RunStringAggregate()
    {
        if (!Dialect.SupportsStringAggregate)
        {
            throw new NotSupportedException($"{Dialect.DisplayName} does not support the string aggregate benchmark.");
        }

        var users = NewUsersTableName();
        using var connection = CreateConnection();
        connection.Open();

        try
        {
            ExecuteNonQuery(connection, Dialect.CreateUsersTable(users));
            ExecuteNonQuery(connection, Dialect.InsertUser(users, 1, "Charlie"));
            ExecuteNonQuery(connection, Dialect.InsertUser(users, 2, "Alice"));
            ExecuteNonQuery(connection, Dialect.InsertUser(users, 3, "Bob"));
            var value = Convert.ToString(ExecuteScalar(connection, Dialect.StringAggregate(users)), CultureInfo.InvariantCulture);
            GC.KeepAlive(value);
        }
        finally
        {
            SafeDropTable(connection, users);
        }
    }

    /// <summary>
    /// EN: Executes the provider-specific scalar date/time query.
    /// PT-br: Executa a consulta escalar de data/hora específica do provedor.
    /// </summary>
    protected virtual void RunDateScalar()
    {
        using var connection = CreateConnection();
        connection.Open();
        var value = ExecuteScalar(connection, Dialect.DateScalar());
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes a SQL command that does not return a result set.
    /// PT-br: Executa um comando SQL que não retorna um conjunto de resultados.
    /// </summary>
    /// <param name="connection">EN: The database connection used to execute the operation. PT-br: A conexão de banco de dados usada para executar a operação.</param>
    /// <param name="sql">EN: The SQL command text to execute. PT-br: O texto do comando SQL a ser executado.</param>
    /// <param name="transaction">EN: The optional transaction associated with the command execution. PT-br: A transação opcional associada à execução do comando.</param>
    protected static void ExecuteNonQuery(DbConnection connection, string sql, DbTransaction? transaction = null)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        if (transaction is not null)
        {
            command.Transaction = transaction;
        }
        command.ExecuteNonQuery();
    }

    /// <summary>
    /// EN: Executes a SQL command and returns its scalar result.
    /// PT-br: Executa um comando SQL e retorna o seu resultado escalar.
    /// </summary>
    /// <param name="connection">EN: The database connection used to execute the operation. PT-br: A conexão de banco de dados usada para executar a operação.</param>
    /// <param name="sql">EN: The SQL command text to execute. PT-br: O texto do comando SQL a ser executado.</param>
    /// <param name="transaction">EN: The optional transaction associated with the command execution. PT-br: A transação opcional associada à execução do comando.</param>
    /// <returns>EN: The scalar value returned by the SQL command. PT-br: O valor escalar retornado pelo comando SQL.</returns>
    protected static object? ExecuteScalar(DbConnection connection, string sql, DbTransaction? transaction = null)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        if (transaction is not null)
        {
            command.Transaction = transaction;
        }
        return command.ExecuteScalar();
    }

    /// <summary>
    /// EN: Tries to drop a table using best-effort cleanup semantics.
    /// PT-br: Tenta remover uma tabela usando uma limpeza de melhor esforço.
    /// </summary>
    /// <param name="connection">EN: The database connection used to execute the operation. PT-br: A conexão de banco de dados usada para executar a operação.</param>
    /// <param name="tableName">EN: The table name targeted by the operation. PT-br: O nome da tabela alvo da operação.</param>
    protected void SafeDropTable(DbConnection connection, string tableName)
    {
        SafeExecute(connection, Dialect.DropTable(tableName));
    }

    /// <summary>
    /// EN: Tries to drop a sequence using best-effort cleanup semantics.
    /// PT-br: Tenta remover uma sequência usando uma limpeza de melhor esforço.
    /// </summary>
    /// <param name="connection">EN: The database connection used to execute the operation. PT-br: A conexão de banco de dados usada para executar a operação.</param>
    /// <param name="sequenceName">EN: The sequence name targeted by the operation. PT-br: O nome da sequência alvo da operação.</param>
    protected void SafeDropSequence(DbConnection connection, string sequenceName)
    {
        SafeExecute(connection, Dialect.DropSequence(sequenceName));
    }

    /// <summary>
    /// EN: Executes a cleanup command while suppressing cleanup failures.
    /// PT-br: Executa um comando de limpeza suprimindo falhas durante a limpeza.
    /// </summary>
    /// <param name="connection">EN: The database connection used to execute the operation. PT-br: A conexão de banco de dados usada para executar a operação.</param>
    /// <param name="sql">EN: The SQL command text to execute. PT-br: O texto do comando SQL a ser executado.</param>
    protected static void SafeExecute(DbConnection connection, string sql)
    {
        try
        {
            using var command = connection.CreateCommand();
            command.CommandText = sql;
            command.ExecuteNonQuery();
        }
        catch
        {
            // cleanup is best-effort only
        }
    }
}
