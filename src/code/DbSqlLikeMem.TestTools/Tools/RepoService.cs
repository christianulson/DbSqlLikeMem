namespace DbSqlLikeMem.TestTools;

/// <summary>
/// EN: Provides helper methods for executing SQL commands and queries against a database connection within the context of scenario-based tests.
/// PT: Fornece métodos auxiliares para executar comandos e consultas SQL contra uma conexão de banco de dados no contexto de testes baseados em cenários.
/// </summary>
/// <param name="cnnFactory"></param>
/// <param name="dialect"></param>
public class RepoService(
    Func<DbConnection> cnnFactory,
    ProviderSqlDialect dialect
    ) : IDisposable
{
    /// <summary>
    /// EN: Gets the SQL dialect used by the provider.
    /// PT: Obtém o dialeto SQL usado pelo provedor.
    /// </summary>
    public ProviderSqlDialect Dialect => dialect;

    /// <summary>
    /// EN: Gets the database connection used for executing SQL commands and queries. The connection is created using the provided factory function and is managed by the RepoService instance, which ensures that it is properly disposed of when no longer needed.
    /// PT: Obtém a conexão de banco de dados usada para executar comandos e consultas SQL. A conexão é criada usando a função de fábrica fornecida e é gerenciada pela instância do RepoService, que garante que ela seja adequadamente descartada quando não for mais necessária.
    /// </summary>
    public DbConnection Cnn { get; } = cnnFactory();

    /// <summary>
    /// EN: Begins a new database transaction on the current connection. The transaction can be used to group multiple SQL commands into a single unit of work, allowing for atomic operations and rollback capabilities in case of errors. The caller is responsible for committing or rolling back the transaction as needed.
    /// PT: Inicia uma nova transação de banco de dados na conexão atual. A transação pode ser usada para agrupar vários comandos SQL em uma única unidade de trabalho, permitindo operações atômicas e capacidades de rollback em caso de erros. O chamador é responsável por confirmar ou reverter a transação conforme necessário.
    /// </summary>
    /// <returns>EN: The database transaction object. PT: O objeto de transação do banco de dados.</returns>
    public DbTransaction BeginTransaction()
        => Cnn.BeginTransaction();

    /// <summary>
    /// EN: Executes a SQL command that does not return a result set.
    /// PT: Executa um comando SQL que nao retorna um conjunto de resultados.
    /// </summary>
    /// <param name="sql">EN: The SQL command text to execute. PT: O texto do comando SQL a ser executado.</param>
    /// <param name="transaction">EN: The optional transaction associated with the command execution. PT: A transacao opcional associada a execucao do comando.</param>
    /// <param name="addParameters">EN: An optional action to add parameters to the command. PT: Uma ação opcional para adicionar parâmetros ao comando.</param>
    internal async Task<int> ExecuteNonQueryAsync(
        string sql,
        DbTransaction? transaction = null,
        Action<DbCommand>? addParameters = null)
    {
        await EnsureConnectionOpenAsync();
        using var command = Cnn.CreateCommand();
        command.CommandText = sql;
        command.Transaction = transaction;
        addParameters?.Invoke(command);
        return await command.ExecuteNonQueryAsync();
    }

    internal async Task<int> ExecuteNonQueryStatementsAsync(
        string sql,
        DbTransaction? transaction = null)
    {
        await EnsureConnectionOpenAsync();

        var affectedRows = 0;
        foreach (var statement in SplitStatements(sql))
        {
            using var command = Cnn.CreateCommand();
            command.CommandText = statement;
            command.Transaction = transaction;
            affectedRows += await command.ExecuteNonQueryAsync();
        }

        return affectedRows;
    }

    /// <summary>
    /// EN: Executes a SQL command and returns its scalar result.
    /// PT: Executa um comando SQL e retorna o seu resultado escalar.
    /// </summary>
    /// <param name="sql">EN: The SQL command text to execute. PT: O texto do comando SQL a ser executado.</param>
    /// <param name="transaction">EN: The optional transaction associated with the command execution. PT: A transacao opcional associada a execucao do comando.</param>
    /// <param name="addParameters">EN: An optional action to add parameters to the command. PT: Uma ação opcional para adicionar parâmetros ao comando.</param>
    /// <returns>EN: The scalar value returned by the SQL command. PT: O valor escalar retornado pelo comando SQL.</returns>
    internal async Task<object?> ExecuteScalarAsync(
        string sql,
        DbTransaction? transaction = null,
        Action<DbCommand>? addParameters = null)
    {
        await EnsureConnectionOpenAsync();
        using var command = Cnn.CreateCommand();
        command.CommandText = sql;
        command.Transaction = transaction;
        addParameters?.Invoke(command);
        var value = await command.ExecuteScalarAsync();
        if (value is not DBNull || dialect.Provider != ProviderId.Firebird)
            return value;

        using var reader = await command.ExecuteReaderAsync();
        if (!await reader.ReadAsync())
            return value;

        return await reader.IsDBNullAsync(0)
            ? DBNull.Value
            : reader.GetValue(0);
    }

    /// <summary>
    /// Executes the specified SQL query asynchronously and returns the results as a list of result sets.
    /// </summary>
    /// <remarks>Each inner list in the returned value corresponds to a result set produced by the query, such
    /// as when executing statements that return multiple result sets. The method does not return empty result
    /// sets.</remarks>
    /// <param name="sql">The SQL query to execute. Must be a valid SQL statement supported by the underlying database.</param>
    /// <param name="transaction">An optional database transaction within which the command executes. If null, the command executes outside of a
    /// transaction.</param>
    /// <param name="addParameters">An optional delegate to configure parameters for the database command before execution. Can be used to add or
    /// modify command parameters.</param>
    /// <returns>A list of result sets, where each result set is represented as a list of object arrays. Each object array
    /// contains the values of a single row. The list is empty if the query returns no results.</returns>
    internal async Task<List<List<object?[]>>> ExecuteReaderAsync(
        string sql,
        DbTransaction? transaction = null,
        Action<DbCommand>? addParameters = null)
    {
        await EnsureConnectionOpenAsync();
        using var command = Cnn.CreateCommand();
        command.CommandText = sql;
        command.Transaction = transaction;
        addParameters?.Invoke(command);

        var lst = new List<List<object?[]>>();
        using var reader = await command.ExecuteReaderAsync();

        var resultSet = await ReadResultSetAsync(reader);
        if (resultSet.Count > 0) lst.Add(resultSet);
        while(await reader.NextResultAsync())
        {
            resultSet = await ReadResultSetAsync(reader);
            if (resultSet.Count > 0) lst.Add(resultSet);
        }

        return lst;
    }

    private static async Task<List<object?[]>> ReadResultSetAsync(DbDataReader reader)
    {
        var resultSet = new List<object?[]>();
        while (await reader.ReadAsync())
        {
            var values = new object[reader.FieldCount];
            var v = reader.GetValues(values);
            if (reader.FieldCount != v) throw new Exception($"Expected {reader.FieldCount} values, but got {v}.");
            resultSet.Add(values);
        }
        return resultSet;
    }

    internal async Task EnsureConnectionOpenAsync()
    {
        if (Cnn.State == ConnectionState.Open) return;
        await Cnn.OpenAsync();
    }

    private static IEnumerable<string> SplitStatements(string sql)
    {
        return StringCompatibility.SplitAndTrim(sql, ';');
    }

    /// <summary>
    /// EN: Creates a new instance of the RepoService class with the same configuration as the current instance.
    /// PT: Cria uma nova instância da classe RepoService com a mesma configuração da instância atual.
    /// </summary>
    /// <remarks>Use this method to obtain a separate RepoService instance with identical settings, which can
    /// be useful for scenarios requiring multiple service instances with the same configuration.</remarks>
    /// <returns>A new RepoService instance that shares the same connection factory and SQL dialect as the original.</returns>
    public RepoService Clone()
        => new(cnnFactory, dialect);

    /// <summary>
    /// EN: Creates a new instance of the RepoService class that shares
    /// PT: Cria uma nova instância da classe RepoService que compartilhaq
    /// </summary>
    /// <returns></returns>
    public RepoService CloneWithSharedDatabase()
    {
        if (Cnn is DbConnectionMockBase mockCnn)
        {
            var sharedDb = mockCnn.Db;
            var connectionType = mockCnn.GetType();
            var ctor = connectionType
                .GetConstructors()
                .FirstOrDefault(c =>
                {
                    var parameters = c.GetParameters();
                    return parameters.Length is 1 or 2
                        && parameters[0].ParameterType.IsInstanceOfType(sharedDb)
                        && (parameters.Length == 1 || parameters[1].ParameterType == typeof(string));
                });

            if (ctor is not null)
            {
                return new RepoService(
                    () =>
                    {
                        var parameters = ctor.GetParameters();
                        object?[] args = parameters.Length == 1
                            ? new object?[] { sharedDb }
                            : new object?[] { sharedDb, null };
                        return (DbConnection)ctor.Invoke(args);
                    },
                    dialect);
            }

            return new RepoService(
                () => (DbConnection)Activator.CreateInstance(connectionType, sharedDb)!,
                dialect);
        }

        return Clone();
    }

    #region Dispose

    private bool disposedValue;

    /// <summary>
    /// EN: Disposes the resources used by the RepoService instance, including the underlying database connection. This method is called by the Dispose method and the finalizer to ensure that all resources are properly released. When disposing is true, it disposes of managed resources; when false, it only releases unmanaged resources.
    /// PT: Dispõe os recursos usados pela instância do RepoService, incluindo a conexão de banco de dados subjacente. Este método é chamado pelo método Dispose e pelo finalizador para garantir que todos os recursos sejam adequadamente liberados. Quando disposing é true, ele descarta os recursos gerenciados; quando false, ele apenas libera os recursos não gerenciados.
    /// </summary>
    /// <param name="disposing"></param>
    protected virtual void Dispose(bool disposing)
    {
        if (!disposedValue)
        {
            if (disposing)
            {
                Cnn.Dispose();
            }

            disposedValue = true;
        }
    }

    /// <summary>
    /// EN: Finalizes the RepoService instance, ensuring that all resources are properly released. This method is called by the garbage collector when the object is no longer in use. It calls the Dispose method with disposing set to false to release unmanaged resources without attempting to dispose of managed resources, which may have already been collected.
    /// PT: Finaliza a instância do RepoService, garantindo que todos os recursos sejam adequadamente liberados. Este método é chamado pelo coletor de lixo quando o objeto não está mais em uso. Ele chama o método Dispose com disposing definido como false para liberar recursos não gerenciados sem tentar descartar recursos gerenciados, que podem já ter sido coletados.
    /// </summary>
    ~RepoService()
    {
        // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
        Dispose(disposing: false);
    }

    /// <summary>
    ///  EN: Disposes the resources used by the RepoService instance, including the underlying database connection. This method should be called when the RepoService is no longer needed to ensure that all resources are properly released.
    ///  PT: Dispõe os recursos usados pela instância do RepoService, incluindo a conexão de banco de dados subjacente. Este método deve ser chamado quando o RepoService não for mais necessário para garantir que todos os recursos sejam adequadamente liberados.
    /// </summary>
    public void Dispose()
    {
        // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
        Dispose(disposing: true);
        GC.SuppressFinalize(this);
    }

    #endregion
}
