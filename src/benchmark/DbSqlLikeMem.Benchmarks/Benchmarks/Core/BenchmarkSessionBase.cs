using IBM.Data.Db2;
using Microsoft.Data.SqlClient;
using MySqlConnector;
using Npgsql;
using Oracle.ManagedDataAccess.Client;
using DbSqlLikeMem.TestTools.Performance;
using DbSqlLikeMem.TestTools.Schema;
using DbSqlLikeMem.TestTools.Query;
using System.Collections.Concurrent;

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
public abstract partial class BenchmarkSessionBase(
    ProviderSqlDialect dialect,
    BenchmarkEngine engine
    ) : IBenchmarkSession
{
    private static int _objectCounter;

    /// <summary>
    /// EN: Gets the SQL dialect abstraction used to generate provider-specific statements for the current session.
    /// PT-br: Obtém a abstração de dialeto SQL usada para gerar comandos específicos do provedor para a sessão atual.
    /// </summary>
    public ProviderSqlDialect Dialect { get; } = dialect;

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
    /// EN: Executes one benchmark feature and routes any provider-specific failure to the benchmark logger.
    /// PT-br: Executa um recurso de benchmark e encaminha qualquer falha especifica do provedor para o logger do benchmark.
    /// </summary>
    public virtual void Execute(BenchmarkFeatureId feature)
    {
        try
        {
            RunFeature(feature);
        }
        catch (InvalidOperationException ex)
        {
            LogBenchmarkIssue("NA-IOE", feature, ex);
        }
        catch (NotSupportedException ex)
        {
            LogBenchmarkIssue("NA-NSE", feature, ex);
        }
        catch (DB2Exception ex)
        {
            LogBenchmarkIssue("NA-DB2E", feature, ex);
        }
        catch (SqlException ex)
        {
            LogBenchmarkIssue("NA-SqlE", feature, ex);
        }
        catch (MySqlException ex)
        {
            LogBenchmarkIssue("NA-MSE", feature, ex);
        }
        catch (NpgsqlException ex)
        {
            LogBenchmarkIssue("NA-NE", feature, ex);
        }
        catch (OracleException ex)
        {
            LogBenchmarkIssue("NA-OE", feature, ex);
        }
        catch (Exception ex)
        {
            LogBenchmarkIssue("NA", feature, ex);
        }
    }

    /// <summary>
    /// EN: Dispatches the requested benchmark feature to the corresponding benchmark routine.
    /// PT-br: Encaminha o recurso de benchmark solicitado para a rotina de benchmark correspondente.
    /// </summary>
    /// <param name="feature">EN: The benchmark feature to execute. PT-br: O recurso de benchmark a ser executado.</param>
    /// <exception cref="NotSupportedException"></exception>
    /// <exception cref="ArgumentOutOfRangeException"></exception>
    public void RunFeature(BenchmarkFeatureId feature)
        => BenchmarkFeatureRegistry.Run(this, feature);

    private static readonly object _logSync = new();

    private static readonly ConcurrentDictionary<string, int> Errors = [];

    protected virtual void LogBenchmarkIssue(string txt, BenchmarkFeatureId feature, Exception ex)
    {
        var root = ex?.GetBaseException();
        var message = root is NotSupportedException
            ? $"[{txt}-{root.GetType().Name}] {feature}: {root.Message}{Environment.NewLine}{Environment.NewLine}"
            : $"[{txt}-{root?.GetType().Name}] {feature}: {root?.Message} -- {ex?.StackTrace}{Environment.NewLine}{Environment.NewLine}";

        Console.WriteLine(message);

        lock (_logSync)
        {
            var errorKey = $"{GetType().FullName}|{Dialect.DisplayName}|{feature}|{root?.GetType().FullName}|{root?.Message}";
            if (Errors.TryGetValue(errorKey, out int value))
            {
                Errors[errorKey] = value + 1;
                return;
            }
            Errors.GetOrAdd(errorKey, 0);

            var directory = BenchmarkLogPath.GetDirectory();
            Directory.CreateDirectory(directory);

            var file = BenchmarkLogPath.GetFilePath($"{GetType().FullName}-{Dialect.DisplayName}-errors.log");
            File.AppendAllText(
                file,
                message + Environment.NewLine);
        }
    }

    /// <summary>
    /// EN: Releases any resources allocated by the benchmark session.
    /// PT-br: Libera os recursos alocados pela sessão de benchmark.
    /// </summary>
    public virtual void Dispose()
    {
        DisposePreparedStates();
    }

    /// <summary>
    /// EN: Creates a new provider-specific connection instance for the current benchmark session.
    /// PT-br: Cria uma nova instância de conexão específica do provedor para a sessão de benchmark atual.
    /// </summary>
    /// <returns>EN: A new provider-specific connection instance. PT-br: Uma nova instância de conexão específica do provedor.</returns>
    protected abstract DbConnection CreateConnection();

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
    /// EN: Tries to drop a temporary table using best-effort cleanup semantics.
    /// PT-br: Tenta remover uma tabela temporaria usando uma limpeza de melhor esforco.
    /// </summary>
    /// <param name="connection">EN: The database connection used to execute the operation. PT-br: A conexao de banco de dados usada para executar a operação.</param>
    /// <param name="tableName">EN: The temporary table name targeted by the operation. PT-br: O nome da tabela temporaria alvo da operação.</param>
    protected void SafeDropTemporaryTable(DbConnection connection, FidelityTestContext context)
    {
        SafeExecute(connection, Dialect.DropTemporaryUsersTable(context));
    }

    /// <summary>
    /// EN: Tries to drop a sequence using best-effort cleanup semantics.
    /// PT-br: Tenta remover uma sequência usando uma limpeza de melhor esforço.
    /// </summary>
    /// <param name="connection">EN: The database connection used to execute the operation. PT-br: A conexão de banco de dados usada para executar a operação.</param>
    /// <param name="sequenceName">EN: The sequence name targeted by the operation. PT-br: O nome da sequência alvo da operação.</param>
    protected void SafeDropSequence(DbConnection connection, FidelityTestContext context)
    {
        SafeExecute(connection, Dialect.DropSequence(context));
    }

    /// <summary>
    /// EN: Executes a cleanup command while suppressing cleanup failures.
    /// PT-br: Executa um comando de limpeza suprimindo falhas durante a limpeza.
    /// </summary>
    /// <param name="connection">EN: The database connection used to execute the operation. PT-br: A conexão de banco de dados usada para executar a operação.</param>
    /// <param name="sql">EN: The SQL command text to execute. PT-br: O texto do comando SQL a ser executado.</param>
    protected void SafeExecute(DbConnection connection, string sql)
    {
        try
        {
            using var command = connection.CreateCommand();
            command.CommandText = sql;
            command.ExecuteNonQuery();
        }
        catch (Exception ex)
        {
            var root = ex.GetBaseException();
            if (IsDb2MissingObjectException(root) || IsOracleMissingObjectException(root))
            {
                return;
            }

            var message = root is NotSupportedException
                ? $"[SAFE-{root.GetType().Name}] {sql}: {root.Message}{Environment.NewLine}{Environment.NewLine}"
                : $"[SAFE-{root.GetType().Name}] {sql}: {root.Message} -- {ex.StackTrace}{Environment.NewLine}{Environment.NewLine}";

            Console.WriteLine(message);

            lock (_logSync)
            {
                var errorKey = $"{GetType().FullName}|{Dialect.DisplayName}|SAFE|{sql}|{root.GetType().FullName}|{root.Message}";
                if (Errors.TryGetValue(errorKey, out int value))
                {
                    Errors[errorKey] = value + 1;
                    return;
                }

                Errors.GetOrAdd(errorKey, 0);

                var file = BenchmarkLogPath.GetFilePath($"{GetType().FullName}-{Dialect.DisplayName}-errors.log");
                Directory.CreateDirectory(BenchmarkLogPath.GetDirectory());
                File.AppendAllText(
                    file,
                    message + Environment.NewLine);
            }
        }
    }

    private static bool IsDb2MissingObjectException(Exception ex)
        => ex is DB2Exception
            && (ex.Message.Contains("SQL0204N", StringComparison.OrdinalIgnoreCase)
                || ex.Message.Contains("42704", StringComparison.OrdinalIgnoreCase)
                || ex.Message.Contains("undefined name", StringComparison.OrdinalIgnoreCase)
                || ex.Message.Contains("not found", StringComparison.OrdinalIgnoreCase));

    private static bool IsOracleMissingObjectException(Exception ex)
        => ex is OracleException
            && (ex.Message.Contains("ORA-00942", StringComparison.OrdinalIgnoreCase)
                || ex.Message.Contains("table or view", StringComparison.OrdinalIgnoreCase)
                || ex.Message.Contains("tabela ou view", StringComparison.OrdinalIgnoreCase));


}

