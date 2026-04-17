using System.Data.Common;
using System.Diagnostics;
using System.Net.Sockets;
using System.Reflection;
using System.Text;
using DbSqlLikeMem.VisualStudioExtension.Core.Models;
using FirebirdSql.Data.FirebirdClient;
using IBM.Data.DB2.Core;
using Microsoft.Data.SqlClient;
using MySqlConnector;
using Npgsql;
using Oracle.ManagedDataAccess.Client;

namespace DbSqlLikeMem.VisualStudioExtension.XamlHarness;

internal sealed partial class HarnessEnvironmentManager
{
    private readonly List<ConnectionDefinition> preparedConnections = [];
    private readonly Dictionary<string, DbConnection> openConnectionsById = new(StringComparer.OrdinalIgnoreCase);
    private readonly SemaphoreSlim lifecycleLock = new(1, 1);
    private bool cleanupCompleted;
    private bool initializationCompleted;

    public Task<IReadOnlyCollection<ConnectionDefinition>> InitializeAsync(CancellationToken cancellationToken = default)
    {
        return InitializeCoreAsync(cancellationToken);
    }

    public async Task CleanupAsync()
    {
        await lifecycleLock.WaitAsync().ConfigureAwait(false);
        try
        {
            if (cleanupCompleted)
            {
                return;
            }

            cleanupCompleted = true;

            foreach (var connection in preparedConnections.AsEnumerable().Reverse())
            {
                try
                {
                    openConnectionsById.TryGetValue(connection.Id, out var dbConnection);
                    await CleanupConnectionAsync(connection, dbConnection, CancellationToken.None).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    Trace.WriteLine($"DbSqlLikeMem XAML harness cleanup failed for {connection.FriendlyName}: {ex}");
                }
            }

            foreach (var dbConnection in openConnectionsById.Values)
            {
                try
                {
                    dbConnection.Dispose();
                }
                catch (Exception ex)
                {
                    Trace.WriteLine($"DbSqlLikeMem XAML harness connection dispose failed: {ex}");
                }
            }

            openConnectionsById.Clear();
        }
        finally
        {
            lifecycleLock.Release();
        }
    }

    private async Task<IReadOnlyCollection<ConnectionDefinition>> InitializeCoreAsync(CancellationToken cancellationToken)
    {
        await lifecycleLock.WaitAsync().ConfigureAwait(false);
        try
        {
            if (initializationCompleted)
            {
                return [.. preparedConnections];
            }

            var connections = ResolveConnections();
            if (connections.Count == 0)
            {
                Trace.WriteLine("DbSqlLikeMem XAML harness did not resolve any benchmark connections.");
                initializationCompleted = true;
                return [];
            }

            preparedConnections.Clear();
            foreach (var connection in connections)
            {
                var seededConnection = await SeedConnectionAsync(connection, cancellationToken).ConfigureAwait(false);
                if (seededConnection is not null)
                {
                    preparedConnections.Add(seededConnection);
                }
            }
            initializationCompleted = true;
            return [.. preparedConnections];
        }
        finally
        {
            lifecycleLock.Release();
        }
    }

    private static IReadOnlyCollection<ConnectionDefinition> ResolveConnections()
    {
        var connections = new List<ConnectionDefinition>();

        TryAddConnection(connections, "harness-mysql", "MySql", "benchmark", "MySql Benchmark",
            ("MYSQL_CONNECTION_STRING", "Server=127.0.0.1;Port=13306;Database=benchmark;Uid=root;Pwd=root;Pooling=false;"));
        TryAddConnection(connections, "harness-mariadb", "MariaDb", "benchmark", "MariaDb Benchmark",
            ("MARIADB_CONNECTION_STRING", "Server=127.0.0.1;Port=13307;Database=benchmark;Uid=root;Pwd=root;Pooling=false;"));
        TryAddConnection(connections, "harness-postgresql", "PostgreSql", "benchmark", "PostgreSql Benchmark",
            ("POSTGRES_CONNECTION_STRING", "Host=127.0.0.1;Port=15432;Database=benchmark;Username=postgres;Password=postgres;Pooling=false;"));
        TryAddConnection(connections, "harness-sqlserver", "SqlServer", "master", "SqlServer Benchmark",
            ("SQLSERVER_CONNECTION_STRING", "Server=127.0.0.1,11433;Database=master;User Id=sa;Password=Your_password123;Encrypt=False;TrustServerCertificate=True;Pooling=false;"));
        TryAddConnection(connections, "harness-oracle", "Oracle", "benchmark", "Oracle Benchmark",
            ("ORACLE_CONNECTION_STRING", "User Id=benchmark;Password=benchmark;Data Source=127.0.0.1:15211/FREEPDB1;Pooling=false;"));
        TryAddConnection(connections, "harness-db2", "Db2", "BENCH", "Db2 Benchmark",
            ("DB2_CONNECTION_STRING", "Server=127.0.0.1:15000;Database=BENCH;UID=db2inst1;PWD=db2inst1;Pooling=false;Persist Security Info=True;"));
        TryAddConnection(connections, "harness-firebird", "Firebird", "benchmark", "Firebird Benchmark",
            ("FIREBIRD_CONNECTION_STRING", "User=benchmark;Password=benchmark;Database=127.0.0.1/13050:/var/lib/firebird/data/benchmark.fdb;Dialect=3;Charset=UTF8;Pooling=false;"));

        return connections;
    }

    private static void TryAddConnection(
        ICollection<ConnectionDefinition> connections,
        string id,
        string databaseType,
        string databaseName,
        string displayName,
        params (string EnvironmentVariableName, string DefaultValue)[] candidates)
    {
        if (!TryResolveConnectionString(out var connectionString, candidates))
        {
            Trace.WriteLine($"DbSqlLikeMem XAML harness skipped {displayName} because no connection string was found.");
            return;
        }

        connections.Add(new ConnectionDefinition(id, databaseType, databaseName, connectionString, displayName));
    }

    private static bool TryResolveConnectionString(out string connectionString, params (string EnvironmentVariableName, string DefaultValue)[] candidates)
    {
        foreach (var candidate in candidates)
        {
            var value = Environment.GetEnvironmentVariable(candidate.EnvironmentVariableName);
            if (!string.IsNullOrWhiteSpace(value))
            {
                connectionString = NormalizeConnectionString(candidate.EnvironmentVariableName, value!);
                return true;
            }

            if (!string.IsNullOrWhiteSpace(candidate.DefaultValue))
            {
                connectionString = NormalizeConnectionString(candidate.EnvironmentVariableName, candidate.DefaultValue);
                return true;
            }
        }

        connectionString = string.Empty;
        return false;
    }

    private static string NormalizeConnectionString(string variableName, string connectionString)
    {
        if (variableName.Contains("ORACLE", StringComparison.OrdinalIgnoreCase))
        {
            var builder = new DbConnectionStringBuilder
            {
                ConnectionString = connectionString
            };

            builder["Connection Timeout"] = 120;
            return builder.ConnectionString;
        }

        return connectionString;
    }

    private async Task<ConnectionDefinition?> SeedConnectionAsync(ConnectionDefinition connection, CancellationToken cancellationToken)
    {
        DbConnection? dbConnection = null;
        var openedSuccessfully = false;
        try
        {
            dbConnection = CreateConnection(connection);
            await OpenConnectionAsync(connection, dbConnection, cancellationToken).ConfigureAwait(false);
            openedSuccessfully = true;
            await CleanupObjectsAsync(connection, dbConnection, cancellationToken).ConfigureAwait(false);

            switch (DatabaseTypeNormalizer.NormalizeKey(connection.DatabaseType))
            {
                case "mysql":
                    await SeedMySqlFamilyAsync(dbConnection, includeSequence: false, cancellationToken).ConfigureAwait(false);
                    break;
                case "mariadb":
                    await SeedMySqlFamilyAsync(dbConnection, includeSequence: true, cancellationToken).ConfigureAwait(false);
                    break;
                case "postgresql":
                    await SeedPostgreSqlAsync(dbConnection, cancellationToken).ConfigureAwait(false);
                    break;
                case "sqlserver":
                case "sqlazure":
                case "azuresql":
                    await SeedSqlServerAsync(dbConnection, cancellationToken).ConfigureAwait(false);
                    break;
                case "oracle":
                    await SeedOracleAsync(dbConnection, cancellationToken).ConfigureAwait(false);
                    break;
                case "db2":
                    await SeedDb2Async(dbConnection, cancellationToken).ConfigureAwait(false);
                    break;
                case "firebird":
                    await SeedFirebirdAsync(dbConnection, cancellationToken).ConfigureAwait(false);
                    break;
                default:
                    Trace.WriteLine($"DbSqlLikeMem XAML harness does not support seeding provider '{connection.DatabaseType}'.");
                    break;
            }

            Trace.WriteLine($"DbSqlLikeMem XAML harness seeded {connection.FriendlyName}.");
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
        catch (Exception ex)
        {
            Trace.WriteLine(BuildExceptionReport($"DbSqlLikeMem XAML harness seed failed for {connection.FriendlyName}", ex));
        }
        finally
        {
            if (openedSuccessfully && dbConnection is not null)
            {
                openConnectionsById[connection.Id] = dbConnection;
            }
            else
            {
                dbConnection?.Dispose();
            }
        }

        return openedSuccessfully ? connection : null;
    }

    private async Task CleanupConnectionAsync(ConnectionDefinition connection, DbConnection? dbConnection, CancellationToken cancellationToken)
    {
        try
        {
            if (dbConnection is null)
            {
                Trace.WriteLine($"DbSqlLikeMem XAML harness cleanup skipped {connection.FriendlyName} because no open connection was retained.");
                return;
            }

            await CleanupObjectsAsync(connection, dbConnection, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            Trace.WriteLine(BuildExceptionReport($"DbSqlLikeMem XAML harness cleanup failed for {connection.FriendlyName}", ex));
        }
    }

    private static DbConnection CreateConnection(ConnectionDefinition connection)
        => DatabaseTypeNormalizer.NormalizeKey(connection.DatabaseType) switch
        {
            "mysql" => new MySqlConnection(connection.ConnectionString),
            "mariadb" => new MySqlConnection(connection.ConnectionString),
            "postgresql" => new NpgsqlConnection(connection.ConnectionString),
            "sqlserver" or "sqlazure" or "azuresql" => new SqlConnection(connection.ConnectionString),
            "oracle" => new OracleConnection(connection.ConnectionString),
            "db2" => CreateDb2Connection(connection.ConnectionString),
            "firebird" => new FbConnection(connection.ConnectionString),
            _ => throw new NotSupportedException($"Unsupported harness database type: {connection.DatabaseType}")
        };

    private static DB2Connection CreateDb2Connection(string connectionString)
    {
        var database = GetDb2ConnectionStringValue(connectionString, "DATABASE", "DBNAME", "DB") ?? "BENCH";
        var server = GetDb2ConnectionStringValue(connectionString, "SERVER") ?? "127.0.0.1:15000";
        var userId = GetDb2ConnectionStringValue(connectionString, "USER_ID", "UID", "USER") ?? "db2inst1";
        var password = GetDb2ConnectionStringValue(connectionString, "PASSWORD", "PWD") ?? "db2inst1";
        var pooling = TryGetDb2Pooling(connectionString) ?? false;

        var normalizedConnectionString = $"""
Server={server};Database={database};UID={userId};PWD={password};Pooling={pooling.ToString().ToLowerInvariant()};Persist Security Info=True;
""";

        return new DB2Connection(normalizedConnectionString);
    }

    private static string? GetDb2ConnectionStringValue(string connectionString, params string[] keys)
    {
        foreach (var segment in connectionString.Split(new[] { ';' }, StringSplitOptions.RemoveEmptyEntries))
        {
            var trimmedSegment = segment.Trim();
            var parts = trimmedSegment.Split(new[] { '=' }, 2);
            if (parts.Length != 2)
            {
                continue;
            }

            var key = parts[0].Trim();
            foreach (var expectedKey in keys)
            {
                if (string.Equals(key, expectedKey, StringComparison.OrdinalIgnoreCase))
                {
                    return parts[1].Trim();
                }
            }
        }

        return null;
    }

    private static bool? TryGetDb2Pooling(string connectionString)
    {
        var poolingValue = GetDb2ConnectionStringValue(connectionString, "POOLING");
        if (string.IsNullOrWhiteSpace(poolingValue))
        {
            return null;
        }

        if (bool.TryParse(poolingValue, out var pooling))
        {
            return pooling;
        }

        return null;
    }

    private static async Task CleanupObjectsAsync(
        ConnectionDefinition connection,
        DbConnection dbConnection,
        CancellationToken cancellationToken)
    {
        switch (DatabaseTypeNormalizer.NormalizeKey(connection.DatabaseType))
        {
            case "mysql":
                await CleanupMySqlFamilyAsync(dbConnection, includeSequence: false, cancellationToken).ConfigureAwait(false);
                break;
            case "mariadb":
                await CleanupMySqlFamilyAsync(dbConnection, includeSequence: true, cancellationToken).ConfigureAwait(false);
                break;
            case "postgresql":
                await CleanupPostgreSqlAsync(dbConnection, cancellationToken).ConfigureAwait(false);
                break;
            case "sqlserver":
            case "sqlazure":
            case "azuresql":
                await CleanupSqlServerAsync(dbConnection, cancellationToken).ConfigureAwait(false);
                break;
            case "oracle":
                await CleanupOracleAsync(dbConnection, cancellationToken).ConfigureAwait(false);
                break;
            case "db2":
                await CleanupDb2Async(dbConnection, cancellationToken).ConfigureAwait(false);
                break;
            case "firebird":
                await CleanupFirebirdAsync(dbConnection, cancellationToken).ConfigureAwait(false);
                break;
            default:
                Trace.WriteLine($"DbSqlLikeMem XAML harness does not support cleanup for provider '{connection.DatabaseType}'.");
                break;
        }
    }

    private static string Qualify(string? schemaName, string objectName)
        => string.IsNullOrWhiteSpace(schemaName) ? objectName : $"{schemaName}.{objectName}";

    private static async Task<bool> TryExecuteNonQueryAsync(
        DbConnection dbConnection,
        string sql,
        CancellationToken cancellationToken,
        string failureContext,
        bool ignoreOracleObjectNotFound = false,
        bool ignoreDb2UndefinedName = false)
    {
        try
        {
            await ExecuteNonQueryAsync(dbConnection, sql, cancellationToken).ConfigureAwait(false);
            return true;
        }
        catch (Exception ex)
        {
            if (ignoreOracleObjectNotFound && IsOracleObjectNotFound(ex))
            {
                return true;
            }

            if (ignoreDb2UndefinedName && IsDb2UndefinedName(ex))
            {
                return true;
            }

            Trace.WriteLine(BuildExceptionReport($"DbSqlLikeMem XAML harness {failureContext} failed", ex, sql));
            return false;
        }
    }

    private static async Task ExecuteNonQueryAsync(DbConnection dbConnection, string sql, CancellationToken cancellationToken)
    {
        using var command = dbConnection.CreateCommand();
        command.CommandText = sql;
        command.CommandTimeout = 120;
        _ = await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private static async Task OpenConnectionAsync(
        ConnectionDefinition connection,
        DbConnection dbConnection,
        CancellationToken cancellationToken)
    {
        if (!DatabaseTypeNormalizer.NormalizeKey(connection.DatabaseType).Equals("firebird", StringComparison.Ordinal))
        {
            await dbConnection.OpenAsync(cancellationToken).ConfigureAwait(false);
            return;
        }

        if (TryResolveFirebirdEndpoint(connection.ConnectionString, out var host, out var port))
        {
            await WaitForTcpPortAsync(host, port, cancellationToken).ConfigureAwait(false);
            await Task.Delay(TimeSpan.FromSeconds(5), cancellationToken).ConfigureAwait(false);
        }

        const int maxAttempts = 15;
        for (var attempt = 1; attempt <= maxAttempts; attempt++)
        {
            try
            {
                await dbConnection.OpenAsync(cancellationToken).ConfigureAwait(false);
                return;
            }
            catch (FbException ex) when (attempt < maxAttempts && IsFirebirdLoginFailure(ex))
            {
                await Task.Delay(TimeSpan.FromSeconds(4), cancellationToken).ConfigureAwait(false);
            }
        }
    }

    private static bool TryResolveFirebirdEndpoint(string connectionString, out string host, out int port)
    {
        host = string.Empty;
        port = 0;

        try
        {
            var builder = new DbConnectionStringBuilder
            {
                ConnectionString = connectionString
            };

            if (!builder.TryGetValue("Database", out var databaseValue))
            {
                return false;
            }

            var databaseText = Convert.ToString(databaseValue, System.Globalization.CultureInfo.InvariantCulture)?.Trim();
            if (string.IsNullOrWhiteSpace(databaseText))
            {
                return false;
            }

            var firstSlash = databaseText!.IndexOf('/');
            var firstColon = databaseText.IndexOf(':');
            if (firstSlash <= 0 || firstColon <= firstSlash + 1)
            {
                return false;
            }

            host = databaseText.Substring(0, firstSlash);
            var portText = databaseText.Substring(firstSlash + 1, firstColon - firstSlash - 1);
            return int.TryParse(portText, out port) && port > 0;
        }
        catch
        {
            return false;
        }
    }

    private static async Task WaitForTcpPortAsync(string host, int port, CancellationToken cancellationToken)
    {
        const int maxAttempts = 20;
        for (var attempt = 1; attempt <= maxAttempts; attempt++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            try
            {
                using var client = new TcpClient();
                var connectTask = client.ConnectAsync(host, port);
                var completed = await Task.WhenAny(connectTask, Task.Delay(TimeSpan.FromSeconds(2), cancellationToken)).ConfigureAwait(false);
                if (completed == connectTask)
                {
                    await connectTask.ConfigureAwait(false);
                    return;
                }
            }
            catch (SocketException) when (attempt < maxAttempts)
            {
            }
            catch (ObjectDisposedException) when (attempt < maxAttempts)
            {
            }

            await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken).ConfigureAwait(false);
        }
    }

    private static bool IsOracleObjectNotFound(Exception exception)
        => exception is OracleException oracleException
           && (oracleException.Number == 4043
               || oracleException.Number == 942
               || oracleException.Number == 2289);

    private static bool IsDb2UndefinedName(Exception exception)
        => exception is DB2Exception db2Exception
           && (db2Exception.Message.Contains("SQL0204N", StringComparison.OrdinalIgnoreCase)
               || db2Exception.Message.Contains("SQL0458N", StringComparison.OrdinalIgnoreCase));

    private static bool IsFirebirdLoginFailure(Exception exception)
        => exception is FbException fbException
           && fbException.Message.Contains("login", StringComparison.OrdinalIgnoreCase);

    private static string BuildExceptionReport(string headline, Exception exception, string? sql = null)
    {
        var builder = new StringBuilder();
        builder.AppendLine(headline);

        if (!string.IsNullOrWhiteSpace(sql))
        {
            builder.AppendLine($"SQL: {sql}");
        }

        AppendException(builder, exception, 0);
        return builder.ToString();
    }

    private static void AppendException(StringBuilder builder, Exception exception, int depth)
    {
        var indent = new string(' ', depth * 2);
        builder.AppendLine($"{indent}Type: {exception.GetType().FullName}");
        builder.AppendLine($"{indent}Message: {exception.Message}");
        builder.AppendLine($"{indent}HResult: 0x{exception.HResult:X8}");
        if (exception.TargetSite is not null)
        {
            builder.AppendLine($"{indent}TargetSite: {exception.TargetSite}");
        }
        if (!string.IsNullOrWhiteSpace(exception.StackTrace))
        {
            builder.AppendLine($"{indent}StackTrace:");
            builder.AppendLine(exception.StackTrace);
        }

        AppendInterestingProperties(builder, exception, indent + "  ");

        if (exception.InnerException is not null)
        {
            builder.AppendLine($"{indent}InnerException:");
            AppendException(builder, exception.InnerException, depth + 1);
        }
    }

    private static void AppendInterestingProperties(StringBuilder builder, Exception exception, string indent)
    {
        foreach (var property in exception.GetType()
                     .GetProperties(BindingFlags.Public | BindingFlags.Instance)
                     .Where(property =>
                         property.CanRead
                         && property.GetIndexParameters().Length == 0
                         && IsInterestingPropertyType(property.PropertyType))
                     .OrderBy(property => property.Name, StringComparer.Ordinal))
        {
            object? value;
            try
            {
                value = property.GetValue(exception);
            }
            catch
            {
                continue;
            }

            if (value is null)
            {
                continue;
            }

            builder.AppendLine($"{indent}{property.Name}: {value}");
        }
    }

    private static bool IsInterestingPropertyType(Type propertyType)
    {
        var underlyingType = Nullable.GetUnderlyingType(propertyType) ?? propertyType;
        return underlyingType.IsPrimitive
               || underlyingType.IsEnum
               || underlyingType == typeof(string)
               || underlyingType == typeof(decimal)
               || underlyingType == typeof(DateTime)
               || underlyingType == typeof(TimeSpan)
               || underlyingType == typeof(Guid);
    }

    private static partial Task SeedMySqlFamilyAsync(DbConnection dbConnection, bool includeSequence, CancellationToken cancellationToken);
    private static partial Task CleanupMySqlFamilyAsync(DbConnection dbConnection, bool includeSequence, CancellationToken cancellationToken);
    private static partial Task SeedPostgreSqlAsync(DbConnection dbConnection, CancellationToken cancellationToken);
    private static partial Task CleanupPostgreSqlAsync(DbConnection dbConnection, CancellationToken cancellationToken);
    private static partial Task SeedSqlServerAsync(DbConnection dbConnection, CancellationToken cancellationToken);
    private static partial Task CleanupSqlServerAsync(DbConnection dbConnection, CancellationToken cancellationToken);
    private static partial Task SeedOracleAsync(DbConnection dbConnection, CancellationToken cancellationToken);
    private static partial Task CleanupOracleAsync(DbConnection dbConnection, CancellationToken cancellationToken);
    private static partial Task SeedDb2Async(DbConnection dbConnection, CancellationToken cancellationToken);
    private static partial Task CleanupDb2Async(DbConnection dbConnection, CancellationToken cancellationToken);
    private static partial Task SeedFirebirdAsync(DbConnection dbConnection, CancellationToken cancellationToken);
    private static partial Task CleanupFirebirdAsync(DbConnection dbConnection, CancellationToken cancellationToken);
    private static partial Task InsertSampleDataAsync(DbConnection dbConnection, string customersTable, string ordersTable, CancellationToken cancellationToken);
}
