#if NET462
using DbSqlLikeMem.Db2.TestTools;
using System.Globalization;
using DB2Connection = IBM.Data.DB2.Core.DB2Connection;
using DB2ConnectionStringBuilder = IBM.Data.DB2.Core.DB2ConnectionStringBuilder;
#elif NET6_0
using DbSqlLikeMem.Db2.TestTools;
using System.Globalization;
using DB2Connection = IBM.Data.DB2.Core.DB2Connection;
using DB2ConnectionStringBuilder = IBM.Data.DB2.Core.DB2ConnectionStringBuilder;
#elif NET8_0
using System.Globalization;
using DB2Connection = IBM.Data.Db2.DB2Connection;
using DB2ConnectionStringBuilder = IBM.Data.Db2.DB2ConnectionStringBuilder;
#endif
using Xunit.Sdk;

namespace DbSqlLikeMem.Db2.Test;

/// <summary>
/// EN: Creates Db2 fidelity connections with a provider-native connection string builder.
/// PT: Cria conexoes de fidelidade Db2 com um builder nativo do provedor.
/// </summary>
internal static class Db2ConnectionFactory
{
    private static readonly object EnsureLock = new();

    /// <summary>
    /// EN: Builds and opens a Db2 connection using the provider-specific connection string builder.
    /// PT: Monta e abre uma conexao Db2 usando o builder de string de conexao especifico do provedor.
    /// </summary>
    /// <param name="connectionString">EN: The base connection string value read from the test environment. PT: O valor base da string de conexao lido do ambiente de teste.</param>
    /// <returns>EN: A connection instance ready for use by the fidelity tests. PT: Uma instancia de conexao pronta para uso pelos testes de fidelidade.</returns>
    internal static DB2Connection Create(string connectionString)
    {
#if NET462 || NET6_0
        try
        {
            Db2NativeClientGuard.EnsureNativeClientAvailable();
        }
        catch (InvalidOperationException ex)
        {
            _ = ex;
            throw SkipException.ForSkip("Db2 native client is not available.");
        }
#endif
        var builder = new DB2ConnectionStringBuilder
        {
            Database = GetValue(connectionString, "DATABASE") ?? "BENCH",
            Server = GetValue(connectionString, "SERVER") ?? "127.0.0.1:15000",
            UserID = GetValue(connectionString, "USER_ID") ?? "db2inst1",
            Password = GetValue(connectionString, "PASSWORD") ?? "db2inst1",
            Pooling = false
        };

        EnsureUserTemporaryTablespace(builder.ConnectionString, builder.UserID);
        return new DB2Connection(builder.ConnectionString);
    }

    private static void EnsureUserTemporaryTablespace(string connectionString, string userId)
    {
        lock (EnsureLock)
        {
            using var connection = new DB2Connection(connectionString);
            connection.Open();

            const string tablespaceName = "USRTMPSPC32K";

            if (TablespaceExists(connection, tablespaceName))
            {
                return;
            }

            using (var command = connection.CreateCommand())
            {
                command.CommandText = $"CREATE USER TEMPORARY TABLESPACE {tablespaceName} PAGESIZE 32 K MANAGED BY AUTOMATIC STORAGE";
                command.ExecuteNonQuery();
            }

            using (var command = connection.CreateCommand())
            {
                command.CommandText = $"GRANT USE OF TABLESPACE {tablespaceName} TO USER {userId}";
                command.ExecuteNonQuery();
            }
        }
    }

    private static bool TablespaceExists(DB2Connection connection, string tablespaceName)
    {
        using var command = connection.CreateCommand();
        command.CommandText = $"SELECT COUNT(*) FROM SYSCAT.TABLESPACES WHERE TBSPACE = '{tablespaceName}'";
        var value = command.ExecuteScalar();
        return value is not null && Convert.ToInt32(value, CultureInfo.InvariantCulture) > 0;
    }

    private static string? GetValue(string connectionString, string key)
    {
        foreach (var segment in connectionString.Split(new[] { ';' }, StringSplitOptions.RemoveEmptyEntries))
        {
            var trimmedSegment = segment.Trim();
            var parts = trimmedSegment.Split(new[] { '=' }, 2);
            if (parts.Length == 2 && string.Equals(parts[0].Trim(), key, StringComparison.OrdinalIgnoreCase))
            {
                return parts[1].Trim();
            }
        }

        return null;
    }
}
