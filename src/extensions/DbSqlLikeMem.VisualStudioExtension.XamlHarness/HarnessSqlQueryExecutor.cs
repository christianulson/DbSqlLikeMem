using System.Data.Common;
using DbSqlLikeMem.VisualStudioExtension.Core.Generation;
using DbSqlLikeMem.VisualStudioExtension.Core.Models;
using FirebirdSql.Data.FirebirdClient;
using IBM.Data.DB2.Core;
using Microsoft.Data.SqlClient;
using MySqlConnector;
using Npgsql;
using Oracle.ManagedDataAccess.Client;

namespace DbSqlLikeMem.VisualStudioExtension.XamlHarness;

internal sealed class HarnessSqlQueryExecutor : ISqlQueryExecutor
{
    public async Task<IReadOnlyCollection<IReadOnlyDictionary<string, object?>>> QueryAsync(
        ConnectionDefinition connection,
        string sql,
        IReadOnlyDictionary<string, object?> parameters,
        CancellationToken cancellationToken = default)
    {
        var factory = GetFactory(connection.DatabaseType);
        using var dbConnection = factory.CreateConnection() ?? throw new InvalidOperationException("Falha ao criar conexão ADO.NET.");
        dbConnection.ConnectionString = connection.ConnectionString;
        await dbConnection.OpenAsync(cancellationToken).ConfigureAwait(false);

        using var command = dbConnection.CreateCommand();
        command.CommandText = sql;

        foreach (var parameterPair in parameters)
        {
            var parameter = command.CreateParameter();
            parameter.ParameterName = parameterPair.Key.TrimStart('@', ':', '?');
            parameter.Value = parameterPair.Value ?? DBNull.Value;
            command.Parameters.Add(parameter);
        }

        var rows = new List<IReadOnlyDictionary<string, object?>>();
        using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            var row = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
            for (var i = 0; i < reader.FieldCount; i++)
            {
                row[reader.GetName(i)] = await reader.IsDBNullAsync(i, cancellationToken).ConfigureAwait(false) ? null : reader.GetValue(i);
            }

            rows.Add(row);
        }

        return rows;
    }

    private static DbProviderFactory GetFactory(string databaseType)
    {
        var normalizedType = DatabaseTypeNormalizer.NormalizeKey(databaseType);
        return normalizedType switch
        {
            "sqlserver" or "sqlazure" or "azuresql" => SqlClientFactory.Instance,
            "postgresql" => NpgsqlFactory.Instance,
            "mysql" or "mariadb" => MySqlConnectorFactory.Instance,
            "oracle" => OracleClientFactory.Instance,
            "db2" => DB2Factory.Instance,
            "firebird" => FirebirdClientFactory.Instance,
            _ => throw new NotSupportedException($"Banco não suportado para conexão real: {databaseType}")
        };
    }
}
