using System.Data.Common;
using DbSqlLikeMem.VisualStudioExtension.Core.Generation;
using DbSqlLikeMem.VisualStudioExtension.Core.Models;
using FirebirdSql.Data.FirebirdClient;
using IBM.Data.DB2.Core;
using Microsoft.Data.SqlClient;
using MySqlConnector;
using Npgsql;
using Oracle.ManagedDataAccess.Client;

namespace DbSqlLikeMem.VisualStudioExtension.Services;

internal sealed class AdoNetSqlQueryExecutor : ISqlQueryExecutor
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
        await dbConnection.OpenAsync(cancellationToken);

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
        using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            var row = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
            for (var i = 0; i < reader.FieldCount; i++)
            {
                row[reader.GetName(i)] = await reader.IsDBNullAsync(i, cancellationToken) ? null : reader.GetValue(i);
            }

            rows.Add(row);
        }

        return rows;
    }

    internal static DbProviderFactory GetFactory(string databaseType)
    {
        var normalizedType = DatabaseTypeNormalizer.NormalizeKey(databaseType);
        return normalizedType switch
        {
            "sqlserver" or "sqlazure" or "azuresql" => SqlClientFactory.Instance,
            "postgresql" => NpgsqlFactory.Instance,
            "mysql" or "mariadb" => MySqlConnectorFactory.Instance,
            "oracle" => OracleClientFactory.Instance,
            "sqlite" => Microsoft.Data.Sqlite.SqliteFactory.Instance,
            "db2" => DB2Factory.Instance,
            "firebird" => FirebirdClientFactory.Instance,
            _ => throw new NotSupportedException($"Banco não suportado para conexão real: {databaseType}")
        };
    }
}
