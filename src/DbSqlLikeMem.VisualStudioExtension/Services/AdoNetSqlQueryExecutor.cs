using System.Collections.Generic;
using System.Data.Common;
using DbSqlLikeMem.VisualStudioExtension.Core.Generation;
using DbSqlLikeMem.VisualStudioExtension.Core.Models;

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
            parameter.ParameterName = parameterPair.Key.StartsWith("@", StringComparison.Ordinal) ? parameterPair.Key : $"@{parameterPair.Key}";
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
                row[reader.GetName(i)] = reader.IsDBNull(i) ? null : reader.GetValue(i);
            }

            rows.Add(row);
        }

        return rows;
    }

    internal static DbProviderFactory GetFactory(string databaseType)
    {
        var provider = databaseType.Trim().ToLowerInvariant() switch
        {
            "sqlserver" => "System.Data.SqlClient",
            "postgresql" => "Npgsql",
            "mysql" => "MySql.Data.MySqlClient",
            "oracle" => "Oracle.ManagedDataAccess.Client",
            "sqlite" => "Microsoft.Data.Sqlite",
            "db2" => "IBM.Data.DB2",
            _ => throw new NotSupportedException($"Banco não suportado para conexão real: {databaseType}")
        };

        return DbProviderFactories.GetFactory(provider);
    }
}
