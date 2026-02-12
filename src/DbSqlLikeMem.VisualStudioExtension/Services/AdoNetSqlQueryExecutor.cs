using System.Collections.Generic;
using System.Data.Common;
using DbSqlLikeMem.VisualStudioExtension.Core.Generation;
using DbSqlLikeMem.VisualStudioExtension.Core.Models;

namespace DbSqlLikeMem.VisualStudioExtension.Services;

internal sealed class AdoNetSqlQueryExecutor : ISqlQueryExecutor
{
    private static readonly IReadOnlyDictionary<string, string[]> ProviderCandidates =
        new Dictionary<string, string[]>(StringComparer.OrdinalIgnoreCase)
        {
            ["sqlserver"] = ["System.Data.SqlClient", "Microsoft.Data.SqlClient"],
            ["postgresql"] = ["Npgsql"],
            ["mysql"] = ["MySqlConnector", "MySql.Data.MySqlClient"],
            ["oracle"] = ["Oracle.ManagedDataAccess.Client"],
            ["sqlite"] = ["Microsoft.Data.Sqlite", "System.Data.SQLite"],
            ["db2"] = ["IBM.Data.DB2", "IBM.Data.DB2.Core"]
        };

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
                row[reader.GetName(i)] = await reader.IsDBNullAsync(i, cancellationToken) ? null : reader.GetValue(i);
            }

            rows.Add(row);
        }

        return rows;
    }

    internal static DbProviderFactory GetFactory(string databaseType)
    {
        if (!ProviderCandidates.TryGetValue(databaseType.Trim().ToLowerInvariant(), out var providerNames))
        {
            throw new NotSupportedException($"Banco não suportado para conexão real: {databaseType}");
        }

        foreach (var providerName in providerNames)
        {
            try
            {
                return DbProviderFactories.GetFactory(providerName);
            }
            catch (ArgumentException)
            {
                // try next
            }
        }

        throw new InvalidOperationException(
            $"Nenhum provider ADO.NET registrado para '{databaseType}'. Tentativas: {string.Join(", ", providerNames)}. " +
            "Instale o provider correspondente (pacote NuGet e/ou registro no machine.config)."
        );
    }
}
