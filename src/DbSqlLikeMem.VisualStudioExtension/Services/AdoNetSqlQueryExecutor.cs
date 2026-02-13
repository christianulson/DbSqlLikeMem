using System.Data.Common;
using System.Reflection;
using DbSqlLikeMem.VisualStudioExtension.Core.Generation;
using DbSqlLikeMem.VisualStudioExtension.Core.Models;

namespace DbSqlLikeMem.VisualStudioExtension.Services;

internal sealed class AdoNetSqlQueryExecutor : ISqlQueryExecutor
{
    private static readonly Dictionary<string, string[]> ProviderCandidates =
        new(StringComparer.OrdinalIgnoreCase)
        {
            ["sqlserver"] = ["System.Data.SqlClient", "Microsoft.Data.SqlClient"],
            ["postgresql"] = ["Npgsql"],
            ["mysql"] = ["MySqlConnector", "MySql.Data.MySqlClient"],
            ["oracle"] = ["Oracle.ManagedDataAccess.Client"],
            ["sqlite"] = ["Microsoft.Data.Sqlite", "System.Data.SQLite"],
            ["db2"] = ["IBM.Data.DB2", "IBM.Data.DB2.Core"]
        };

    private static readonly Dictionary<string, string[]> FactoryTypeCandidates =
        new(StringComparer.OrdinalIgnoreCase)
        {
            ["sqlserver"] = ["System.Data.SqlClient.SqlClientFactory", "Microsoft.Data.SqlClient.SqlClientFactory"],
            ["postgresql"] = ["Npgsql.NpgsqlFactory"],
            ["mysql"] = ["MySqlConnector.MySqlConnectorFactory", "MySql.Data.MySqlClient.MySqlClientFactory"],
            ["oracle"] = ["Oracle.ManagedDataAccess.Client.OracleClientFactory"],
            ["sqlite"] = ["Microsoft.Data.Sqlite.SqliteFactory", "System.Data.SQLite.SQLiteFactory"],
            ["db2"] = ["IBM.Data.DB2.DB2Factory", "IBM.Data.DB2.Core.DB2Factory"]
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
        var normalizedType = databaseType.Trim().ToLowerInvariant();
        if (!ProviderCandidates.TryGetValue(normalizedType, out var providerNames))
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

        if (FactoryTypeCandidates.TryGetValue(normalizedType, out var factoryTypeNames))
        {
            foreach (var factoryTypeName in factoryTypeNames)
            {
                var factory = TryCreateFactoryFromType(factoryTypeName);
                if (factory is not null)
                {
                    return factory;
                }
            }
        }

        throw new InvalidOperationException(
            $"Nenhum provider ADO.NET registrado para '{databaseType}'. Tentativas: {string.Join(", ", providerNames)}. " +
            "Instale o provider correspondente (pacote NuGet e/ou registro no machine.config)."
        );
    }

    private static DbProviderFactory? TryCreateFactoryFromType(string fullTypeName)
    {
        var providerType = Type.GetType(fullTypeName, throwOnError: false)
            ?? TryResolveFromLoadedAssemblies(fullTypeName)
            ?? TryResolveByLoadingAssembly(fullTypeName);

        if (providerType is null || !typeof(DbProviderFactory).IsAssignableFrom(providerType))
        {
            return null;
        }

        var instanceMember = providerType.GetMember("Instance", BindingFlags.Public | BindingFlags.Static).FirstOrDefault();
        if (instanceMember is PropertyInfo propertyInfo && propertyInfo.GetValue(null) is DbProviderFactory propertyFactory)
        {
            return propertyFactory;
        }

        if (instanceMember is FieldInfo fieldInfo && fieldInfo.GetValue(null) is DbProviderFactory fieldFactory)
        {
            return fieldFactory;
        }

        return Activator.CreateInstance(providerType) as DbProviderFactory;
    }

    private static Type? TryResolveFromLoadedAssemblies(string fullTypeName)
        => AppDomain.CurrentDomain
            .GetAssemblies()
            .Select(a => a.GetType(fullTypeName, throwOnError: false))
            .FirstOrDefault(t => t is not null);

    private static Type? TryResolveByLoadingAssembly(string fullTypeName)
    {
        var lastDotIndex = fullTypeName.LastIndexOf('.');
        if (lastDotIndex <= 0)
        {
            return null;
        }

        var assemblyNameCandidate = fullTypeName.Substring(0, lastDotIndex);
        try
        {
            var assembly = Assembly.Load(new AssemblyName(assemblyNameCandidate));
            return assembly.GetType(fullTypeName, throwOnError: false);
        }
        catch
        {
            return null;
        }
    }
}
