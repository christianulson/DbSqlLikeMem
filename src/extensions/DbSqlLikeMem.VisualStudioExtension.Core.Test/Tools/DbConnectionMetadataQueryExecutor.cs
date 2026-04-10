namespace DbSqlLikeMem.VisualStudioExtension.Core.Test.Tools;

/// <summary>
/// EN: Executes metadata queries against a real ADO.NET connection created for a benchmark provider.
/// PT: Executa consultas de metadados contra uma conexao ADO.NET real criada para um provedor de benchmark.
/// </summary>
public sealed class DbConnectionMetadataQueryExecutor : ISqlQueryExecutor
{
    private readonly Func<string, DbConnection> connectionFactory;
    private readonly Action<DbCommand>? configureCommand;
    private readonly Func<string, string> parameterNameFormatter;

    /// <summary>
    /// EN: Creates a metadata query executor that opens one connection per query using the supplied factory.
    /// PT: Cria um executor de consultas de metadados que abre uma conexao por consulta usando a factory informada.
    /// </summary>
    /// <param name="connectionFactory">EN: Factory used to create an unopened connection for each query. PT: Factory usada para criar uma conexao nao aberta para cada consulta.</param>
    /// <param name="configureCommand">EN: Optional command configurator applied before parameters are added. PT: Configurador opcional de comando aplicado antes de adicionar parametros.</param>
    /// <param name="parameterNameFormatter">EN: Optional formatter used to adapt parameter names for the provider. PT: Formatador opcional usado para adaptar nomes de parametro ao provedor.</param>
    public DbConnectionMetadataQueryExecutor(
        Func<string, DbConnection> connectionFactory,
        Action<DbCommand>? configureCommand = null,
        Func<string, string>? parameterNameFormatter = null)
    {
        ArgumentNullException.ThrowIfNull(connectionFactory);
        this.connectionFactory = connectionFactory;
        this.configureCommand = configureCommand;
        this.parameterNameFormatter = parameterNameFormatter ?? (name => name);
    }

    /// <inheritdoc />
    public Task<IReadOnlyCollection<IReadOnlyDictionary<string, object?>>> QueryAsync(
        ConnectionDefinition connection,
        string sql,
        IReadOnlyDictionary<string, object?> parameters,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(connection);
        ArgumentNullException.ThrowIfNull(sql);
        ArgumentNullException.ThrowIfNull(parameters);

        using var dbConnection = connectionFactory(connection.ConnectionString)
            ?? throw new InvalidOperationException("Falha ao criar conexao ADO.NET de benchmark.");

        dbConnection.Open();

        using var command = dbConnection.CreateCommand();
        configureCommand?.Invoke(command);
        command.CommandText = sql;

        foreach (var parameter in parameters)
        {
            var dbParameter = command.CreateParameter();
            dbParameter.ParameterName = parameterNameFormatter(parameter.Key);
            dbParameter.Value = parameter.Value ?? DBNull.Value;
            command.Parameters.Add(dbParameter);
        }

        using var reader = command.ExecuteReader();
        var rows = new List<IReadOnlyDictionary<string, object?>>();
        while (reader.Read())
        {
            var row = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
            for (var i = 0; i < reader.FieldCount; i++)
            {
                row[reader.GetName(i)] = reader.IsDBNull(i) ? null : reader.GetValue(i);
            }

            rows.Add(row);
        }

        return Task.FromResult<IReadOnlyCollection<IReadOnlyDictionary<string, object?>>>(rows);
    }
}
