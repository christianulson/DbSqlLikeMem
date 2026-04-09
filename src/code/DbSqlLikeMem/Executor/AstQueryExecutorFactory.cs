using System.Collections.Concurrent;

namespace DbSqlLikeMem;

/// <summary>
/// Factory central para criar o executor do AST por dialeto.
///
/// Hoje o projeto tem implementação real para MySQL.
/// Para SQL Server/Oracle/Postgre deixamos um executor "placeholder" que falha de forma explícita,
/// evitando "build quebrar" quando você começar a plugar os novos dialetos.
/// </summary>
internal static class AstQueryExecutorFactory
{
    private static readonly ConcurrentDictionary<string, Func<
        QueryExecutionContext,
        IAstQueryExecutor>> _executors = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// EN: Gets the registered AST executors keyed by dialect name.
    /// PT: Obtem os executores de AST registrados pela chave do nome do dialeto.
    /// </summary>
    public static IReadOnlyDictionary<string, Func<
        QueryExecutionContext,
        IAstQueryExecutor>> Executors => _executors;

    /// <summary>
    /// EN: Registers a dialect executor when it is not already available.
    /// PT: Registra um executor de dialeto quando ele ainda nao esta disponivel.
    /// </summary>
    public static void RegisterExecutor(
        string dialectName,
        Func<QueryExecutionContext, IAstQueryExecutor> executorFactory)
    {
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(dialectName, nameof(dialectName));
        ArgumentNullExceptionCompatible.ThrowIfNull(executorFactory, nameof(executorFactory));
        _executors.TryAdd(dialectName, executorFactory);
    }

    /// <summary>
    /// EN: Creates an AST executor for the requested dialect or returns a fallback executor when one is unavailable.
    /// PT: Cria um executor de AST para o dialeto solicitado ou retorna um executor fallback quando ele nao estiver disponivel.
    /// </summary>
    public static IAstQueryExecutor Create(
        ISqlDialect dialect,
        DbConnectionMockBase connection,
        IDataParameterCollection parameters)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(dialect, nameof(dialect));
        ArgumentNullExceptionCompatible.ThrowIfNull(connection, nameof(connection));
        ArgumentNullExceptionCompatible.ThrowIfNull(parameters, nameof(parameters));

        return Create(new QueryExecutionContext(connection, dialect, (DbParameterCollection)parameters));
    }

    /// <summary>
    /// EN: Creates an AST executor from a pre-built query execution context.
    /// PT: Cria um executor de AST a partir de um contexto de execução de query pre-construído.
    /// </summary>
    public static IAstQueryExecutor Create(QueryExecutionContext context)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(context, nameof(context));

        if (_executors.TryGetValue(context.Dialect.Name, out var exc))
            return exc(context);

        if (context.Dialect.Name.Equals("auto", StringComparison.OrdinalIgnoreCase)
            && _executors.TryGetValue(context.Connection.ProviderExecutionDialect.Name, out var providerExecutor))
        {
            return providerExecutor(context);
        }

        // Preparado para futura implementação.
        return new NotImplementedAstQueryExecutor(context.Dialect.Name);
    }

    private sealed class NotImplementedAstQueryExecutor(string dialectName)
        : IAstQueryExecutor
    {
        private readonly string _dialectName = dialectName;

        /// <summary>
        /// EN: Executes a SELECT through the fallback executor and reports that the dialect is not implemented.
        /// PT: Executa um SELECT pelo executor fallback e informa que o dialeto nao esta implementado.
        /// </summary>
        public TableResultMock ExecuteSelect(SqlSelectQuery q)
            => throw new NotSupportedException(
                $"AST executor não implementado para dialeto '{_dialectName}'. " +
                "Implemente um executor e registre no AstQueryExecutorFactory.");

        /// <summary>
        /// EN: Executes a UNION through the fallback executor and reports that the dialect is not implemented.
        /// PT: Executa um UNION pelo executor fallback e informa que o dialeto nao esta implementado.
        /// </summary>
        public TableResultMock ExecuteUnion(
            IReadOnlyList<SqlSelectQuery> parts,
            IReadOnlyList<bool> allFlags,
            IReadOnlyList<SqlOrderByItem>? orderBy = null,
            SqlRowLimit? rowLimit = null,
            string? sqlContextForErrors = null)
            => throw new NotSupportedException(
                $"AST executor não implementado para dialeto '{_dialectName}'. " +
                "Implemente um executor e registre no AstQueryExecutorFactory.");
    }
}
