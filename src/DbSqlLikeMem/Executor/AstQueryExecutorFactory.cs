
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
        DbConnectionMockBase,
        IDataParameterCollection,
        IAstQueryExecutor>> _executors = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// EN: Implements this member.
    /// PT: Implementa este membro.
    /// </summary>
    public static IReadOnlyDictionary<string, Func<
        DbConnectionMockBase,
        IDataParameterCollection,
        IAstQueryExecutor>> Executors => _executors;

    /// <summary>
    /// EN: Registers a dialect executor when it is not already available.
    /// PT: Registra um executor de dialeto quando ele ainda nao esta disponivel.
    /// </summary>
    public static void RegisterExecutor(
        string dialectName,
        Func<DbConnectionMockBase, IDataParameterCollection, IAstQueryExecutor> executorFactory)
    {
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(dialectName, nameof(dialectName));
        ArgumentNullExceptionCompatible.ThrowIfNull(executorFactory, nameof(executorFactory));
        _executors.TryAdd(dialectName, executorFactory);
    }

    /// <summary>
    /// EN: Implements Create.
    /// PT: Implementa Create.
    /// </summary>
    public static IAstQueryExecutor Create(
        ISqlDialect dialect,
        DbConnectionMockBase connection,
        IDataParameterCollection parameters)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(dialect, nameof(dialect));
        ArgumentNullExceptionCompatible.ThrowIfNull(connection, nameof(connection));
        ArgumentNullExceptionCompatible.ThrowIfNull(parameters, nameof(parameters));

        if (_executors.TryGetValue(dialect.Name, out var exc))
            return exc(connection, parameters);

        if (dialect.Name.Equals("auto", StringComparison.OrdinalIgnoreCase)
            && _executors.TryGetValue(connection.Db.Dialect.Name, out var providerExecutor))
        {
            return providerExecutor(connection, parameters);
        }

        // Preparado para futura implementação.
        return new NotImplementedAstQueryExecutor(dialect.Name);
    }

    private sealed class NotImplementedAstQueryExecutor(string dialectName)
        : IAstQueryExecutor
    {
        private readonly string _dialectName = dialectName;

        /// <summary>
        /// EN: Implements ExecuteSelect.
        /// PT: Implementa ExecuteSelect.
        /// </summary>
        public TableResultMock ExecuteSelect(SqlSelectQuery q)
            => throw new NotSupportedException(
                $"AST executor não implementado para dialeto '{_dialectName}'. " +
                "Implemente um executor e registre no AstQueryExecutorFactory.");

        /// <summary>
        /// EN: Implements ExecuteUnion.
        /// PT: Implementa ExecuteUnion.
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
