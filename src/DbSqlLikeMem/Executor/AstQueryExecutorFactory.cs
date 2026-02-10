
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
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public static Dictionary<string, Func<
        DbConnectionMockBase,
        IDataParameterCollection,
        IAstQueryExecutor>> Executors { get; set; } = new Dictionary<string, Func<DbConnectionMockBase, IDataParameterCollection, IAstQueryExecutor>>(StringComparer.OrdinalIgnoreCase);


    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public static IAstQueryExecutor Create(
        ISqlDialect dialect,
        DbConnectionMockBase connection,
        IDataParameterCollection parameters)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(dialect, nameof(dialect));
        ArgumentNullExceptionCompatible.ThrowIfNull(connection, nameof(connection));
        ArgumentNullExceptionCompatible.ThrowIfNull(parameters, nameof(parameters));

        if(Executors.TryGetValue(dialect.Name, out var exc))
            return exc(connection, parameters);

        // Preparado para futura implementação.
        return new NotImplementedAstQueryExecutor(dialect.Name);
    }

    private sealed class NotImplementedAstQueryExecutor(string dialectName)
        : IAstQueryExecutor
    {
        private readonly string _dialectName = dialectName;

        /// <summary>
        /// Auto-generated summary.
        /// </summary>
        public TableResultMock ExecuteSelect(SqlSelectQuery q)
            => throw new NotSupportedException(
                $"AST executor não implementado para dialeto '{_dialectName}'. " +
                "Implemente um executor e registre no AstQueryExecutorFactory.");

        /// <summary>
        /// Auto-generated summary.
        /// </summary>
        public TableResultMock ExecuteUnion(
            IReadOnlyList<SqlSelectQuery> parts,
            IReadOnlyList<bool> allFlags,
            string? sqlContextForErrors = null)
            => throw new NotSupportedException(
                $"AST executor não implementado para dialeto '{_dialectName}'. " +
                "Implemente um executor e registre no AstQueryExecutorFactory.");
    }
}
