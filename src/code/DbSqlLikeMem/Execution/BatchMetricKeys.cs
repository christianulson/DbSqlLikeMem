namespace DbSqlLikeMem;

/// <summary>
/// EN: Holds the metric key fragments used to tag batch execution phases and type-specific counters.
/// PT: Contém os fragmentos de chave de métrica usados para marcar fases de execução em lote e contadores por tipo.
/// </summary>
internal static class BatchMetricKeys
{
    /// <summary>
    /// EN: Groups metric keys for batch execution phases.
    /// PT: Agrupa chaves de métrica para fases de execução em lote.
    /// </summary>
    public static class Phases
    {
        /// <summary>
        /// EN: Metric key for the materialization phase.
        /// PT: Chave de métrica para a fase de materializacao.
        /// </summary>
        public const string Materialization = "materialization";
        /// <summary>
        /// EN: Metric key for the reader phase.
        /// PT: Chave de métrica para a fase de leitura.
        /// </summary>
        public const string Reader = "reader";
        /// <summary>
        /// EN: Metric key for the fallback non-query phase.
        /// PT: Chave de métrica para a fase fallback de non-query.
        /// </summary>
        public const string FallbackNonQuery = "fallback-nonquery";
        /// <summary>
        /// EN: Metric key for the non-query phase.
        /// PT: Chave de métrica para a fase de non-query.
        /// </summary>
        public const string NonQuery = "nonquery";
        /// <summary>
        /// EN: Metric key for the scalar phase.
        /// PT: Chave de métrica para a fase escalar.
        /// </summary>
        public const string Scalar = "scalar";
    }

    /// <summary>
    /// EN: Groups metric key prefixes for batch execution types.
    /// PT: Agrupa prefixos de chave de métrica para tipos de execução em lote.
    /// </summary>
    public static class TypePrefixes
    {
        /// <summary>
        /// EN: Metric key prefix for materialization counters.
        /// PT: Prefixo de chave de métrica para contadores de materializacao.
        /// </summary>
        public const string Materialize = "materialize:";
        /// <summary>
        /// EN: Metric key prefix for reader counters.
        /// PT: Prefixo de chave de métrica para contadores de leitura.
        /// </summary>
        public const string Reader = "reader:";
        /// <summary>
        /// EN: Metric key prefix for fallback non-query counters.
        /// PT: Prefixo de chave de métrica para contadores de fallback de non-query.
        /// </summary>
        public const string FallbackNonQuery = "fallback-nonquery:";
        /// <summary>
        /// EN: Metric key prefix for non-query counters.
        /// PT: Prefixo de chave de métrica para contadores de non-query.
        /// </summary>
        public const string NonQuery = "nonquery:";
        /// <summary>
        /// EN: Metric key prefix for scalar counters.
        /// PT: Prefixo de chave de métrica para contadores escalares.
        /// </summary>
        public const string Scalar = "scalar:";
    }
}
