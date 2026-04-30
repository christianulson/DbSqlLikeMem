namespace DbSqlLikeMem;

/// <summary>
/// EN: Holds the performance metric keys emitted by the mock execution pipeline.
/// PT: Contém as chaves de métricas de desempenho emitidas pelo pipeline de execucao do mock.
/// </summary>
internal static class DbPerformanceMetricKeys
{
    /// <summary>
    /// EN: Metric key for SQL parsing time.
    /// PT: Chave de métrica para o tempo de parsing de SQL.
    /// </summary>
    public const string SqlParse = "sql.parse";
    /// <summary>
    /// EN: Metric key for index rebuild time.
    /// PT: Chave de métrica para o tempo de reconstrução de indice.
    /// </summary>
    public const string IndexRebuild = "index.rebuild";
    /// <summary>
    /// EN: Metric key for index update time.
    /// PT: Chave de métrica para o tempo de atualização de indice.
    /// </summary>
    public const string IndexUpdate = "index.update";
    /// <summary>
    /// EN: Metric key for index removal time.
    /// PT: Chave de métrica para o tempo de remocao de indice.
    /// </summary>
    public const string IndexRemove = "index.remove";
    /// <summary>
    /// EN: Metric key for index shift time.
    /// PT: Chave de métrica para o tempo de deslocamento de indice.
    /// </summary>
    public const string IndexShift = "index.shift";
    /// <summary>
    /// EN: Metric key for LINQ plan materialization time.
    /// PT: Chave de métrica para o tempo de materializacao de plano LINQ.
    /// </summary>
    public const string MaterializationLinqPlan = "materialization.linq.plan";
    /// <summary>
    /// EN: Metric key for LINQ row materialization time.
    /// PT: Chave de métrica para o tempo de materializacao de linha LINQ.
    /// </summary>
    public const string MaterializationLinqRow = "materialization.linq.row";
    /// <summary>
    /// EN: Metric key for object row materialization time.
    /// PT: Chave de métrica para o tempo de materializacao de linha de objeto.
    /// </summary>
    public const string MaterializationObjectRow = "materialization.object.row";
    /// <summary>
    /// EN: Metric key for row snapshot creation time.
    /// PT: Chave de métrica para o tempo de criacao de snapshot de linha.
    /// </summary>
    public const string RowSnapshot = "row.snapshot";
}
