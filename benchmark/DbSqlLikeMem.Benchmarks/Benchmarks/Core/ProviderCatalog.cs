namespace DbSqlLikeMem.Benchmarks.Core;

/// <summary>
/// EN: Provides the catalog of providers included in the benchmark matrix.
/// PT-br: Fornece o catálogo de provedores incluídos na matriz de benchmark.
/// </summary>
public static class ProviderCatalog
{
    /// <summary>
    /// EN: Gets all provider definitions included in the current benchmark catalog.
    /// PT-br: Obtém todas as definições de provedores incluídas no catálogo atual.
    /// </summary>
    public static IReadOnlyList<ProviderDefinition> All { get; } =
    [
        new(
            BenchmarkProviderId.MySql,
            "MySQL",
            "8.4",
            BenchmarkEngine.Testcontainers,
            "mysql:8.4",
            SupportsUpsert: true,
            SupportsSequence: false,
            SupportsStringAggregate: true,
            SupportsComparableBenchmarks: true,
            IndexRefs: ["3.1.1", "3.1.2"],
            Notes: "Família MySQL comparada com Testcontainers.MySql e imagem mysql:8.4."),
        new(
            BenchmarkProviderId.SqlServer,
            "SQL Server",
            "2022",
            BenchmarkEngine.Testcontainers,
            "mcr.microsoft.com/mssql/server:2022-CU14-ubuntu-22.04",
            SupportsUpsert: true,
            SupportsSequence: true,
            SupportsStringAggregate: true,
            SupportsComparableBenchmarks: true,
            IndexRefs: ["3.2.1", "3.2.2"],
            Notes: "Família SQL Server 2022."),
        new(
            BenchmarkProviderId.SqlAzure,
            "SQL Azure",
            "170",
            BenchmarkEngine.NotAvailable,
            null,
            SupportsUpsert: true,
            SupportsSequence: true,
            SupportsStringAggregate: true,
            SupportsComparableBenchmarks: false,
            IndexRefs: ["3.2.2"],
            Notes: "Benchmark externo local não foi incluído; use SQL Server como proxy operacional."),
        new(
            BenchmarkProviderId.Oracle,
            "Oracle",
            "23",
            BenchmarkEngine.Testcontainers,
            "gvenzl/oracle-free:23-slim-faststart",
            SupportsUpsert: true,
            SupportsSequence: true,
            SupportsStringAggregate: true,
            SupportsComparableBenchmarks: true,
            IndexRefs: ["3.3.1", "3.3.2"],
            Notes: "Imagem Oracle Free 23 para benchmark local."),
        new(
            BenchmarkProviderId.Npgsql,
            "PostgreSQL / Npgsql",
            "17",
            BenchmarkEngine.Testcontainers,
            "postgres:17",
            SupportsUpsert: true,
            SupportsSequence: true,
            SupportsStringAggregate: true,
            SupportsComparableBenchmarks: true,
            IndexRefs: ["3.4.1", "3.4.2"],
            Notes: "Família PostgreSQL 17."),
        new(
            BenchmarkProviderId.Sqlite,
            "SQLite",
            "3",
            BenchmarkEngine.NativeAdoNet,
            null,
            SupportsUpsert: true,
            SupportsSequence: false,
            SupportsStringAggregate: true,
            SupportsComparableBenchmarks: true,
            IndexRefs: ["3.5.1", "3.5.2", "3.5.3"],
            Notes: "Comparação contra Microsoft.Data.Sqlite em memória, sem container."),
        new(
            BenchmarkProviderId.Db2,
            "DB2",
            "11",
            BenchmarkEngine.Testcontainers,
            "icr.io/db2_community/db2:12.1.0.0",
            SupportsUpsert: true,
            SupportsSequence: true,
            SupportsStringAggregate: true,
            SupportsComparableBenchmarks: true,
            IndexRefs: ["3.6.1", "3.6.2", "3.6.3"],
            Notes: "A constante da imagem está centralizada para você poder trocar facilmente para uma tag 11.5.x se quiser alinhar 1:1 com a faixa simulada."),
    ];
}
