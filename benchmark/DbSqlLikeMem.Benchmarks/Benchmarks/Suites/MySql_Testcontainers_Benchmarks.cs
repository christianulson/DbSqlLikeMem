using DbSqlLikeMem.Benchmarks.Sessions.External;

namespace DbSqlLikeMem.Benchmarks.Suites;

/// <summary>
/// EN: Defines the MySQL benchmark suite backed by Testcontainers.
/// PT: Define a suite de benchmark de MySQL apoiada por Testcontainers.
/// </summary>
public class MySql_Testcontainers_Benchmarks : BenchmarkSuiteBase
{
    /// <summary>
    /// EN: Creates the benchmark session used by the MySQL Testcontainers suite.
    /// PT: Cria a sessao de benchmark usada pela suite MySQL Testcontainers.
    /// </summary>
    protected override IBenchmarkSession CreateSession() => new MySqlTestcontainersSession();

}

