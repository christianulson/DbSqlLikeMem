using DbSqlLikeMem.Benchmarks.Sessions.External;

namespace DbSqlLikeMem.Benchmarks.Suites;

/// <summary>
/// EN: Defines the SQLite native benchmark suite.
/// PT: Define a suite de benchmark nativa de SQLite.
/// </summary>
public class Sqlite_Native_Benchmarks : BenchmarkSuiteBase
{
    /// <summary>
    /// EN: Creates the benchmark session used by the native SQLite suite.
    /// PT: Cria a sessao de benchmark usada pela suite nativa de SQLite.
    /// </summary>
    protected override IBenchmarkSession CreateSession() => new SqliteNativeSession();

}

