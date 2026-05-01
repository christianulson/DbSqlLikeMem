using DbSqlLikeMem.Benchmarks.Sessions.DbSqlLikeMem;

namespace DbSqlLikeMem.Benchmarks.Suites;

/// <summary>
/// EN: Defines the DbSqlLikeMem benchmark suite for MySQL.
/// PT-br: Define a suite de benchmark DbSqlLikeMem para MySQL.
/// </summary>
public class MySql_DbSqlLikeMem_Benchmarks : BenchmarkSuiteBase
{
    /// <summary>
    /// EN: Creates the benchmark session used by the MySQL DbSqlLikeMem suite.
    /// PT-br: Cria a sessao de benchmark usada pela suite DbSqlLikeMem de MySQL.
    /// </summary>
    protected override IBenchmarkSession CreateSession() => new MySqlDbSqlLikeMemSession();

}

