using DbSqlLikeMem.Benchmarks.Sessions.DbSqlLikeMem;

namespace DbSqlLikeMem.Benchmarks.Suites;

/// <summary>
/// EN: Defines the DbSqlLikeMem benchmark suite for SQLite.
/// PT-br: Define a suite de benchmark DbSqlLikeMem para SQLite.
/// </summary>
public class Sqlite_DbSqlLikeMem_Benchmarks : BenchmarkSuiteBase
{
    /// <summary>
    /// EN: Creates the benchmark session used by the SQLite DbSqlLikeMem suite.
    /// PT-br: Cria a sessao de benchmark usada pela suite DbSqlLikeMem de SQLite.
    /// </summary>
    protected override IBenchmarkSession CreateSession() => new SqliteDbSqlLikeMemSession();

}

///// <summary>
///// EN: Defines the DbSqlLikeMem benchmark suite for SQLite.
///// PT-br: Define a suite de benchmark DbSqlLikeMem para SQLite.
///// </summary>
//public class Sqlite_DbSqlLikeMem_Benchmarks2 : BenchmarkSuiteBaseNew<>
//{


//}