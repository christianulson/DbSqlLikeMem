using DbSqlLikeMem.Benchmarks.Core;
using DbSqlLikeMem.Benchmarks.Sessions.DbSqlLikeMem;
using DbSqlLikeMem.Benchmarks.Sessions.External;
using Xunit;

namespace DbSqlLikeMem.Benchmarks.Test;

/// <summary>
/// EN: Verifies the SQLite benchmark hot paths still execute on both the mock and the native provider.
/// PT: Verifica se os caminhos quentes de benchmark do SQLite ainda executam tanto no provider mock quanto no nativo.
/// </summary>
public sealed class SqliteHotPathGuardTests
{
    /// <summary>
    /// EN: Verifies the connection bootstrap and core query-path benchmark features still run on SQLite.
    /// PT: Verifica se o bootstrap de conexao e os recursos principais de caminho de consulta ainda executam no SQLite.
    /// </summary>
    [Theory]
    [MemberData(nameof(SqliteConnectionAndQueryHotPathCases))]
    public void SqliteSessions_RunConnectionAndQueryHotPathGuards(
        string sessionName,
        Func<BenchmarkSessionBase> factory,
        BenchmarkFeatureId[] features)
        => RunHotPathFeatures(sessionName, factory, features);

    /// <summary>
    /// EN: Verifies the SQLite window-function benchmark features still run on both providers.
    /// PT: Verifica se os recursos de benchmark de funcoes de janela do SQLite ainda executam em ambos os providers.
    /// </summary>
    [Theory]
    [MemberData(nameof(SqliteWindowHotPathCases))]
    public void SqliteSessions_RunWindowHotPathGuards(
        string sessionName,
        Func<BenchmarkSessionBase> factory,
        BenchmarkFeatureId[] features)
        => RunHotPathFeatures(sessionName, factory, features);

    /// <summary>
    /// EN: Verifies the SQLite string-aggregation benchmark features still run on both providers.
    /// PT: Verifica se os recursos de benchmark de agregacao de strings do SQLite ainda executam em ambos os providers.
    /// </summary>
    [Theory]
    [MemberData(nameof(SqliteStringAggregateHotPathCases))]
    public void SqliteSessions_RunStringAggregateHotPathGuards(
        string sessionName,
        Func<BenchmarkSessionBase> factory,
        BenchmarkFeatureId[] features)
        => RunHotPathFeatures(sessionName, factory, features);

    /// <summary>
    /// EN: Provides SQLite hot-path cases for the connection bootstrap and core query benchmarks.
    /// PT: Fornece casos de hot path do SQLite para o bootstrap de conexao e os benchmarks de consulta principais.
    /// </summary>
    public static IEnumerable<object[]> SqliteConnectionAndQueryHotPathCases()
        => [Case("Sqlite", () => new SqliteDbSqlLikeMemSession(), ConnectionAndQueryHotPathFeatures()),
            Case("Sqlite native", () => new SqliteNativeSession(), ConnectionAndQueryHotPathFeatures())];

    /// <summary>
    /// EN: Provides SQLite hot-path cases for window-function benchmarks.
    /// PT: Fornece casos de hot path do SQLite para benchmarks de funcoes de janela.
    /// </summary>
    public static IEnumerable<object[]> SqliteWindowHotPathCases()
        => [Case("Sqlite", () => new SqliteDbSqlLikeMemSession(), WindowHotPathFeatures()),
            Case("Sqlite native", () => new SqliteNativeSession(), WindowHotPathFeatures())];

    /// <summary>
    /// EN: Provides SQLite hot-path cases for string-aggregation benchmarks.
    /// PT: Fornece casos de hot path do SQLite para benchmarks de agregacao de strings.
    /// </summary>
    public static IEnumerable<object[]> SqliteStringAggregateHotPathCases()
        => [Case("Sqlite", () => new SqliteDbSqlLikeMemSession(), StringAggregateHotPathFeatures()),
            Case("Sqlite native", () => new SqliteNativeSession(), StringAggregateHotPathFeatures())];

    private static BenchmarkFeatureId[] ConnectionAndQueryHotPathFeatures()
        => [
            BenchmarkFeatureId.ConnectionOpen,
            BenchmarkFeatureId.ParameterProjection,
            BenchmarkFeatureId.SelectByPk,
            BenchmarkFeatureId.SelectJoin,
            BenchmarkFeatureId.SelectExistsPredicate,
            BenchmarkFeatureId.SelectInSubquery,
            BenchmarkFeatureId.SelectNotInSubquery,
            BenchmarkFeatureId.SelectScalarSubquery
        ];

    private static BenchmarkFeatureId[] WindowHotPathFeatures()
        => [
            BenchmarkFeatureId.WindowRowNumber,
            BenchmarkFeatureId.WindowRankDenseRank,
            BenchmarkFeatureId.WindowFirstLastValue,
            BenchmarkFeatureId.WindowNthValue
        ];

    private static BenchmarkFeatureId[] StringAggregateHotPathFeatures()
        => [
            BenchmarkFeatureId.StringAggregate,
            BenchmarkFeatureId.StringAggregateOrdered,
            BenchmarkFeatureId.StringAggregateLargeGroup
        ];

    private static object[] Case(
        string sessionName,
        Func<BenchmarkSessionBase> factory,
        BenchmarkFeatureId[] features)
        => [sessionName, factory, features];

    private static void RunHotPathFeatures(
        string sessionName,
        Func<BenchmarkSessionBase> factory,
        BenchmarkFeatureId[] features)
    {
        using var session = factory();
        session.Initialize();

        foreach (var feature in features)
        {
            session.RunFeature(feature);
        }

        GC.KeepAlive(sessionName);
    }
}
