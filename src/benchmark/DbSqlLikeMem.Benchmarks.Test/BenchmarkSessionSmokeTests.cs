using DbSqlLikeMem.Benchmarks.Sessions.DbSqlLikeMem;

namespace DbSqlLikeMem.Benchmarks.Test;

/// <summary>
/// EN: Verifies that the benchmark mock sessions can initialize and run representative benchmark features for each provider.
/// PT-br: Verifica se as sessoes mock de benchmark conseguem inicializar e executar recursos representativos de benchmark para cada provedor.
/// </summary>
public sealed class BenchmarkSessionSmokeTests
{
    /// <summary>
    /// EN: Verifies the SQL Server mock benchmark session runs the shared smoke features.
    /// PT-br: Verifica se a sessao mock de benchmark de SQL Server executa os recursos de smoke compartilhados.
    /// </summary>
    [Theory]
    [MemberData(nameof(SqlServerMockCases))]
    public void SqlServerDbSqlLikeMemSession_RunsSmokeFeatures(
        string sessionName,
        Func<BenchmarkSessionBase> factory,
        BenchmarkFeatureId[] features)
        => RunSmokeFeatures(sessionName, factory, features);

    /// <summary>
    /// EN: Verifies the SQLite mock benchmark session runs the shared smoke features.
    /// PT-br: Verifica se a sessao mock de benchmark de SQLite executa os recursos de smoke compartilhados.
    /// </summary>
    [Theory]
    [MemberData(nameof(SqliteMockCases))]
    public void SqliteDbSqlLikeMemSession_RunsSmokeFeatures(
        string sessionName,
        Func<BenchmarkSessionBase> factory,
        BenchmarkFeatureId[] features)
        => RunSmokeFeatures(sessionName, factory, features);

    /// <summary>
    /// EN: Verifies the SQL Azure mock benchmark session runs the shared smoke features.
    /// PT-br: Verifica se a sessao mock de benchmark de SQL Azure executa os recursos de smoke compartilhados.
    /// </summary>
    [Theory]
    [MemberData(nameof(SqlAzureMockCases))]
    public void SqlAzureDbSqlLikeMemSession_RunsSmokeFeatures(
        string sessionName,
        Func<BenchmarkSessionBase> factory,
        BenchmarkFeatureId[] features)
        => RunSmokeFeatures(sessionName, factory, features);

    /// <summary>
    /// EN: Verifies the Oracle mock benchmark session runs the shared smoke features.
    /// PT-br: Verifica se a sessao mock de benchmark de Oracle executa os recursos de smoke compartilhados.
    /// </summary>
    [Theory]
    [MemberData(nameof(OracleMockCases))]
    public void OracleDbSqlLikeMemSession_RunsSmokeFeatures(
        string sessionName,
        Func<BenchmarkSessionBase> factory,
        BenchmarkFeatureId[] features)
        => RunSmokeFeatures(sessionName, factory, features);

    /// <summary>
    /// EN: Verifies the Npgsql mock benchmark session runs the shared smoke features.
    /// PT-br: Verifica se a sessao mock de benchmark de Npgsql executa os recursos de smoke compartilhados.
    /// </summary>
    [Theory]
    [MemberData(nameof(NpgsqlMockCases))]
    public void NpgsqlDbSqlLikeMemSession_RunsSmokeFeatures(
        string sessionName,
        Func<BenchmarkSessionBase> factory,
        BenchmarkFeatureId[] features)
        => RunSmokeFeatures(sessionName, factory, features);

    /// <summary>
    /// EN: Verifies the MySql mock benchmark session runs the shared smoke features.
    /// PT-br: Verifica se a sessao mock de benchmark de MySql executa os recursos de smoke compartilhados.
    /// </summary>
    [Theory]
    [MemberData(nameof(MySqlMockCases))]
    public void MySqlDbSqlLikeMemSession_RunsSmokeFeatures(
        string sessionName,
        Func<BenchmarkSessionBase> factory,
        BenchmarkFeatureId[] features)
        => RunSmokeFeatures(sessionName, factory, features);

    /// <summary>
    /// EN: Verifies the MariaDb mock benchmark session runs the shared smoke features.
    /// PT-br: Verifica se a sessao mock de benchmark de MariaDb executa os recursos de smoke compartilhados.
    /// </summary>
    [Theory]
    [MemberData(nameof(MariaDbMockCases))]
    public void MariaDbDbSqlLikeMemSession_RunsSmokeFeatures(
        string sessionName,
        Func<BenchmarkSessionBase> factory,
        BenchmarkFeatureId[] features)
        => RunSmokeFeatures(sessionName, factory, features);

    /// <summary>
    /// EN: Verifies the Db2 mock benchmark session runs the shared smoke features.
    /// PT-br: Verifica se a sessao mock de benchmark de Db2 executa os recursos de smoke compartilhados.
    /// </summary>
    [Theory]
    [MemberData(nameof(Db2MockCases))]
    public void Db2DbSqlLikeMemSession_RunsSmokeFeatures(
        string sessionName,
        Func<BenchmarkSessionBase> factory,
        BenchmarkFeatureId[] features)
        => RunSmokeFeatures(sessionName, factory, features);

    /// <summary>
    /// EN: Verifies the Firebird mock benchmark session runs the shared smoke features and its Firebird-specific block flow.
    /// PT-br: Verifica se a sessao mock de benchmark de Firebird executa os recursos de smoke compartilhados e o fluxo especifico de bloco Firebird.
    /// </summary>
    [Theory]
    [MemberData(nameof(FirebirdMockCases))]
    public void FirebirdDbSqlLikeMemSession_RunsSmokeFeatures(
        string sessionName,
        Func<BenchmarkSessionBase> factory,
        BenchmarkFeatureId[] features)
        => RunSmokeFeatures(sessionName, factory, features);

    /// <summary>
    /// EN: Verifies the Firebird mock benchmark session handles its SQLSTATE-specific EXECUTE BLOCK flow.
    /// PT-br: Verifica se a sessao mock de benchmark de Firebird trata seu fluxo EXECUTE BLOCK especifico de SQLSTATE.
    /// </summary>
    [Fact]
    public void FirebirdDbSqlLikeMemSession_ExecuteBlockSqlState23000_ShouldNotThrow()
    {
        using var session = new FirebirdDbSqlLikeMemSession();

        session.Initialize();
        session.Execute(BenchmarkFeatureId.ExecuteBlockSqlState23000);
    }

    public static IEnumerable<object[]> SqlServerMockCases()
        => [SmokeCase("SqlServer", () => new SqlServerDbSqlLikeMemSession(), SmokeFeatures(
            BenchmarkFeatureId.ConnectionOpen,
            BenchmarkFeatureId.PagedNameProjection,
            BenchmarkFeatureId.SelectLeftJoinAntiJoin,
            BenchmarkFeatureId.StoredProcedureCall))];

    public static IEnumerable<object[]> SqliteMockCases()
        => [SmokeCase("Sqlite", () => new SqliteDbSqlLikeMemSession(), SmokeFeatures(
            BenchmarkFeatureId.ConnectionOpen,
            BenchmarkFeatureId.PagedNameProjection,
            BenchmarkFeatureId.SelectLeftJoinAntiJoin,
            BenchmarkFeatureId.StoredProcedureCall))];

    public static IEnumerable<object[]> SqlAzureMockCases()
        => [SmokeCase("SqlAzure", () => new SqlAzureDbSqlLikeMemSession(), SmokeFeatures(
            BenchmarkFeatureId.ConnectionOpen,
            BenchmarkFeatureId.PagedNameProjection,
            BenchmarkFeatureId.SelectLeftJoinAntiJoin,
            BenchmarkFeatureId.StoredProcedureCall))];

    public static IEnumerable<object[]> OracleMockCases()
        => [SmokeCase("Oracle", () => new OracleDbSqlLikeMemSession(), SmokeFeatures(
            BenchmarkFeatureId.ConnectionOpen,
            BenchmarkFeatureId.PagedNameProjection,
            BenchmarkFeatureId.SelectLeftJoinAntiJoin,
            BenchmarkFeatureId.StoredProcedureCall))];

    public static IEnumerable<object[]> NpgsqlMockCases()
        => [SmokeCase("Npgsql", () => new NpgsqlDbSqlLikeMemSession(), SmokeFeatures(
            BenchmarkFeatureId.ConnectionOpen,
            BenchmarkFeatureId.PagedNameProjection,
            BenchmarkFeatureId.SelectLeftJoinAntiJoin,
            BenchmarkFeatureId.StoredProcedureCall))];

    public static IEnumerable<object[]> MySqlMockCases()
        => [SmokeCase("MySql", () => new MySqlDbSqlLikeMemSession(), SmokeFeatures(
            BenchmarkFeatureId.ConnectionOpen,
            BenchmarkFeatureId.PagedNameProjection,
            BenchmarkFeatureId.SelectLeftJoinAntiJoin,
            BenchmarkFeatureId.StoredProcedureCall))];

    public static IEnumerable<object[]> MariaDbMockCases()
        => [SmokeCase("MariaDb", () => new MariaDbDbSqlLikeMemSession(), SmokeFeatures(
            BenchmarkFeatureId.ConnectionOpen,
            BenchmarkFeatureId.PagedNameProjection,
            BenchmarkFeatureId.SelectLeftJoinAntiJoin,
            BenchmarkFeatureId.StoredProcedureCall))];

    public static IEnumerable<object[]> Db2MockCases()
        => [SmokeCase("Db2", () => new Db2DbSqlLikeMemSession(), SmokeFeatures(
            BenchmarkFeatureId.ConnectionOpen,
            BenchmarkFeatureId.PagedNameProjection,
            BenchmarkFeatureId.SelectLeftJoinAntiJoin,
            BenchmarkFeatureId.StoredProcedureCall))];

    public static IEnumerable<object[]> FirebirdMockCases()
        => [SmokeCase("Firebird", () => new FirebirdDbSqlLikeMemSession(), SmokeFeatures(
            BenchmarkFeatureId.ConnectionOpen,
            BenchmarkFeatureId.PagedNameProjection,
            BenchmarkFeatureId.SelectLeftJoinAntiJoin,
            BenchmarkFeatureId.StoredProcedureCall))];

    private static object[] SmokeCase(
        string sessionName,
        Func<BenchmarkSessionBase> factory,
        BenchmarkFeatureId[] features)
        => [sessionName, factory, features];

    private static BenchmarkFeatureId[] SmokeFeatures(params BenchmarkFeatureId[] features)
        => features;

    private static void RunSmokeFeatures(
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
