namespace DbSqlLikeMem.TestTools.Benchmarks;

internal enum LegacyBenchmarkScenarioKind
{
    None,
    Noop,
    SelectByPk,
    Users,
    UsersOrders,
    InsertUsers,
    Sequence,
    CreateTable,
    CreateTableWithFK,
    DropTable,
    TemporaryTable,
    TemporaryUsers
}

internal interface ILegacyBenchmarkScenario
{
    LegacyBenchmarkScenarioKind Kind { get; }

    (int id, string name)[] SeedUsers { get; }

    (int id, int userId, string note)[] SeedOrders { get; }
}

internal readonly record struct LegacyBenchmarkScenarioData(
    LegacyBenchmarkScenarioKind Kind,
    (int id, string name)[] SeedUsers,
    (int id, int userId, string note)[] SeedOrders)
{
    public static LegacyBenchmarkScenarioData Empty { get; } =
        new(LegacyBenchmarkScenarioKind.None, [], []);
}

internal static class LegacyBenchmarkScenarioExtensions
{
    internal static LegacyBenchmarkScenarioData GetLegacyBenchmarkScenarioData(this ITestScenario scenario)
        => scenario is ILegacyBenchmarkScenario legacy
            ? new LegacyBenchmarkScenarioData(legacy.Kind, legacy.SeedUsers, legacy.SeedOrders)
            : LegacyBenchmarkScenarioData.Empty;
}

internal sealed class LegacyBenchmarkScenario(
    LegacyBenchmarkScenarioKind kind,
    (int id, string name)[]? seedUsers = null,
    (int id, int userId, string note)[]? seedOrders = null)
    : ITestScenario,
        ILegacyBenchmarkScenario
{
    public LegacyBenchmarkScenarioKind Kind { get; } = kind;

    public (int id, string name)[] SeedUsers { get; } = seedUsers ?? [];

    public (int id, int userId, string note)[] SeedOrders { get; } = seedOrders ?? [];

    public Task CreateScenarioAsync()
        => Task.CompletedTask;

    public Task DropScenarioAsync()
        => Task.CompletedTask;
}
