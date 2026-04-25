namespace DbSqlLikeMem.TestTools.Tests.Query;

internal static class SelectTestsBaseSeeds
{
    internal static readonly object?[] InicialData = [(1, "Alice"), (2, "Bob"), (3, "Charlie"), (4, "Delta"), (5, "Echo")];
    internal static readonly object?[] InicialData2 = [(1, "Aaron"), (2, "Alice"), (3, "Bob"), (4, "Charlie"), (5, "Delta")];
    internal static readonly object?[] InicialData3 = [(1, "Charlie"), (2, "Bob"), (3, "Alice"), (4, "Bob"), (5, "Delta")];
    internal static readonly object?[] seedUsers = [(1, "Alice"), (2, "Bob"), (3, "Carla")];

    internal static readonly object?[] seedUsers2 =
        [
            (1, "Alice"),
            (2, "Bob")
        ];

    internal static readonly object?[] seedOrders = [
        (10, 1, "A"),
        (11, 1, "A"),
        (12, 1, "B"),
        (13, 2, "C")
    ];

    internal static readonly object?[] seedOrders2 = [
            (10, 1, "A"),
            (11, 1, "B"),
            (12, 2, "C")
        ];
}
