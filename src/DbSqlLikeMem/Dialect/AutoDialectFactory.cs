namespace DbSqlLikeMem;

internal static class AutoDialectFactory
{
    internal const string DialectName = "auto";
    private const string AutoDialectTypeName = "DbSqlLikeMem.Auto.AutoSqlDialect, DbSqlLikeMem.Auto";
    private static readonly Func<int, ISqlDialect> _factory = CreateFactory();

    internal static ISqlDialect Create(int version = 1)
        => _factory(version);

    private static Func<int, ISqlDialect> CreateFactory()
    {
        var autoDialectType = Type.GetType(AutoDialectTypeName, throwOnError: false);
        if (autoDialectType is not null)
        {
            return version =>
            {
                var created = Activator.CreateInstance(
                    autoDialectType,
                    System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.NonPublic,
                    binder: null,
                    args: [version],
                    culture: null);

                if (created is ISqlDialect dialect)
                    return dialect;

                throw new InvalidOperationException("AutoSqlDialect could not be created from the DbSqlLikeMem.Auto assembly.");
            };
        }

        return version => new AutoSqlDialect(version);
    }

    internal static bool IsAutoDialect(ISqlDialect dialect)
        => dialect.Name.Equals(DialectName, StringComparison.OrdinalIgnoreCase);
}
