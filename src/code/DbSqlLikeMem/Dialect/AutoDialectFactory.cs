namespace DbSqlLikeMem;

internal static class AutoDialectFactory
{
    internal const string DialectName = "auto";
    private const string AutoDialectTypeName = "DbSqlLikeMem.Auto.AutoSqlDialect, DbSqlLikeMem.Auto";
    private const string AutoSyntaxDetectorTypeName = "DbSqlLikeMem.Auto.AutoSqlSyntaxDetector, DbSqlLikeMem.Auto";
    private static readonly Func<int, ISqlDialect> _factory = CreateFactory();
    private static readonly IAutoSqlSyntaxDetector _syntaxDetector = CreateSyntaxDetector();

    internal static ISqlDialect Create(int version = 1)
        => _factory(version);

    internal static AutoSqlSyntaxFeatures DetectSyntax(string? sql, IReadOnlyList<SqlToken> tokens)
        => _syntaxDetector.Detect(sql, tokens);

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

    private static IAutoSqlSyntaxDetector CreateSyntaxDetector()
    {
        var autoSyntaxDetectorType = Type.GetType(AutoSyntaxDetectorTypeName, throwOnError: false);
        if (autoSyntaxDetectorType is not null)
        {
            var created = Activator.CreateInstance(
                autoSyntaxDetectorType,
                System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.NonPublic,
                binder: null,
                args: [],
                culture: null);

            if (created is IAutoSqlSyntaxDetector detector)
                return detector;
        }

        return new DefaultAutoSqlSyntaxDetector();
    }

    internal static bool IsAutoDialect(ISqlDialect dialect)
        => dialect.Name.Equals(DialectName, StringComparison.OrdinalIgnoreCase);
}

internal interface IAutoSqlSyntaxDetector
{
    AutoSqlSyntaxFeatures Detect(string? sql, IReadOnlyList<SqlToken> tokens);
}

internal sealed class DefaultAutoSqlSyntaxDetector : IAutoSqlSyntaxDetector
{
    public AutoSqlSyntaxFeatures Detect(string? sql, IReadOnlyList<SqlToken> tokens)
        => SqlSyntaxDetector.Detect(sql, tokens);
}
