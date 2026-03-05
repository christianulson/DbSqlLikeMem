using System.Collections.Concurrent;
using System.Reflection;

namespace DbSqlLikeMem;

/// <summary>
/// EN: Creates provider database mocks and their matching <see cref="IDbConnection"/> instances with optional table mapping setup.
/// PT: Cria mocks de banco por provedor e suas conexões <see cref="IDbConnection"/> correspondentes, com configuração opcional de mapeamento de tabelas.
/// </summary>
public static class DbMockConnectionFactory
{
    private static readonly ConcurrentDictionary<string, ProviderResolutionPlan> ProviderPlans =
        new(StringComparer.OrdinalIgnoreCase);

    private static readonly IReadOnlyDictionary<string, string> ProviderHintAliases =
        new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["oracle"] = "Oracle",
            ["sqlserver"] = "SqlServer",
            ["sqlazure"] = "SqlAzure",
            ["azuresql"] = "SqlAzure",
            ["mysql"] = "MySql",
            ["sqlite"] = "Sqlite",
            ["db2"] = "Db2",
            ["npgsql"] = "Npgsql",
            ["postgres"] = "Npgsql",
            ["postgresql"] = "Npgsql"
        };

    public static (DbMock Db, IDbConnection Connection) CreateOracleWithTables(params Action<DbMock>[] tableMappers)
        => CreateWithTables("Oracle", tableMappers);

    public static (DbMock Db, IDbConnection Connection) CreateSqlServerWithTables(params Action<DbMock>[] tableMappers)
        => CreateWithTables("SqlServer", tableMappers);

    public static (DbMock Db, IDbConnection Connection) CreateSqlAzureWithTables(params Action<DbMock>[] tableMappers)
        => CreateWithTables("SqlAzure", tableMappers);

    public static (DbMock Db, IDbConnection Connection) CreateMySqlWithTables(params Action<DbMock>[] tableMappers)
        => CreateWithTables("MySql", tableMappers);

    public static (DbMock Db, IDbConnection Connection) CreateSqliteWithTables(params Action<DbMock>[] tableMappers)
        => CreateWithTables("Sqlite", tableMappers);

    public static (DbMock Db, IDbConnection Connection) CreateDb2WithTables(params Action<DbMock>[] tableMappers)
        => CreateWithTables("Db2", tableMappers);

    public static (DbMock Db, IDbConnection Connection) CreateNpgsqlWithTables(params Action<DbMock>[] tableMappers)
        => CreateWithTables("Npgsql", tableMappers);

    /// <summary>
    /// EN: Creates a provider-specific <see cref="DbMock"/> and resolves an <see cref="IDbConnection"/> for it.
    /// PT: Cria um <see cref="DbMock"/> específico do provedor e resolve um <see cref="IDbConnection"/> para ele.
    /// </summary>
    /// <param name="providerHint">EN: Provider name hint like Oracle, SqlServer, SqlAzure (also sqlazure/azure-sql), MySql, Sqlite, Db2 or Npgsql. PT: Indicação do provedor como Oracle, SqlServer, SqlAzure (também sqlazure/azure-sql), MySql, Sqlite, Db2 ou Npgsql.</param>
    /// <param name="tableMappers">EN: Optional actions to configure tables/schemas on the created mock. PT: Ações opcionais para configurar tabelas/esquemas no mock criado.</param>
    public static (DbMock Db, IDbConnection Connection) CreateWithTables(
        string providerHint,
        params Action<DbMock>[] tableMappers)
    {
        var canonicalProviderHint = CanonicalizeProviderHint(providerHint);
        var plan = ProviderPlans.GetOrAdd(canonicalProviderHint, BuildProviderResolutionPlan);
        var db = plan.CreateDbMock();

        foreach (var map in tableMappers)
        {
            map(db);
        }

        var connection = plan.ResolveConnection(db);
        return (db, connection);
    }

    private static ProviderResolutionPlan BuildProviderResolutionPlan(string providerHint)
    {
        EnsureProviderAssembliesLoaded(providerHint);

        var allDbMockTypes = AppDomain.CurrentDomain
            .GetAssemblies()
            .SelectMany(SafeGetTypes)
            .Where(static type =>
                typeof(DbMock).IsAssignableFrom(type)
                && !type.IsAbstract)
            .ToArray();

        var preferred = allDbMockTypes
            .FirstOrDefault(type => ContainsProviderHint(type, providerHint))
            ?? allDbMockTypes.FirstOrDefault();

        if (preferred is null)
        {
            throw new InvalidOperationException(
                $"No concrete DbMock implementation was found. Loaded assemblies: {AppDomain.CurrentDomain.GetAssemblies().Length}.");
        }

        var dbFactory = CreateDbMockFactory(preferred);
        var connectionResolver = CreateConnectionResolver(preferred, providerHint);
        return new ProviderResolutionPlan(dbFactory, connectionResolver);
    }


    private static void EnsureProviderAssembliesLoaded(string providerHint)
    {
        var candidates = new[]
        {
            "DbSqlLikeMem.Sqlite",
            "DbSqlLikeMem.MySql",
            "DbSqlLikeMem.SqlServer",
            "DbSqlLikeMem.SqlAzure",
            "DbSqlLikeMem.Oracle",
            "DbSqlLikeMem.Db2",
            "DbSqlLikeMem.Npgsql"
        };

        foreach (var assemblyName in candidates)
        {
            if (!assemblyName.Contains(providerHint, StringComparison.OrdinalIgnoreCase)
                && !providerHint.Contains(assemblyName.Split('.').Last(), StringComparison.OrdinalIgnoreCase))
                continue;

            try
            {
                _ = Assembly.Load(assemblyName);
            }
            catch
            {
                // Best effort: continue discovery with assemblies already loaded.
            }
        }
    }


    private static Func<DbMock> CreateDbMockFactory(Type dbType)
    {
        var parameterlessCtor = dbType.GetConstructor(Type.EmptyTypes);
        if (parameterlessCtor is not null)
        {
            return () => (DbMock)parameterlessCtor.Invoke(null);
        }

        var optionalCtor = dbType
            .GetConstructors(BindingFlags.Instance | BindingFlags.Public)
            .OrderBy(ctor => ctor.GetParameters().Length)
            .FirstOrDefault(ctor => ctor.GetParameters().All(p => p.IsOptional));

        if (optionalCtor is null)
            throw new MissingMethodException($"No compatible constructor was found for '{dbType.FullName}'.");

        var optionalCtorArgs = optionalCtor
            .GetParameters()
            .Select(_ => Type.Missing)
            .ToArray();

        return () => (DbMock)optionalCtor!.Invoke(optionalCtorArgs!);
    }

    private static IEnumerable<Type> SafeGetTypes(Assembly assembly)
    {
        try
        {
            return assembly.GetTypes();
        }
        catch (ReflectionTypeLoadException ex)
        {
            return ex.Types.Where(type => type is not null)!;
        }
    }

    private static Func<DbMock, IDbConnection> CreateConnectionResolver(Type dbType, string providerHint)
    {
        if (typeof(IDbConnection).IsAssignableFrom(dbType))
        {
            return static db => (IDbConnection)db;
        }

        var candidateMembers = dbType
            .GetMembers(BindingFlags.Instance | BindingFlags.Public)
            .Where(member =>
                member is PropertyInfo property && typeof(IDbConnection).IsAssignableFrom(property.PropertyType)
                || member is MethodInfo method && typeof(IDbConnection).IsAssignableFrom(method.ReturnType) && method.GetParameters().Length == 0);

        var accessors = new List<Func<DbMock, IDbConnection?>>();
        foreach (var member in candidateMembers)
        {
            if (member is PropertyInfo property)
            {
                accessors.Add(db => property.GetValue(db) as IDbConnection);
                continue;
            }

            if (member is MethodInfo method)
            {
                accessors.Add(db => method.Invoke(db, null) as IDbConnection);
            }
        }

        if (accessors.Count > 0)
        {
            return db =>
            {
                foreach (var accessor in accessors)
                {
                    var connection = accessor(db);
                    if (connection is not null)
                        return connection;
                }

                throw new InvalidOperationException(
                    $"Could not resolve an IDbConnection from DbMock type '{dbType.FullName}' with provider hint '{providerHint}'.");
            };
        }

        var connectionType = AppDomain.CurrentDomain
            .GetAssemblies()
            .SelectMany(SafeGetTypes)
            .Where(type =>
                typeof(IDbConnection).IsAssignableFrom(type)
                && !type.IsAbstract
                && ContainsProviderHint(type, providerHint))
            .OrderByDescending(type => type.Name.Contains("Connection", StringComparison.OrdinalIgnoreCase))
            .FirstOrDefault(type => CanInstantiateConnectionForDb(type, dbType));

        if (connectionType is null)
        {
            return _ => throw new InvalidOperationException(
                $"Could not resolve an IDbConnection from DbMock type '{dbType.FullName}' with provider hint '{providerHint}'.");
        }

        var ctor = GetCompatibleConnectionCtor(connectionType, dbType)
            ?? throw new InvalidOperationException($"No compatible connection constructor found for '{connectionType.FullName}'.");
        var ctorParameters = ctor.GetParameters();

        return db =>
        {
            var args = new object?[ctorParameters.Length];
            args[0] = db;
            for (var i = 1; i < ctorParameters.Length; i++)
                args[i] = Type.Missing;
            return (IDbConnection)ctor.Invoke(args);
        };
    }


    private static bool CanInstantiateConnectionForDb(Type connectionType, Type dbType)
        => connectionType
            .GetConstructors(BindingFlags.Instance | BindingFlags.Public)
            .Any(ctor =>
            {
                var ps = ctor.GetParameters();
                if (ps.Length == 0)
                    return false;

                if (!ps[0].ParameterType.IsAssignableFrom(dbType))
                    return false;

                return ps.Skip(1).All(p => p.IsOptional);
            });

    private static ConstructorInfo? GetCompatibleConnectionCtor(Type connectionType, Type dbType)
    {
        return connectionType
            .GetConstructors(BindingFlags.Instance | BindingFlags.Public)
            .OrderBy(c => c.GetParameters().Length)
            .FirstOrDefault(c =>
            {
                var ps = c.GetParameters();
                if (ps.Length == 0)
                    return false;

                if (!ps[0].ParameterType.IsAssignableFrom(dbType))
                    return false;

                return ps.Skip(1).All(p => p.IsOptional);
            });
    }

    private static bool ContainsProviderHint(Type type, string providerHint)
    {
        if (string.IsNullOrWhiteSpace(providerHint))
            return false;

        return type.Name.Contains(providerHint, StringComparison.OrdinalIgnoreCase)
            || (type.Namespace?.Contains(providerHint, StringComparison.OrdinalIgnoreCase) ?? false)
            || (type.Assembly.GetName().Name?.Contains(providerHint, StringComparison.OrdinalIgnoreCase) ?? false);
    }

    private static string CanonicalizeProviderHint(string providerHint)
    {
        var normalized = (providerHint ?? string.Empty)
            .Replace("_", string.Empty)
            .Replace("-", string.Empty)
            .Trim();

        return ProviderHintAliases.TryGetValue(normalized, out var canonical)
            ? canonical
            : (providerHint ?? string.Empty).Trim();
    }

    private sealed record ProviderResolutionPlan(
        Func<DbMock> CreateDbMock,
        Func<DbMock, IDbConnection> ResolveConnection);
}
