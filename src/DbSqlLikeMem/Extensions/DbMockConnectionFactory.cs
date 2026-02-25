using System.Reflection;

namespace DbSqlLikeMem;

/// <summary>
/// EN: Creates provider database mocks and their matching <see cref="IDbConnection"/> instances with optional table mapping setup.
/// PT: Cria mocks de banco por provedor e suas conexões <see cref="IDbConnection"/> correspondentes, com configuração opcional de mapeamento de tabelas.
/// </summary>
public static class DbMockConnectionFactory
{
    public static (DbMock Db, IDbConnection Connection) CreateOracleWithTables(params Action<DbMock>[] tableMappers)
        => CreateWithTables("Oracle", tableMappers);

    public static (DbMock Db, IDbConnection Connection) CreateSqlServerWithTables(params Action<DbMock>[] tableMappers)
        => CreateWithTables("SqlServer", tableMappers);

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
    /// <param name="providerHint">EN: Provider name hint like Oracle, SqlServer, MySql, Sqlite, Db2 or Npgsql. PT: Indicação do provedor como Oracle, SqlServer, MySql, Sqlite, Db2 ou Npgsql.</param>
    /// <param name="tableMappers">EN: Optional actions to configure tables/schemas on the created mock. PT: Ações opcionais para configurar tabelas/esquemas no mock criado.</param>
    public static (DbMock Db, IDbConnection Connection) CreateWithTables(
        string providerHint,
        params Action<DbMock>[] tableMappers)
    {
        var db = CreateDbMock(providerHint);

        foreach (var map in tableMappers)
        {
            map(db);
        }

        var connection = ResolveConnection(db, providerHint);
        return (db, connection);
    }

    private static DbMock CreateDbMock(string providerHint)
    {
        EnsureProviderAssembliesLoaded(providerHint);

        var allTypes = AppDomain.CurrentDomain
            .GetAssemblies()
            .SelectMany(SafeGetTypes)
            .Where(type =>
                typeof(DbMock).IsAssignableFrom(type)
                && !type.IsAbstract
                && type.GetConstructor(Type.EmptyTypes) is not null)
            .ToArray();

        var preferred = allTypes
            .FirstOrDefault(type => ContainsProviderHint(type, providerHint))
            ?? allTypes.FirstOrDefault();

        if (preferred is null)
        {
            throw new InvalidOperationException(
                $"No concrete DbMock implementation was found. Loaded assemblies: {AppDomain.CurrentDomain.GetAssemblies().Length}.");
        }

        return (DbMock)Activator.CreateInstance(preferred)!;
    }


    private static void EnsureProviderAssembliesLoaded(string providerHint)
    {
        var candidates = new[]
        {
            "DbSqlLikeMem.Sqlite",
            "DbSqlLikeMem.MySql",
            "DbSqlLikeMem.SqlServer",
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

    private static IDbConnection ResolveConnection(DbMock db, string providerHint)
    {
        if (db is IDbConnection directConnection)
        {
            return directConnection;
        }

        var candidateMembers = db.GetType()
            .GetMembers(BindingFlags.Instance | BindingFlags.Public)
            .Where(member =>
                member is PropertyInfo property && typeof(IDbConnection).IsAssignableFrom(property.PropertyType)
                || member is MethodInfo method && typeof(IDbConnection).IsAssignableFrom(method.ReturnType) && method.GetParameters().Length == 0);

        foreach (var member in candidateMembers)
        {
            object? value = member switch
            {
                PropertyInfo property => property.GetValue(db),
                MethodInfo method => method.Invoke(db, null),
                _ => null
            };

            if (value is IDbConnection connection)
            {
                return connection;
            }
        }

        var connectionType = AppDomain.CurrentDomain
            .GetAssemblies()
            .SelectMany(SafeGetTypes)
            .Where(type =>
                typeof(IDbConnection).IsAssignableFrom(type)
                && !type.IsAbstract
                && ContainsProviderHint(type, providerHint))
            .OrderByDescending(type => type.Name.Contains("Connection", StringComparison.OrdinalIgnoreCase))
            .FirstOrDefault(type =>
                type.GetConstructor([db.GetType()]) is not null
                || type.GetConstructor([typeof(DbMock)]) is not null);

        if (connectionType is not null)
        {
            var byExactCtor = connectionType.GetConstructor([db.GetType()]);
            var byBaseCtor = connectionType.GetConstructor([typeof(DbMock)]);
            if (byExactCtor is not null)
            {
                return (IDbConnection)byExactCtor.Invoke([db]);
            }

            if (byBaseCtor is not null)
            {
                return (IDbConnection)byBaseCtor.Invoke([db]);
            }
        }

        throw new InvalidOperationException(
            $"Could not resolve an IDbConnection from DbMock type '{db.GetType().FullName}' with provider hint '{providerHint}'.");
    }

    private static bool ContainsProviderHint(Type type, string providerHint)
    {
        if (string.IsNullOrWhiteSpace(providerHint))
            return false;

        return type.Name.Contains(providerHint, StringComparison.OrdinalIgnoreCase)
            || (type.Namespace?.Contains(providerHint, StringComparison.OrdinalIgnoreCase) ?? false)
            || (type.Assembly.GetName().Name?.Contains(providerHint, StringComparison.OrdinalIgnoreCase) ?? false);
    }
}
