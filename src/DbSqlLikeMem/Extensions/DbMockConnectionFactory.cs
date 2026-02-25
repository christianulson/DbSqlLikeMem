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
                && !type.IsAbstract)
            .ToArray();

        var preferred = allTypes
            .FirstOrDefault(type => ContainsProviderHint(type, providerHint))
            ?? allTypes.FirstOrDefault();

        if (preferred is null)
        {
            throw new InvalidOperationException(
                $"No concrete DbMock implementation was found. Loaded assemblies: {AppDomain.CurrentDomain.GetAssemblies().Length}.");
        }

        return (DbMock)CreateInstanceAllowingOptionalCtor(preferred)!;
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


    private static object CreateInstanceAllowingOptionalCtor(Type type)
    {
        try
        {
            return Activator.CreateInstance(type)!;
        }
        catch (MissingMethodException)
        {
            var optionalCtor = type
                .GetConstructors(BindingFlags.Instance | BindingFlags.Public)
                .OrderBy(ctor => ctor.GetParameters().Length)
                .FirstOrDefault(ctor => ctor.GetParameters().All(p => p.IsOptional));

            if (optionalCtor is null)
                throw;

            var args = optionalCtor
                .GetParameters()
                .Select(_ => Type.Missing)
                .ToArray();

            return optionalCtor.Invoke(args);
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
            .FirstOrDefault(type => CanInstantiateConnectionForDb(type, db.GetType()));

        if (connectionType is not null)
            return (IDbConnection)CreateConnectionInstanceAllowingOptionalCtor(connectionType, db);

        throw new InvalidOperationException(
            $"Could not resolve an IDbConnection from DbMock type '{db.GetType().FullName}' with provider hint '{providerHint}'.");
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

    private static object CreateConnectionInstanceAllowingOptionalCtor(Type connectionType, DbMock db)
    {
        var ctor = connectionType
            .GetConstructors(BindingFlags.Instance | BindingFlags.Public)
            .OrderBy(c => c.GetParameters().Length)
            .FirstOrDefault(c =>
            {
                var ps = c.GetParameters();
                if (ps.Length == 0)
                    return false;

                if (!ps[0].ParameterType.IsAssignableFrom(db.GetType()))
                    return false;

                return ps.Skip(1).All(p => p.IsOptional);
            });

        if (ctor is null)
            throw new InvalidOperationException($"No compatible connection constructor found for '{connectionType.FullName}'.");

        var psCtor = ctor.GetParameters();
        var args = new object?[psCtor.Length];
        args[0] = db;
        for (var i = 1; i < psCtor.Length; i++)
            args[i] = Type.Missing;

        return ctor.Invoke(args);
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
