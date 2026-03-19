using System.Collections.Concurrent;

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
            ["ora"] = "Oracle",
            ["sqlserver"] = "SqlServer",
            ["mssql"] = "SqlServer",
            ["sqlsrv"] = "SqlServer",
            ["sqlazure"] = "SqlAzure",
            ["azuresql"] = "SqlAzure",
            ["azuresqldb"] = "SqlAzure",
            ["azuresqlserver"] = "SqlAzure",
            ["mysql"] = "MySql",
            ["mariadb"] = "MariaDb",
            ["sqlite"] = "Sqlite",
            ["sqlite3"] = "Sqlite",
            ["db2"] = "Db2",
            ["ibmdb2"] = "Db2",
            ["npgsql"] = "Npgsql",
            ["pg"] = "Npgsql",
            ["postgres"] = "Npgsql",
            ["postgresql"] = "Npgsql"
        };

    public static (DbMock Db, IDbConnection Connection) CreateOracleWithTables(params Action<DbMock>[] tableMappers)
        => CreateWithTables("Oracle", tableMappers);

    /// <summary>
    /// EN: Creates an Oracle mock and wraps its connection with the interception pipeline.
    /// PT: Cria um mock Oracle e encapsula sua conexao com o pipeline de interceptacao.
    /// </summary>
    public static (DbMock Db, DbConnection Connection) CreateOracleWithTablesIntercepted(
        params DbConnectionInterceptor[] interceptors)
        => CreateWithTablesIntercepted("Oracle", interceptors);

    /// <summary>
    /// EN: Creates an Oracle mock and wraps its connection using interception options.
    /// PT: Cria um mock Oracle e encapsula sua conexao usando opcoes de interceptacao.
    /// </summary>
    public static (DbMock Db, DbConnection Connection) CreateOracleWithTablesIntercepted(
        DbInterceptionOptions options,
        params Action<DbMock>[] tableMappers)
        => CreateWithTablesIntercepted("Oracle", options, tableMappers);

    public static (DbMock Db, IDbConnection Connection) CreateSqlServerWithTables(params Action<DbMock>[] tableMappers)
        => CreateWithTables("SqlServer", tableMappers);

    /// <summary>
    /// EN: Creates a SQL Server mock and wraps its connection with the interception pipeline.
    /// PT: Cria um mock SQL Server e encapsula sua conexao com o pipeline de interceptacao.
    /// </summary>
    public static (DbMock Db, DbConnection Connection) CreateSqlServerWithTablesIntercepted(
        params DbConnectionInterceptor[] interceptors)
        => CreateWithTablesIntercepted("SqlServer", interceptors);

    /// <summary>
    /// EN: Creates a SQL Server mock and wraps its connection using interception options.
    /// PT: Cria um mock SQL Server e encapsula sua conexao usando opcoes de interceptacao.
    /// </summary>
    public static (DbMock Db, DbConnection Connection) CreateSqlServerWithTablesIntercepted(
        DbInterceptionOptions options,
        params Action<DbMock>[] tableMappers)
        => CreateWithTablesIntercepted("SqlServer", options, tableMappers);

    public static (DbMock Db, IDbConnection Connection) CreateSqlAzureWithTables(params Action<DbMock>[] tableMappers)
        => CreateWithTables("SqlAzure", tableMappers);

    /// <summary>
    /// EN: Creates a SQL Azure mock and wraps its connection with the interception pipeline.
    /// PT: Cria um mock SQL Azure e encapsula sua conexao com o pipeline de interceptacao.
    /// </summary>
    public static (DbMock Db, DbConnection Connection) CreateSqlAzureWithTablesIntercepted(
        params DbConnectionInterceptor[] interceptors)
        => CreateWithTablesIntercepted("SqlAzure", interceptors);

    /// <summary>
    /// EN: Creates a SQL Azure mock and wraps its connection using interception options.
    /// PT: Cria um mock SQL Azure e encapsula sua conexao usando opcoes de interceptacao.
    /// </summary>
    public static (DbMock Db, DbConnection Connection) CreateSqlAzureWithTablesIntercepted(
        DbInterceptionOptions options,
        params Action<DbMock>[] tableMappers)
        => CreateWithTablesIntercepted("SqlAzure", options, tableMappers);

    public static (DbMock Db, IDbConnection Connection) CreateMySqlWithTables(params Action<DbMock>[] tableMappers)
        => CreateWithTables("MySql", tableMappers);

    /// <summary>
    /// EN: Creates a MariaDB mock and resolves its matching connection.
    /// PT: Cria um mock MariaDB e resolve sua conexao correspondente.
    /// </summary>
    public static (DbMock Db, IDbConnection Connection) CreateMariaDbWithTables(params Action<DbMock>[] tableMappers)
        => CreateWithTables("MariaDb", tableMappers);

    /// <summary>
    /// EN: Creates a MySQL mock and wraps its connection with the interception pipeline.
    /// PT: Cria um mock MySQL e encapsula sua conexao com o pipeline de interceptacao.
    /// </summary>
    public static (DbMock Db, DbConnection Connection) CreateMySqlWithTablesIntercepted(
        params DbConnectionInterceptor[] interceptors)
        => CreateWithTablesIntercepted("MySql", interceptors);

    /// <summary>
    /// EN: Creates a MariaDB mock and wraps its connection with the interception pipeline.
    /// PT: Cria um mock MariaDB e encapsula sua conexao com o pipeline de interceptacao.
    /// </summary>
    public static (DbMock Db, DbConnection Connection) CreateMariaDbWithTablesIntercepted(
        params DbConnectionInterceptor[] interceptors)
        => CreateWithTablesIntercepted("MariaDb", interceptors);

    /// <summary>
    /// EN: Creates a MySQL mock and wraps its connection using interception options.
    /// PT: Cria um mock MySQL e encapsula sua conexao usando opcoes de interceptacao.
    /// </summary>
    public static (DbMock Db, DbConnection Connection) CreateMySqlWithTablesIntercepted(
        DbInterceptionOptions options,
        params Action<DbMock>[] tableMappers)
        => CreateWithTablesIntercepted("MySql", options, tableMappers);

    /// <summary>
    /// EN: Creates a MariaDB mock and wraps its connection using interception options.
    /// PT: Cria um mock MariaDB e encapsula sua conexao usando opcoes de interceptacao.
    /// </summary>
    public static (DbMock Db, DbConnection Connection) CreateMariaDbWithTablesIntercepted(
        DbInterceptionOptions options,
        params Action<DbMock>[] tableMappers)
        => CreateWithTablesIntercepted("MariaDb", options, tableMappers);

    public static (DbMock Db, IDbConnection Connection) CreateSqliteWithTables(params Action<DbMock>[] tableMappers)
        => CreateWithTables("Sqlite", tableMappers);

    /// <summary>
    /// EN: Creates a SQLite mock and wraps its connection with the interception pipeline.
    /// PT: Cria um mock SQLite e encapsula sua conexao com o pipeline de interceptacao.
    /// </summary>
    public static (DbMock Db, DbConnection Connection) CreateSqliteWithTablesIntercepted(
        params DbConnectionInterceptor[] interceptors)
        => CreateWithTablesIntercepted("Sqlite", interceptors);

    /// <summary>
    /// EN: Creates a SQLite mock and wraps its connection using interception options.
    /// PT: Cria um mock SQLite e encapsula sua conexao usando opcoes de interceptacao.
    /// </summary>
    public static (DbMock Db, DbConnection Connection) CreateSqliteWithTablesIntercepted(
        DbInterceptionOptions options,
        params Action<DbMock>[] tableMappers)
        => CreateWithTablesIntercepted("Sqlite", options, tableMappers);

    public static (DbMock Db, IDbConnection Connection) CreateDb2WithTables(params Action<DbMock>[] tableMappers)
        => CreateWithTables("Db2", tableMappers);

    /// <summary>
    /// EN: Creates a DB2 mock and wraps its connection with the interception pipeline.
    /// PT: Cria um mock DB2 e encapsula sua conexao com o pipeline de interceptacao.
    /// </summary>
    public static (DbMock Db, DbConnection Connection) CreateDb2WithTablesIntercepted(
        params DbConnectionInterceptor[] interceptors)
        => CreateWithTablesIntercepted("Db2", interceptors);

    /// <summary>
    /// EN: Creates a DB2 mock and wraps its connection using interception options.
    /// PT: Cria um mock DB2 e encapsula sua conexao usando opcoes de interceptacao.
    /// </summary>
    public static (DbMock Db, DbConnection Connection) CreateDb2WithTablesIntercepted(
        DbInterceptionOptions options,
        params Action<DbMock>[] tableMappers)
        => CreateWithTablesIntercepted("Db2", options, tableMappers);

    public static (DbMock Db, IDbConnection Connection) CreateNpgsqlWithTables(params Action<DbMock>[] tableMappers)
        => CreateWithTables("Npgsql", tableMappers);

    /// <summary>
    /// EN: Creates an Npgsql mock and wraps its connection with the interception pipeline.
    /// PT: Cria um mock Npgsql e encapsula sua conexao com o pipeline de interceptacao.
    /// </summary>
    public static (DbMock Db, DbConnection Connection) CreateNpgsqlWithTablesIntercepted(
        params DbConnectionInterceptor[] interceptors)
        => CreateWithTablesIntercepted("Npgsql", interceptors);

    /// <summary>
    /// EN: Creates an Npgsql mock and wraps its connection using interception options.
    /// PT: Cria um mock Npgsql e encapsula sua conexao usando opcoes de interceptacao.
    /// </summary>
    public static (DbMock Db, DbConnection Connection) CreateNpgsqlWithTablesIntercepted(
        DbInterceptionOptions options,
        params Action<DbMock>[] tableMappers)
        => CreateWithTablesIntercepted("Npgsql", options, tableMappers);

    /// <summary>
    /// EN: Creates a provider-specific <see cref="DbMock"/> and resolves an <see cref="IDbConnection"/> for it.
    /// PT: Cria um <see cref="DbMock"/> específico do provedor e resolve um <see cref="IDbConnection"/> para ele.
    /// </summary>
    /// <param name="providerHint">EN: Provider name hint like Oracle, SqlServer, SqlAzure, MySql, MariaDb, Sqlite, Db2 or Npgsql. PT: Indicação do provedor como Oracle, SqlServer, SqlAzure, MySql, MariaDb, Sqlite, Db2 ou Npgsql.</param>
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
        if (!IsConnectionTypeCompatibleForProvider(connection.GetType(), canonicalProviderHint))
        {
            ProviderPlans.TryRemove(canonicalProviderHint, out _);
            plan = BuildProviderResolutionPlan(canonicalProviderHint);
            ProviderPlans[canonicalProviderHint] = plan;
            connection = plan.ResolveConnection(db);

            if (!IsConnectionTypeCompatibleForProvider(connection.GetType(), canonicalProviderHint))
            {
                throw new InvalidOperationException(
                    SqlExceptionMessages.ResolvedConnectionTypeNotCompatible(
                        connection.GetType().FullName ?? connection.GetType().Name,
                        canonicalProviderHint));
            }
        }
        return (db, connection);
    }

    /// <summary>
    /// EN: Creates a provider-specific mock and returns its connection already wrapped by the interception pipeline.
    /// PT: Cria um mock especifico do provedor e retorna sua conexao ja encapsulada pelo pipeline de interceptacao.
    /// </summary>
    /// <param name="providerHint">EN: Provider name hint. PT: Indicacao do provedor.</param>
    /// <param name="interceptors">EN: Interceptors applied in registration order. PT: Interceptors aplicados na ordem de registro.</param>
    /// <param name="tableMappers">EN: Optional actions to configure tables/schemas on the created mock. PT: Acoes opcionais para configurar tabelas/esquemas no mock criado.</param>
    /// <returns>EN: Provider mock and wrapped connection. PT: Mock do provedor e conexao encapsulada.</returns>
    public static (DbMock Db, DbConnection Connection) CreateWithTablesIntercepted(
        string providerHint,
        DbConnectionInterceptor[] interceptors,
        params Action<DbMock>[] tableMappers)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(interceptors, nameof(interceptors));
        var created = CreateWithTables(providerHint, tableMappers);
        if (created.Connection is not DbConnection dbConnection)
            throw new InvalidOperationException("The resolved provider connection does not derive from DbConnection.");

        return (created.Db, DbInterceptionPipeline.Wrap(dbConnection, interceptors));
    }

    /// <summary>
    /// EN: Creates a provider-specific mock and returns its connection wrapped with interceptors built from the supplied options.
    /// PT: Cria um mock especifico do provedor e retorna sua conexao encapsulada com interceptors construidos a partir das opcoes informadas.
    /// </summary>
    /// <param name="providerHint">EN: Provider name hint. PT: Indicacao do provedor.</param>
    /// <param name="options">EN: Interception options. PT: Opcoes de interceptacao.</param>
    /// <param name="tableMappers">EN: Optional actions to configure tables/schemas on the created mock. PT: Acoes opcionais para configurar tabelas/esquemas no mock criado.</param>
    /// <returns>EN: Provider mock and wrapped connection. PT: Mock do provedor e conexao encapsulada.</returns>
    public static (DbMock Db, DbConnection Connection) CreateWithTablesIntercepted(
        string providerHint,
        DbInterceptionOptions options,
        params Action<DbMock>[] tableMappers)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(options, nameof(options));
        return CreateWithTablesIntercepted(providerHint, options.BuildInterceptors(), tableMappers);
    }

    private static ProviderResolutionPlan BuildProviderResolutionPlan(string providerHint)
    {
        EnsureProviderAssembliesLoaded(providerHint);
        if (TryBuildKnownProviderResolutionPlan(providerHint, out var knownPlan))
            return knownPlan;

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
                SqlExceptionMessages.NoConcreteDbMockImplementationFound(AppDomain.CurrentDomain.GetAssemblies().Length));
        }

        var dbFactory = CreateDbMockFactory(preferred);
        var connectionResolver = CreateConnectionResolver(preferred, providerHint);
        return new ProviderResolutionPlan(dbFactory, connectionResolver);
    }

    private static bool TryBuildKnownProviderResolutionPlan(
        string providerHint,
        out ProviderResolutionPlan plan)
    {
        plan = default!;
        var key = providerHint.Trim();
        if (key.Length == 0)
            return false;

        var known = key.ToUpperInvariant() switch
        {
            "SQLSERVER" => ("DbSqlLikeMem.SqlServer.SqlServerDbMock, DbSqlLikeMem.SqlServer", "DbSqlLikeMem.SqlServer.SqlServerConnectionMock, DbSqlLikeMem.SqlServer"),
            "SQLAZURE" => ("DbSqlLikeMem.SqlAzure.SqlAzureDbMock, DbSqlLikeMem.SqlAzure", "DbSqlLikeMem.SqlAzure.SqlAzureConnectionMock, DbSqlLikeMem.SqlAzure"),
            "MYSQL" => ("DbSqlLikeMem.MySql.MySqlDbMock, DbSqlLikeMem.MySql", "DbSqlLikeMem.MySql.MySqlConnectionMock, DbSqlLikeMem.MySql"),
            "MARIADB" => ("DbSqlLikeMem.MariaDb.MariaDbDbMock, DbSqlLikeMem.MariaDb", "DbSqlLikeMem.MariaDb.MariaDbConnectionMock, DbSqlLikeMem.MariaDb"),
            "SQLITE" => ("DbSqlLikeMem.Sqlite.SqliteDbMock, DbSqlLikeMem.Sqlite", "DbSqlLikeMem.Sqlite.SqliteConnectionMock, DbSqlLikeMem.Sqlite"),
            "DB2" => ("DbSqlLikeMem.Db2.Db2DbMock, DbSqlLikeMem.Db2", "DbSqlLikeMem.Db2.Db2ConnectionMock, DbSqlLikeMem.Db2"),
            "NPGSQL" => ("DbSqlLikeMem.Npgsql.NpgsqlDbMock, DbSqlLikeMem.Npgsql", "DbSqlLikeMem.Npgsql.NpgsqlConnectionMock, DbSqlLikeMem.Npgsql"),
            "ORACLE" => ("DbSqlLikeMem.Oracle.OracleDbMock, DbSqlLikeMem.Oracle", "DbSqlLikeMem.Oracle.OracleConnectionMock, DbSqlLikeMem.Oracle"),
            _ => default
        };

        if (string.IsNullOrWhiteSpace(known.Item1) || string.IsNullOrWhiteSpace(known.Item2))
            return false;

        var dbType = Type.GetType(known.Item1, throwOnError: false);
        var connectionType = Type.GetType(known.Item2, throwOnError: false);
        if (dbType is null || connectionType is null)
            return false;

        if (!typeof(DbMock).IsAssignableFrom(dbType) || !typeof(IDbConnection).IsAssignableFrom(connectionType))
            return false;

        var dbFactory = CreateDbMockFactory(dbType);
        var ctor = GetCompatibleConnectionCtor(connectionType, dbType);
        if (ctor is null)
            return false;

        var ctorParameters = ctor.GetParameters();
        Func<DbMock, IDbConnection> resolver = db =>
        {
            var args = new object?[ctorParameters.Length];
            args[0] = db;
            for (var i = 1; i < ctorParameters.Length; i++)
                args[i] = Type.Missing;
            return (IDbConnection)ctor.Invoke(args);
        };

        plan = new ProviderResolutionPlan(dbFactory, resolver);
        return true;
    }


    private static void EnsureProviderAssembliesLoaded(string providerHint)
    {
        var candidates = new[]
        {
            "DbSqlLikeMem.Sqlite",
            "DbSqlLikeMem.MySql",
            "DbSqlLikeMem.MariaDb",
            "DbSqlLikeMem.SqlServer",
            "DbSqlLikeMem.SqlAzure",
            "DbSqlLikeMem.Oracle",
            "DbSqlLikeMem.Db2",
            "DbSqlLikeMem.Npgsql"
        };

        foreach (var assemblyName in candidates)
        {
            if (providerHint.Contains("MariaDb", StringComparison.OrdinalIgnoreCase)
                && (assemblyName.Equals("DbSqlLikeMem.MySql", StringComparison.OrdinalIgnoreCase)
                    || assemblyName.Equals("DbSqlLikeMem.MariaDb", StringComparison.OrdinalIgnoreCase)))
            {
                try
                {
                    _ = Assembly.Load(assemblyName);
                }
                catch
                {
                    // Best effort: continue discovery with assemblies already loaded.
                }

                continue;
            }

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
            throw new MissingMethodException(SqlExceptionMessages.NoCompatibleDbMockConstructorFound(dbType.FullName ?? dbType.Name));

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
                    SqlExceptionMessages.CouldNotResolveConnectionFromDbMock(
                        dbType.FullName ?? dbType.Name,
                        providerHint));
            };
        }

        var connectionType = AppDomain.CurrentDomain
            .GetAssemblies()
            .SelectMany(SafeGetTypes)
            .Where(type =>
                typeof(IDbConnection).IsAssignableFrom(type)
                && !type.IsAbstract
                && type.IsPublic
                && !type.IsNested
                && !IsTestType(type)
                && ContainsProviderHint(type, providerHint))
            .OrderByDescending(type => IsPreferredProviderConnectionType(type, providerHint))
            .ThenByDescending(type => type.Name.Contains("Connection", StringComparison.OrdinalIgnoreCase))
            .FirstOrDefault(type => CanInstantiateConnectionForDb(type, dbType));

        if (connectionType is null)
        {
            return _ => throw new InvalidOperationException(
                SqlExceptionMessages.CouldNotResolveConnectionFromDbMock(
                    dbType.FullName ?? dbType.Name,
                    providerHint));
        }

        var ctor = GetCompatibleConnectionCtor(connectionType, dbType)
            ?? throw new InvalidOperationException(SqlExceptionMessages.NoCompatibleConnectionConstructorFound(connectionType.FullName ?? connectionType.Name));
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

    private static bool IsPreferredProviderConnectionType(Type type, string providerHint)
    {
        if (string.IsNullOrWhiteSpace(providerHint))
            return false;

        var expectedName = $"{providerHint}ConnectionMock";
        return type.Name.Equals(expectedName, StringComparison.OrdinalIgnoreCase);
    }

    private static bool IsTestType(Type type)
    {
        var assemblyName = type.Assembly.GetName().Name ?? string.Empty;
        if (assemblyName.Contains(".Test", StringComparison.OrdinalIgnoreCase))
            return true;

        var namespaceName = type.Namespace ?? string.Empty;
        if (namespaceName.Contains(".Test", StringComparison.OrdinalIgnoreCase))
            return true;

        return false;
    }

    private static bool IsConnectionTypeCompatibleForProvider(Type type, string providerHint)
        => !type.IsAbstract
            && type.IsPublic
            && !type.IsNested
            && !IsTestType(type)
            && ContainsProviderHint(type, providerHint);

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
