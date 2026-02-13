using System.Reflection;

namespace DbSqlLikeMem;

internal static class DapperLateBinding
{
    private const string SqlMapperTypeName = "Dapper.SqlMapper";
    private static readonly string[] CandidateAssemblyNames = ["Dapper", "Dapper.StrongName"];

    internal static MethodInfo FindSqlMapperMethodWithOptionalTail(string name, int genericArgCount)
    {
        var sqlMapper = ResolveSqlMapperType();

        var candidates = sqlMapper
            .GetMethods(BindingFlags.Public | BindingFlags.Static)
            .Where(m => m.Name == name
                && m.IsGenericMethodDefinition
                && m.GetGenericArguments().Length == genericArgCount)
            .Select(m => new { Method = m, Params = m.GetParameters() })
            .Where(x => x.Params.Length >= 3
                && typeof(IDbConnection).IsAssignableFrom(x.Params[0].ParameterType)
                && x.Params[1].ParameterType == typeof(string)
                && (x.Params[2].ParameterType == typeof(object)
                    || x.Params[2].ParameterType.IsAssignableFrom(typeof(object)))
                && x.Params.Skip(3).All(p => p.IsOptional))
            .ToList();

        if (candidates.Count == 0)
            throw new InvalidOperationException(
                $"Não encontrei overload de {SqlMapperTypeName}.{name} com prefixo (IDbConnection, string, object) e cauda opcional.");

        return candidates
            .OrderByDescending(x => x.Params.Length)
            .First()
            .Method;
    }

    internal static object?[] BuildInvokeArgs(
        ParameterInfo[] ps,
        IDbConnection cnn,
        string sql,
        object? paramObj)
    {
        var args = new object?[ps.Length];
        args[0] = cnn;
        args[1] = sql;
        args[2] = paramObj ?? new { };

        for (var i = 3; i < ps.Length; i++)
            args[i] = Type.Missing;

        return args;
    }

    internal static void AddTypeMap(Type type, DbType dbType)
    {
        if (!TryResolveSqlMapperType(out var sqlMapper))
            return;

        var mi = sqlMapper.GetMethod(
            "AddTypeMap",
            BindingFlags.Public | BindingFlags.Static,
            binder: null,
            types: [typeof(Type), typeof(DbType)],
            modifiers: null);

        if (mi is null)
            throw new MissingMethodException($"{SqlMapperTypeName}.AddTypeMap(Type, DbType) não encontrado.");

        mi.Invoke(null, [type, dbType]);
    }

    private static Type ResolveSqlMapperType()
    {
        if (TryResolveSqlMapperType(out var t))
            return t;

        throw new InvalidOperationException(
            "Dapper não foi encontrado em runtime. Adicione referência ao pacote Dapper ou Dapper.StrongName para habilitar execução LINQ via late-binding.");
    }

    private static bool TryResolveSqlMapperType(out Type? t)
    {
        foreach (var assemblyName in CandidateAssemblyNames)
        {
            t = Type.GetType($"{SqlMapperTypeName}, {assemblyName}", throwOnError: false);
            if (t is not null)
                return true;
        }

        t = AppDomain.CurrentDomain
            .GetAssemblies()
            .Select(a => a.GetType(SqlMapperTypeName, throwOnError: false))
            .FirstOrDefault(x => x is not null);

        return t is not null;
    }
}
