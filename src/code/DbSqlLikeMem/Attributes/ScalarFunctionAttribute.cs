namespace DbSqlLikeMem;

[AttributeUsage(AttributeTargets.Method, AllowMultiple = true, Inherited = false)]
internal sealed class ScalarFunctionAttribute : Attribute
{
    public ScalarFunctionAttribute(string name, string returnTypeSql)
    {
        Name = name;
        ReturnTypeSql = returnTypeSql;
    }

    public string Name { get; }

    public string ReturnTypeSql { get; }

    public DbInvocationStyle InvocationStyle { get; init; } = DbInvocationStyle.Call;

    public int TemporalKind { get; init; } = -1;

    public int MinVersion { get; init; }
}

