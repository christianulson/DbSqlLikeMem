using System.Reflection;
using Xunit.Sdk;

namespace DbSqlLikeMem.Npgsql;

[AttributeUsage(AttributeTargets.Method, AllowMultiple = false)]
public sealed class MemberDataNpgsqlVersionAttribute : DataAttribute
{

    public MemberDataNpgsqlVersionAttribute()
    {

    }

    public override IEnumerable<object[]> GetData(MethodInfo testMethod)
    {
        var versions = NpgsqlDbVersions.Versions();

        foreach (var ver in versions)
            yield return [ver];
    }
}
