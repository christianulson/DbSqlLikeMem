using System.Reflection;
using Xunit.Sdk;

namespace DbSqlLikeMem.SqlServer;

[AttributeUsage(AttributeTargets.Method, AllowMultiple = false)]
public sealed class MemberDataSqlServerVersionAttribute : DataAttribute
{

    public MemberDataSqlServerVersionAttribute()
    {

    }

    public override IEnumerable<object[]> GetData(MethodInfo testMethod)
    {
        var versions = SqlServerDbVersions.Versions();

        foreach (var ver in versions)
            yield return [ver];
    }
}
