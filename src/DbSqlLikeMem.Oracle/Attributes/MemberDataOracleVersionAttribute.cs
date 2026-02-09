using System.Reflection;
using Xunit.Sdk;

namespace DbSqlLikeMem.Oracle;

[AttributeUsage(AttributeTargets.Method, AllowMultiple = false)]
public sealed class MemberDataOracleVersionAttribute : DataAttribute
{

    public MemberDataOracleVersionAttribute()
    {

    }

    public override IEnumerable<object[]> GetData(MethodInfo testMethod)
    {
        var versions = OracleDbVersions.Versions();

        foreach (var ver in versions)
            yield return [ver];
    }
}
