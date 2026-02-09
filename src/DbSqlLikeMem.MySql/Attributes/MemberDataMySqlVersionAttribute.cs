using System.Reflection;
using Xunit.Sdk;

namespace DbSqlLikeMem.MySql;

[AttributeUsage(AttributeTargets.Method, AllowMultiple = false)]
public sealed class MemberDataMySqlVersionAttribute : DataAttribute
{

    public MemberDataMySqlVersionAttribute()
    {

    }

    public override IEnumerable<object[]> GetData(MethodInfo testMethod)
    {
        var versions = MySqlDbVersions.Versions();

        foreach (var ver in versions)
            yield return [ver];
    }
}
