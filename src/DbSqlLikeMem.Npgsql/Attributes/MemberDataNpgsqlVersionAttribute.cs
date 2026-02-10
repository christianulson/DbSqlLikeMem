using System.Reflection;
using Xunit;
using Xunit.Sdk;

namespace DbSqlLikeMem.Npgsql;

[AttributeUsage(AttributeTargets.Method, AllowMultiple = false)]
public sealed class MemberDataNpgsqlVersionAttribute(
    int[]? specificVersions = null,
    int? versionGraterOrEqual = null,
    int? versionLessOrEqual = null
) : DataAttribute
{
    public override IEnumerable<object[]> GetData(MethodInfo testMethod)
    {
        var versions = specificVersions ?? NpgsqlDbVersions.Versions();

        if (versionGraterOrEqual != null)
            versions = versions.Where(_ => _ >= versionGraterOrEqual);
        if (versionLessOrEqual != null)
            versions = versions.Where(_ => _ <= versionLessOrEqual);

        versions = [.. versions];

        Assert.NotEmpty(versions);

        foreach (var ver in versions)
            yield return [ver];
    }
}
