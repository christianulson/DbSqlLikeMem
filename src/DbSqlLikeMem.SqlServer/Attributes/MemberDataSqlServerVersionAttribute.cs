using System.Reflection;
using Xunit;
using Xunit.Sdk;

namespace DbSqlLikeMem.SqlServer;

[AttributeUsage(AttributeTargets.Method, AllowMultiple = false)]
public sealed class MemberDataSqlServerVersionAttribute
    : DataAttribute
{

    public int[]? SpecificVersions { get; set; }
    public int? VersionGraterOrEqual { get; set; }
    public int? VersionLessOrEqual { get; set; }
    public override IEnumerable<object[]> GetData(MethodInfo testMethod)
    {
        var versions = SpecificVersions ?? SqlServerDbVersions.Versions();

        if (VersionGraterOrEqual != null)
            versions = versions.Where(_ => _ >= VersionGraterOrEqual);
        if (VersionLessOrEqual != null)
            versions = versions.Where(_ => _ <= VersionLessOrEqual);

        versions = [.. versions];

        Assert.NotEmpty(versions);

        foreach (var ver in versions)
            yield return [ver];
    }
}
