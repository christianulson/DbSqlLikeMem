using System.Reflection;
using Xunit;
using Xunit.Sdk;

namespace DbSqlLikeMem.Oracle;

/// <summary>
/// Auto-generated summary.
/// </summary>
[AttributeUsage(AttributeTargets.Method, AllowMultiple = false)]
public sealed class MemberDataOracleVersionAttribute
    : DataAttribute
{

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public int[]? SpecificVersions { get; set; }
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public int? VersionGraterOrEqual { get; set; }
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public int? VersionLessOrEqual { get; set; }

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override IEnumerable<object[]> GetData(MethodInfo testMethod)
    {
        var versions = SpecificVersions ?? OracleDbVersions.Versions();

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
