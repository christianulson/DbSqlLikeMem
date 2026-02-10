using System.Reflection;
using Xunit;
using Xunit.Sdk;


namespace DbSqlLikeMem;

/// <inheritdoc/>
public abstract class MemberDataVersionAttribute
#if NET8_0_OR_GREATER
    : Xunit.v3.DataAttribute
#else
    : DataAttribute
#endif
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
    /// DbVersions to be used in the test when SpecificVersions is null.
    /// </summary>
    protected abstract IEnumerable<int> Versions { get; }

    /// <inheritdoc/>
#if NET8_0_OR_GREATER
    public override bool SupportsDiscoveryEnumeration() => true;

    /// <inheritdoc/>
    public override ValueTask<IReadOnlyCollection<ITheoryDataRow>> GetData(MethodInfo testMethod, DisposalTracker disposalTracker)
#else
    public override IEnumerable<object[]> GetData(MethodInfo testMethod)
#endif
    { 
        var versions = SpecificVersions ?? Versions;

        if (VersionGraterOrEqual != null)
            versions = versions.Where(_ => _ >= VersionGraterOrEqual);
        if (VersionLessOrEqual != null)
            versions = versions.Where(_ => _ <= VersionLessOrEqual);

        versions = [.. versions];

        if (!versions.Any())
            throw new Exception("Nenhuma versão de MySql selecionada para o teste.");

#if NET8_0_OR_GREATER
        return new ValueTask<IReadOnlyCollection<ITheoryDataRow>>(
            Task.FromResult((IReadOnlyCollection<ITheoryDataRow>)versions
                .Select(ver => new TheoryDataRow(ver))
                .ToList()
                .AsReadOnly()));
#else
        foreach (var ver in versions)
            yield return [ver];
#endif
    }
}
