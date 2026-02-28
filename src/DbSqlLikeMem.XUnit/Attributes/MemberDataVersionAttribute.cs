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
    /// EN: Gets or sets SpecificVersions.
    /// PT: Obtém ou define SpecificVersions.
    /// </summary>
    public int[]? SpecificVersions { get; set; }
    /// <summary>
    /// EN: Gets or sets VersionGraterOrEqual.
    /// PT: Obtém ou define VersionGraterOrEqual.
    /// </summary>
    public int VersionGraterOrEqual { get; set; } = int.MinValue;
    /// <summary>
    /// EN: Gets or sets VersionLessOrEqual.
    /// PT: Obtém ou define VersionLessOrEqual.
    /// </summary>
    public int VersionLessOrEqual { get; set; } = int.MinValue;
    /// <summary>
    /// EN: Gets or sets VersionLowerThan.
    /// PT: Obtém ou define VersionLowerThan.
    /// </summary>
    public int VersionLowerThan { get; set; } = int.MinValue;

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

        if (VersionGraterOrEqual != int.MinValue)
            versions = versions.Where(_ => _ >= VersionGraterOrEqual);
        if (VersionLessOrEqual != int.MinValue)
            versions = versions.Where(_ => _ <= VersionLessOrEqual);
        if (VersionLowerThan != int.MinValue)
            versions = versions.Where(_ => _ < VersionLowerThan);

        versions = [.. versions];

        if (!versions.Any())
            throw new Exception("Nenhuma verso de MySql selecionada para o teste.");

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
