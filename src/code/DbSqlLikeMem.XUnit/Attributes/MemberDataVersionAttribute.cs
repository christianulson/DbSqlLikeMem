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
    /// PT-br: Obtem ou define SpecificVersions.
    /// </summary>
    public int[]? SpecificVersions { get; set; }
    /// <summary>
    /// EN: Gets or sets VersionGraterOrEqual.
    /// PT-br: Obtem ou define VersionGraterOrEqual.
    /// </summary>
    public int VersionGraterOrEqual { get; set; } = int.MinValue;
    /// <summary>
    /// EN: Gets or sets VersionLessOrEqual.
    /// PT-br: Obtem ou define VersionLessOrEqual.
    /// </summary>
    public int VersionLessOrEqual { get; set; } = int.MinValue;
    /// <summary>
    /// EN: Gets or sets VersionLowerThan.
    /// PT-br: Obtem ou define VersionLowerThan.
    /// </summary>
    public int VersionLowerThan { get; set; } = int.MinValue;

    /// <summary>
    /// EN: Gets the default database versions used when SpecificVersions is null.
    /// PT-br: Obtem as versoes padrao de banco usadas quando SpecificVersions e null.
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
