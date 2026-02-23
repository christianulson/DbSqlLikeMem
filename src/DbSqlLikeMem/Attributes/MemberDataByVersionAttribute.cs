using Xunit;
using Xunit.Sdk;

namespace DbSqlLikeMem;

/// <inheritdoc/>
public abstract class MemberDataByVersionAttribute(
    string dataMemberName
)
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
    public int? VersionGraterOrEqual { get; set; }
    /// <summary>
    /// EN: Gets or sets VersionLessOrEqual.
    /// PT: Obtém ou define VersionLessOrEqual.
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
        var declaringType = testMethod.DeclaringType
#if NET8_0_OR_GREATER
            ?? throw new UnsetPropertyException(dataMemberName, typeof(object));
#else
            ?? throw new XunitException("DeclaringType do método de teste é null.");
#endif


        var versions = SpecificVersions ?? Versions;

        if (VersionGraterOrEqual != null)
            versions = versions.Where(_ => _ >= VersionGraterOrEqual);
        if (VersionLessOrEqual != null)
            versions = versions.Where(_ => _ <= VersionLessOrEqual);

        versions = [.. versions];

        if (!versions.Any())
            throw new Exception("Nenhuma versão de MySql selecionada para o teste.");

        var data = GetMemberData(declaringType, dataMemberName);

#if NET8_0_OR_GREATER
        return new ValueTask<IReadOnlyCollection<ITheoryDataRow>>(
            Task.FromResult((IReadOnlyCollection<ITheoryDataRow>)data
            .SelectMany(row => versions.Select(ver => new TheoryDataRow([.. row, ver])))
            .ToList()
            .AsReadOnly()));
#else
        foreach (var row in data)
            foreach (var ver in versions)
                yield return [.. row, ver];
#endif
    }

    private static IEnumerable<object[]> GetMemberData(Type type, string memberName)
    {
        var flags = BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static;

        var member =
            (MemberInfo?)type.GetMethod(memberName, flags)
            ?? (MemberInfo?)type.GetProperty(memberName, flags)
            ?? (MemberInfo?)type.GetField(memberName, flags);

        if (member is null)
#if NET8_0_OR_GREATER
            throw new Exception($"Member '{memberName}' não encontrado em '{type.FullName}'.");
#else
            throw new XunitException($"Member '{memberName}' não encontrado em '{type.FullName}'.");
#endif

        object? value = member switch
        {
            MethodInfo m => m.Invoke(null, []),
            PropertyInfo p => p.GetValue(null),
            FieldInfo f => f.GetValue(null),
            _ => null
        };

        if (value is not IEnumerable enumerable)
#if NET8_0_OR_GREATER
            throw new Exception($"Member '{memberName}' em '{type.FullName}' não retorna IEnumerable.");
#else
            throw new XunitException($"Member '{memberName}' em '{type.FullName}' não retorna IEnumerable.");
#endif

        foreach (var item in enumerable!)
        {
            if (item is object[] arr)
                yield return arr;
            else
            {
#if NET8_0_OR_GREATER
                throw new Exception($"Item em '{memberName}' não é object[].");
#else
                throw new XunitException($"Item em '{memberName}' não é object[].");
#endif
            }
        }
    }
}
