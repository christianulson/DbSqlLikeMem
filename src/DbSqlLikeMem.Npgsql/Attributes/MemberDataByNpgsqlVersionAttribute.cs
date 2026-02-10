using System.Collections;
using System.Reflection;
using Xunit;
using Xunit.Sdk;

namespace DbSqlLikeMem.Npgsql;

[AttributeUsage(AttributeTargets.Method, AllowMultiple = false)]
public sealed class MemberDataByNpgsqlVersionAttribute(
    string dataMemberName,
    int[]? specificVersions = null,
    int? versionGraterOrEqual = null,
    int? versionLessOrEqual = null
 ) : DataAttribute
{
    public override IEnumerable<object[]> GetData(MethodInfo testMethod)
    {
        var declaringType = testMethod.DeclaringType
            ?? throw new XunitException("DeclaringType do método de teste é null.");

        var versions = specificVersions ?? NpgsqlDbVersions.Versions();

        if (versionGraterOrEqual != null)
            versions = versions.Where(_ => _ >= versionGraterOrEqual);
        if (versionLessOrEqual != null)
            versions = versions.Where(_ => _ <= versionLessOrEqual);

        versions = [.. versions];

        Assert.NotEmpty(versions);


        var data = GetMemberData(declaringType, dataMemberName);

        foreach (var row in data)
            foreach (var ver in versions)
                yield return [.. row, ver];
    }

    private static IEnumerable<object[]> GetMemberData(Type type, string memberName)
    {
        var flags = BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static;

        var member =
            (MemberInfo?)type.GetMethod(memberName, flags)
            ?? (MemberInfo?)type.GetProperty(memberName, flags)
            ?? (MemberInfo?)type.GetField(memberName, flags);

        if (member is null)
            throw new XunitException(
                $"Member '{memberName}' não encontrado em '{type.FullName}'.");

        object? value = member switch
        {
            MethodInfo m => m.Invoke(null, Array.Empty<object>()),
            PropertyInfo p => p.GetValue(null),
            FieldInfo f => f.GetValue(null),
            _ => null
        };

        if (value is not IEnumerable enumerable)
            throw new XunitException(
                $"Member '{memberName}' em '{type.FullName}' não retorna IEnumerable.");

        foreach (var item in enumerable)
        {
            if (item is object[] arr)
                yield return arr;
            else
                throw new XunitException(
                    $"Item em '{memberName}' não é object[].");
        }
    }
}
