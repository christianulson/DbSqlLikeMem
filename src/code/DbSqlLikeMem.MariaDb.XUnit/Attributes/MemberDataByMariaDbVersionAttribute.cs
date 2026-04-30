namespace DbSqlLikeMem.MariaDb;

/// <inheritdoc/>
[AttributeUsage(AttributeTargets.Method, AllowMultiple = false)]
public sealed class MemberDataByMariaDbVersionAttribute(
    string dataMemberName
) : MemberDataByVersionAttribute(dataMemberName)
{
    /// <inheritdoc/>
    protected override IEnumerable<int> Versions => MariaDbDbVersions.Versions();
}
