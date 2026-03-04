namespace DbSqlLikeMem.SqlServer;

/// <inheritdoc/>
[AttributeUsage(AttributeTargets.Method, AllowMultiple = false)]
public sealed class MemberDataBySqlServerVersionAttribute(
    string dataMemberName
 ) : MemberDataByVersionAttribute(dataMemberName)
{
    /// <inheritdoc/>
    protected override IEnumerable<int> Versions => SqlServerDbVersions.Versions();
}