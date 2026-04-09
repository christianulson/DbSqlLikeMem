namespace DbSqlLikeMem.SqlAzure;

/// <inheritdoc/>
[AttributeUsage(AttributeTargets.Method, AllowMultiple = false)]
public sealed class MemberDataBySqlAzureCompatibilityLevelAttribute(
    string dataMemberName
) : MemberDataByVersionAttribute(dataMemberName)
{
    /// <inheritdoc/>
    protected override IEnumerable<int> Versions => SqlAzureDbCompatibilityLevels.Versions();
}
