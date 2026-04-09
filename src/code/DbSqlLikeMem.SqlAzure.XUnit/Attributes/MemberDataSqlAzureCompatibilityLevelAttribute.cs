namespace DbSqlLikeMem.SqlAzure;

/// <inheritdoc/>
[AttributeUsage(AttributeTargets.Method, AllowMultiple = false)]
public sealed class MemberDataSqlAzureCompatibilityLevelAttribute
    : MemberDataVersionAttribute
{
    /// <inheritdoc/>
    protected override IEnumerable<int> Versions => SqlAzureDbCompatibilityLevels.Versions();
}
