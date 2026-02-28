namespace DbSqlLikeMem.Oracle;

/// <inheritdoc/>
[AttributeUsage(AttributeTargets.Method, AllowMultiple = false)]
public sealed class MemberDataByOracleVersionAttribute(
    string dataMemberName
 ) : MemberDataByVersionAttribute(dataMemberName)
{
    /// <inheritdoc/>
    protected override IEnumerable<int> Versions => OracleDbVersions.Versions();
}