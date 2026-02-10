namespace DbSqlLikeMem.Oracle;

/// <summary>
/// Auto-generated summary.
/// </summary>
[AttributeUsage(AttributeTargets.Method, AllowMultiple = false)]
public sealed class MemberDataOracleVersionAttribute
    : MemberDataVersionAttribute
{
    /// <inheritdoc/>
    protected override IEnumerable<int> Versions => OracleDbVersions.Versions();
}