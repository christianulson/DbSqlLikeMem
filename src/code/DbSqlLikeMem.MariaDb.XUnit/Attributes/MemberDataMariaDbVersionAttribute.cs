namespace DbSqlLikeMem.MariaDb;

/// <inheritdoc/>
[AttributeUsage(AttributeTargets.Method, AllowMultiple = false)]
public sealed class MemberDataMariaDbVersionAttribute
    : MemberDataVersionAttribute
{
    /// <inheritdoc/>
    protected override IEnumerable<int> Versions => MariaDbDbVersions.Versions();
}
