namespace DbSqlLikeMem.Sqlite;

/// <inheritdoc/>
[AttributeUsage(AttributeTargets.Method, AllowMultiple = false)]
public sealed class MemberDataSqliteVersionAttribute
    : MemberDataVersionAttribute
{
    /// <inheritdoc/>
    protected override IEnumerable<int> Versions => SqliteDbVersions.Versions();
}