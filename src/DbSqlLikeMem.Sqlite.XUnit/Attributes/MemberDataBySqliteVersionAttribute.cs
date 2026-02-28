namespace DbSqlLikeMem.Sqlite;

/// <inheritdoc/>
[AttributeUsage(AttributeTargets.Method, AllowMultiple = false)]
public sealed class MemberDataBySqliteVersionAttribute(
    string dataMemberName
) : MemberDataByVersionAttribute(dataMemberName)
{
    /// <inheritdoc/>
    protected override IEnumerable<int> Versions => SqliteDbVersions.Versions();
}