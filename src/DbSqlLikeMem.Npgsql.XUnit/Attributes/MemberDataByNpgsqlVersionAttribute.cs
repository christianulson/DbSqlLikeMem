namespace DbSqlLikeMem.Npgsql;

/// <inheritdoc/>
[AttributeUsage(AttributeTargets.Method, AllowMultiple = false)]
public sealed class MemberDataByNpgsqlVersionAttribute(
    string dataMemberName
 ) : MemberDataByVersionAttribute(dataMemberName)
{
    /// <inheritdoc/>
    protected override IEnumerable<int> Versions => NpgsqlDbVersions.Versions();
}