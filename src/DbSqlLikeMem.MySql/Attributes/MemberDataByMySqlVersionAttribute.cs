
namespace DbSqlLikeMem.MySql;

/// <inheritdoc/>
[AttributeUsage(AttributeTargets.Method, AllowMultiple = false)]
public sealed class MemberDataByMySqlVersionAttribute(
    string dataMemberName
) : MemberDataByVersionAttribute(dataMemberName)
{
    /// <inheritdoc/>
    protected override IEnumerable<int> Versions => MySqlDbVersions.Versions();
}
