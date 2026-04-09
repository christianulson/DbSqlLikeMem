namespace DbSqlLikeMem.Firebird;

/// <inheritdoc/>
[AttributeUsage(AttributeTargets.Method, AllowMultiple = false)]
public sealed class MemberDataByFirebirdVersionAttribute(
    string dataMemberName
) : MemberDataByVersionAttribute(dataMemberName)
{
    /// <inheritdoc/>
    protected override IEnumerable<int> Versions => FirebirdDbVersions.Versions();
}
