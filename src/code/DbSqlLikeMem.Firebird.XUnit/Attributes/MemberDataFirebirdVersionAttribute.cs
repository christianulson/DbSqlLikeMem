namespace DbSqlLikeMem.Firebird;

/// <inheritdoc/>
[AttributeUsage(AttributeTargets.Method, AllowMultiple = false)]
public sealed class MemberDataFirebirdVersionAttribute
    : MemberDataVersionAttribute
{
    /// <inheritdoc/>
    protected override IEnumerable<int> Versions => FirebirdDbVersions.Versions();
}
