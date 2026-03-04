namespace DbSqlLikeMem.Oracle;

/// <summary>
/// EN: Defines the class MemberDataOracleVersionAttribute.
/// PT: Define a classe MemberDataOracleVersionAttribute.
/// </summary>
[AttributeUsage(AttributeTargets.Method, AllowMultiple = false)]
public sealed class MemberDataOracleVersionAttribute
    : MemberDataVersionAttribute
{
    /// <inheritdoc/>
    protected override IEnumerable<int> Versions => OracleDbVersions.Versions();
}