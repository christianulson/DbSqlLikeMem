namespace DbSqlLikeMem.Oracle;

/// <summary>
/// EN: Provides Oracle version data for xUnit member-data sources.
/// PT: Fornece dados de versao Oracle para fontes de member-data do xUnit.
/// </summary>
[AttributeUsage(AttributeTargets.Method, AllowMultiple = false)]
public sealed class MemberDataOracleVersionAttribute
    : MemberDataVersionAttribute
{
    /// <inheritdoc/>
    protected override IEnumerable<int> Versions => OracleDbVersions.Versions();
}
