using DbSqlLikeMem.SqlServer.TestTools;

namespace DbSqlLikeMem.SqlAzure.TestTools;

/// <summary>
/// EN: Provides SQL Azure-specific SQL snippets built on the shared SQL Server shape.
/// PT-br: Fornece trechos SQL especificos de SQL Azure com base na mesma estrutura compartilhada do SQL Server.
/// </summary>
public sealed class SqlAzureProviderSqlDialect : SqlServerProviderSqlDialect
{
    /// <inheritdoc />
    public override ProviderId Provider => ProviderId.SqlAzure;

    /// <inheritdoc />
    public override string DisplayName => "SQL Azure";
}
