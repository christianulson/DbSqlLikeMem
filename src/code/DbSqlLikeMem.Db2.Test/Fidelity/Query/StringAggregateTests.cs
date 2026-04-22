using DbSqlLikeMem.Db2.TestTools;
using DbSqlLikeMem.TestTools.Tests.Query;
#if NET462
using DB2Connection = IBM.Data.DB2.Core.DB2Connection;
#endif

namespace DbSqlLikeMem.Db2.Test.Fidelity.Query;

/// <summary>
/// EN: Runs Db2 fidelity tests for the shared string-aggregation workflows.
/// PT: Executa testes de fidelidade Db2 para os fluxos compartilhados de agregacao de strings.
/// </summary>
public class StringAggregateTests(
    ITestOutputHelper helper
    ) : StringAggregateTestsBase<Db2ConnectionMock, DB2Connection>(
    helper,
    new Db2ProviderSqlDialect(),
    () => new Db2ConnectionMock(),
    Db2ConnectionFactory.Create
    )
{
    /// <inheritdoc />
    protected override string[] NormalizeSnapshotColumnNames(string[] columnNames)
        => Array.ConvertAll(columnNames, static name => name.ToUpperInvariant());
}
