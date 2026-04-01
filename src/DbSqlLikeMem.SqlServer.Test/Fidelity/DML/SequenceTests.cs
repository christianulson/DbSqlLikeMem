using DbSqlLikeMem.SqlServer.TestTools;
using DbSqlLikeMem.TestTools.Tests.DML;

namespace DbSqlLikeMem.SqlServer.Test.Fidelity.DML;

/// <summary>
/// EN: Runs SQL Server fidelity tests for the shared sequence workflows.
/// PT: Executa testes de fidelidade SQL Server para os fluxos compartilhados de sequence.
/// </summary>
public class SequenceTests(
    ITestOutputHelper helper
    ) : SequenceTestsBase<SqlServerConnectionMock, SqlConnection>(
    helper,
    new SqlServerProviderSqlDialect(),
    () => new SqlServerConnectionMock(),
    s => new SqlConnection(s)
    )
{
    /// <summary>
    /// EN: Indicates that SQL Server does not support reading the current sequence value in this benchmark.
    /// PT: Indica que o SQL Server nao suporta leitura do valor corrente da sequence neste benchmark.
    /// </summary>
    protected override bool SupportsCurrentSequenceValue => false;
}
