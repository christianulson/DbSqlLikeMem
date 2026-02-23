using System.Data.Common;

namespace DbSqlLikeMem.SqlServer.Test;

public sealed class ExecutionPlanPlanWarningsTests(ITestOutputHelper helper)
    : ExecutionPlanPlanWarningsTestsBase(helper)
{
    protected override DbConnectionMockBase CreateConnection() => new SqlServerConnectionMock();

    protected override DbCommand CreateCommand(DbConnectionMockBase connection, string commandText)
        => new SqlServerCommandMock((SqlServerConnectionMock)connection) { CommandText = commandText };

    protected override string SelectOrderByWithLimitSql => "SELECT TOP 10 Id FROM users ORDER BY Id";
}
