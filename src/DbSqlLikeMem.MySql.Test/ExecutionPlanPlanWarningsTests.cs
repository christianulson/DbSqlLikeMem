using System.Data.Common;

namespace DbSqlLikeMem.MySql.Test;

public sealed class ExecutionPlanPlanWarningsTests(ITestOutputHelper helper)
    : ExecutionPlanPlanWarningsTestsBase(helper)
{
    protected override DbConnectionMockBase CreateConnection() => new MySqlConnectionMock();

    protected override DbCommand CreateCommand(DbConnectionMockBase connection, string commandText)
        => new MySqlCommandMock((MySqlConnectionMock)connection) { CommandText = commandText };

    protected override string SelectOrderByWithLimitSql => "SELECT Id FROM users ORDER BY Id LIMIT 10";
}
