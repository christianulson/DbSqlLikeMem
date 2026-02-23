using System.Data.Common;

namespace DbSqlLikeMem.Sqlite.Test;

public sealed class ExecutionPlanPlanWarningsTests(ITestOutputHelper helper)
    : ExecutionPlanPlanWarningsTestsBase(helper)
{
    protected override DbConnectionMockBase CreateConnection() => new SqliteConnectionMock();

    protected override DbCommand CreateCommand(DbConnectionMockBase connection, string commandText)
        => new SqliteCommandMock((SqliteConnectionMock)connection) { CommandText = commandText };

    protected override string SelectOrderByWithLimitSql => "SELECT Id FROM users ORDER BY Id LIMIT 10";
}
