namespace DbSqlLikeMem.MySql;

#pragma warning disable CA1010 // Generic interface should also be implemented
public class MySqlDataReaderMock(
#pragma warning restore CA1010 // Generic interface should also be implemented
    IList<TableResultMock> tables
    ) : DbDataReaderMockBase(tables)
{

}