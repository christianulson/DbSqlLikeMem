using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
namespace DbSqlLikeMem.SqlAzure;

public sealed class SqlAzureDataReaderMock(
    IList<TableResultMock> tables
    ) : DbDataReaderMockBase(tables)
{
}