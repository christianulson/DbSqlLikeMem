using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
namespace DbSqlLikeMem.SqlAzure;

public static class SqlAzureDbVersions
{
    /// <summary>
    /// EN: Returns all SQL Azure compatibility levels exposed by this alias helper.
    /// PT: Retorna todos os niveis de compatibilidade do SQL Azure expostos por este helper de alias.
    /// </summary>
    public static IEnumerable<int> Versions() => SqlAzureDbCompatibilityLevels.Versions();
}