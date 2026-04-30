using System.Runtime.CompilerServices;

// Core runtime and test surface
[assembly: InternalsVisibleTo("DbSqlLikeMem.Auto")]
[assembly: InternalsVisibleTo("DbSqlLikeMem.Auto.Test")]
[assembly: InternalsVisibleTo("DbSqlLikeMem.Test")]
[assembly: InternalsVisibleTo("DbSqlLikeMem.TestTools")]

// DB2 provider family
[assembly: InternalsVisibleTo("DbSqlLikeMem.Db2")]
[assembly: InternalsVisibleTo("DbSqlLikeMem.Db2.Test")]
[assembly: InternalsVisibleTo("DbSqlLikeMem.Db2.Dapper.Test")]

// Firebird provider family
[assembly: InternalsVisibleTo("DbSqlLikeMem.Firebird")]
[assembly: InternalsVisibleTo("DbSqlLikeMem.Firebird.Test")]
[assembly: InternalsVisibleTo("DbSqlLikeMem.Firebird.Dapper.Test")]

// MariaDB provider family
[assembly: InternalsVisibleTo("DbSqlLikeMem.MariaDb")]
[assembly: InternalsVisibleTo("DbSqlLikeMem.MariaDb.Test")]
[assembly: InternalsVisibleTo("DbSqlLikeMem.MariaDb.XUnit")]

// MySQL provider family
[assembly: InternalsVisibleTo("DbSqlLikeMem.MySql")]
[assembly: InternalsVisibleTo("DbSqlLikeMem.MySql.Test")]
[assembly: InternalsVisibleTo("DbSqlLikeMem.MySql.Dapper.Test")]
[assembly: InternalsVisibleTo("DbSqlLikeMem.MySql.MiniProfiler")]
[assembly: InternalsVisibleTo("DbSqlLikeMem.MySql.MiniProfiler.Test")]

// PostgreSQL provider family
[assembly: InternalsVisibleTo("DbSqlLikeMem.Npgsql")]
[assembly: InternalsVisibleTo("DbSqlLikeMem.Npgsql.Test")]
[assembly: InternalsVisibleTo("DbSqlLikeMem.Npgsql.Dapper.Test")]

// Oracle provider family
[assembly: InternalsVisibleTo("DbSqlLikeMem.Oracle")]
[assembly: InternalsVisibleTo("DbSqlLikeMem.Oracle.Test")]
[assembly: InternalsVisibleTo("DbSqlLikeMem.Oracle.Dapper.Test")]

// SQLite provider family
[assembly: InternalsVisibleTo("DbSqlLikeMem.Sqlite")]
[assembly: InternalsVisibleTo("DbSqlLikeMem.Sqlite.Test")]
[assembly: InternalsVisibleTo("DbSqlLikeMem.Sqlite.Dapper.Test")]

// SQL Server provider family
[assembly: InternalsVisibleTo("DbSqlLikeMem.SqlServer")]
[assembly: InternalsVisibleTo("DbSqlLikeMem.SqlServer.Test")]
[assembly: InternalsVisibleTo("DbSqlLikeMem.SqlServer.Dapper.Test")]
[assembly: InternalsVisibleTo("DbSqlLikeMem.SqlAzure")]
[assembly: InternalsVisibleTo("DbSqlLikeMem.SqlAzure.Test")]
