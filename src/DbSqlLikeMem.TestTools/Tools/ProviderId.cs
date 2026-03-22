namespace DbSqlLikeMem.TestTools;

/// <summary>
/// EN: Identifies the supported database provider families.
/// PT: Identifica as familias de provedores de banco suportadas.
/// </summary>
public enum ProviderId
{
    /// <summary>
    /// EN: MySQL provider family.
    /// PT: Familia de provedor MySQL.
    /// </summary>
    MySql,
    /// <summary>
    /// EN: MariaDB provider family.
    /// PT: Familia de provedor MariaDB.
    /// </summary>
    MariaDb,
    /// <summary>
    /// EN: SQL Server provider family.
    /// PT: Familia de provedor SQL Server.
    /// </summary>
    SqlServer,
    /// <summary>
    /// EN: Azure SQL provider family.
    /// PT: Familia de provedor Azure SQL.
    /// </summary>
    SqlAzure,
    /// <summary>
    /// EN: Oracle provider family.
    /// PT: Familia de provedor Oracle.
    /// </summary>
    Oracle,
    /// <summary>
    /// EN: PostgreSQL provider family.
    /// PT: Familia de provedor PostgreSQL.
    /// </summary>
    Npgsql,
    /// <summary>
    /// EN: SQLite provider family.
    /// PT: Familia de provedor SQLite.
    /// </summary>
    Sqlite,
    /// <summary>
    /// EN: Db2 provider family.
    /// PT: Familia de provedor Db2.
    /// </summary>
    Db2
}
