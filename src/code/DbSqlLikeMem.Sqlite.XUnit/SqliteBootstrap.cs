using SQLitePCL;

namespace DbSqlLikeMem.Sqlite;

internal static class SqliteBootstrap
{
    internal static void Initialize()
    {
        Batteries_V2.Init();
    }
}
