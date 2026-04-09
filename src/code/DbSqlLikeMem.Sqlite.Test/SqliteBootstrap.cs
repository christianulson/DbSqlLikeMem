using SQLitePCL;

namespace DbSqlLikeMem.Sqlite.Test;

internal static class SqliteBootstrap
{
    internal static void Initialize()
    {
        Batteries_V2.Init();
    }
}
