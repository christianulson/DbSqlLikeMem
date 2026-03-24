using DbSqlLikeMem;

namespace DbSqlLikeMem.MySql;

internal partial class MySqlDialect
{
    partial void RegisterTableFunctions(int version)
    {
        if (version < MySqlDialect.JsonExtractMinVersion)
            return;

        SqlSharedTableFunctionRegistry.RegisterJsonTable(this);
    }
}
