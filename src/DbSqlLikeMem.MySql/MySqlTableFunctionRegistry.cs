namespace DbSqlLikeMem.MySql;

internal partial class MySqlDialect
{
    partial void RegisterTableFunctions(int version)
    {
        if (version < MySqlDialect.JsonArrowOperatorsMinVersion)
            return;

        SqlSharedTableFunctionRegistry.RegisterJsonTable(this);
    }
}
