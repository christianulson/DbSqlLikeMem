namespace DbSqlLikeMem.MySql;

internal partial class MySqlDialect
{
    partial void RegisterTableFunctions(int version)
    {
        if (!string.Equals(Name, MySqlDialect.DialectName, StringComparison.OrdinalIgnoreCase))
            return;

        if (version < MySqlDialect.JsonArrowOperatorsMinVersion)
            return;

        SqlSharedTableFunctionRegistry.RegisterJsonTable(this);
    }
}
