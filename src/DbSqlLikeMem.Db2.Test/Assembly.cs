using Dapper;
using System.Data;
using System.Runtime.CompilerServices;

internal static class TestBootstrap
{
    [ModuleInitializer]
    internal static void Init()
    {
        Db2AstQueryExecutorRegister.Register();
        SqlMapper.AddTypeMap(typeof(Guid), DbType.String);
    }
}
