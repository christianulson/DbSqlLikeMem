using System.Runtime.CompilerServices;

internal static class TestBootstrap
{
    [ModuleInitializer]
    internal static void Init()
    {
        Db2AstQueryExecutorRegister.Register();
    }
}