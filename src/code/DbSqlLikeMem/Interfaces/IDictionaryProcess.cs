namespace DbSqlLikeMem.Interfaces;

internal interface IDictionaryProcess<T>
    : IDictionary<string, T>
    where T : ProcessDef
{
}
