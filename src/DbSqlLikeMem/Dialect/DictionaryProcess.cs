namespace DbSqlLikeMem.Dialect;

internal class DictionaryProcess<T>()
    : Dictionary<string, T>(StringComparer.OrdinalIgnoreCase),
    IDictionaryProcess<T>
    where T: ProcessDef
{
}
