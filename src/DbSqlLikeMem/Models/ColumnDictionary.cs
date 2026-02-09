namespace DbSqlLikeMem;
public class ColumnDictionary
    : Dictionary<string, ColumnDef>,
        IColumnDictionary
{
    public ColumnDictionary() : base(StringComparer.OrdinalIgnoreCase) { }
}
