namespace DbSqlLikeMem;
public class IndexDictionary : Dictionary<string, IndexDef>
{
    public IndexDictionary() : base(StringComparer.OrdinalIgnoreCase)
    {
    }

    public IEnumerable<IndexDef> GetUnique()
        => Values.Where(i => i.Unique);
}
