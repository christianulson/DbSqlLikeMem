namespace DbSqlLikeMem;

public class IndexDef
{
    public string Name { get; }
    public IReadOnlyList<string> KeyCols { get; }
    public bool Unique { get; }

    public IReadOnlyList<string> Include { get; }

    public IndexDef(
        string name,
        IEnumerable<string> keyCols,
        string[]? include = null,
        bool unique = false)
    {
        Name = name ?? throw new ArgumentNullException(nameof(name));
        KeyCols = [.. keyCols ?? throw new ArgumentNullException(nameof(keyCols))];
        if (KeyCols.Count == 0) throw new ArgumentException("At least one key column is required", nameof(keyCols));
        Unique = unique;
        Include = include ?? [];
    }
}