using System.Diagnostics;

namespace DbSqlLikeMem;

public sealed class DbMetrics
{
    public int Selects { get; internal set; }
    public int Inserts { get; internal set; }
    public int Updates { get; internal set; }
    public int Deletes { get; internal set; }
    public TimeSpan Elapsed => _sw.Elapsed;

    private readonly Stopwatch _sw = Stopwatch.StartNew();
}