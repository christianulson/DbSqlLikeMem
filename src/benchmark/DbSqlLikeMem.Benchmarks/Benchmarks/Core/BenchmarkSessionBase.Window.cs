namespace DbSqlLikeMem.Benchmarks.Core;

public abstract partial class BenchmarkSessionBase
{
    [BenchmarkFeature(BenchmarkFeatureId.WindowRowNumber)]
    protected virtual void RunWindowRowNumber()
    {
        var state = GetPreparedUsersQueryState(
            "WindowRowNumber",
            (1, "Bob"), (2, "Alice"), (3, "Charlie"));
        var value = state.Service.RunWindowRowNumberAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    [BenchmarkFeature(BenchmarkFeatureId.WindowLag)]
    protected virtual void RunWindowLag()
    {
        var state = GetPreparedUsersQueryState(
            "WindowLag",
            (1, "Bob"), (2, "Alice"), (3, "Charlie"));
        var value = state.Service.RunWindowLagAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    [BenchmarkFeature(BenchmarkFeatureId.WindowLead)]
    protected virtual void RunWindowLead()
    {
        var state = GetPreparedUsersQueryState(
            "WindowLead",
            (1, "Bob"), (2, "Alice"), (3, "Charlie"));
        var value = state.Service.RunWindowLeadAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    [BenchmarkFeature(BenchmarkFeatureId.WindowRankDenseRank)]
    protected virtual void RunWindowRankDenseRank()
    {
        var state = GetPreparedUsersQueryState(
            "WindowRankDenseRank",
            (1, "Aaron"), (2, "Bravo"), (3, "Bravo"), (4, "Charlie"));
        var value = state.Service.RunWindowRankDenseRank("Aaron");
        GC.KeepAlive(value);
    }

    [BenchmarkFeature(BenchmarkFeatureId.WindowFirstLastValue)]
    protected virtual void RunWindowFirstLastValue()
    {
        var state = GetPreparedUsersQueryState(
            "WindowFirstLastValue",
            (1, "Aaron"), (2, "Bravo"), (3, "Bravo"), (4, "Charlie"));
        var value = state.Service.RunWindowFirstLastValue("Aaron");
        GC.KeepAlive(value);
    }

    [BenchmarkFeature(BenchmarkFeatureId.WindowNtile)]
    protected virtual void RunWindowNtile()
    {
        var state = GetPreparedUsersQueryState(
            "WindowNtile",
            (1, "Aaron"), (2, "Bravo"), (3, "Bravo"), (4, "Charlie"));
        var value = state.Service.RunWindowNtile("Aaron");
        GC.KeepAlive(value);
    }

    [BenchmarkFeature(BenchmarkFeatureId.WindowPercentRankCumeDist)]
    protected virtual void RunWindowPercentRankCumeDist()
    {
        var state = GetPreparedUsersQueryState(
            "WindowPercentRankCumeDist",
            (1, "Aaron"), (2, "Bravo"), (3, "Bravo"), (4, "Charlie"));
        var value = state.Service.RunWindowPercentRankCumeDist("Aaron");
        GC.KeepAlive(value);
    }

    [BenchmarkFeature(BenchmarkFeatureId.WindowNthValue)]
    protected virtual void RunWindowNthValue()
    {
        var state = GetPreparedUsersQueryState(
            "WindowNthValue",
            (1, "Aaron"), (2, "Bravo"), (3, "Bravo"), (4, "Charlie"));
        var value = state.Service.RunWindowNthValue("Aaron");
        GC.KeepAlive(value);
    }
}
