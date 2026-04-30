namespace DbSqlLikeMem;

internal sealed class TableTriggerManager(TableMock table)
{
    private readonly Dictionary<TableTriggerEvent, List<Action<TableTriggerContext>>> _triggers = [];
    private readonly Dictionary<string, (TableTriggerEvent Event, Action<TableTriggerContext> Handler)> _namedTriggers = new(StringComparer.OrdinalIgnoreCase);

    internal void AddTrigger(TableTriggerEvent evt, Action<TableTriggerContext> handler)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(handler, nameof(handler));
        if (!_triggers.TryGetValue(evt, out var handlers))
        {
            handlers = [];
            _triggers[evt] = handlers;
        }

        handlers.Add(handler);
    }

    internal void AddOrReplaceTrigger(
        string triggerName,
        TableTriggerEvent evt,
        Action<TableTriggerContext> handler,
        bool orReplace = false)
    {
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(triggerName, nameof(triggerName));
        ArgumentNullExceptionCompatible.ThrowIfNull(handler, nameof(handler));

        var normalizedName = triggerName.NormalizeName();
        if (_namedTriggers.TryGetValue(normalizedName, out var previous))
        {
            if (!orReplace)
                throw new InvalidOperationException($"Trigger '{normalizedName}' already exists.");

            if (_triggers.TryGetValue(previous.Event, out var previousHandlers))
            {
                previousHandlers.Remove(previous.Handler);
                if (previousHandlers.Count == 0)
                    _triggers.Remove(previous.Event);
            }
        }

        AddTrigger(evt, handler);
        _namedTriggers[normalizedName] = (evt, handler);
    }

    internal bool RemoveTrigger(string triggerName)
    {
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(triggerName, nameof(triggerName));

        var normalizedName = triggerName.NormalizeName();
        if (!_namedTriggers.TryGetValue(normalizedName, out var previous))
            return false;

        if (_triggers.TryGetValue(previous.Event, out var previousHandlers))
        {
            previousHandlers.Remove(previous.Handler);
            if (previousHandlers.Count == 0)
                _triggers.Remove(previous.Event);
        }

        _namedTriggers.Remove(normalizedName);
        return true;
    }

    internal bool TryGetTriggerEvent(
        string triggerName,
        out TableTriggerEvent evt)
    {
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(triggerName, nameof(triggerName));

        var normalizedName = triggerName.NormalizeName();
        if (_namedTriggers.TryGetValue(normalizedName, out var previous))
        {
            evt = previous.Event;
            return true;
        }

        evt = default;
        return false;
    }

    internal void ExecuteTriggers(
        TableTriggerEvent evt,
        IReadOnlyDictionary<int, object?>? oldRow,
        IReadOnlyDictionary<int, object?>? newRow)
    {
        if (!_triggers.TryGetValue(evt, out var handlers) || handlers.Count == 0)
            return;

        using var scope = DbConnectionMockBase.BeginTriggerScope(evt);
        var context = new TableTriggerContext(table, oldRow, newRow);
        foreach (var handler in handlers)
            handler(context);
    }

    internal bool HasTriggers(TableTriggerEvent evt)
        => _triggers.TryGetValue(evt, out var handlers) && handlers.Count > 0;

    internal bool HasRegisteredTriggers()
        => _triggers.Count > 0;
}
