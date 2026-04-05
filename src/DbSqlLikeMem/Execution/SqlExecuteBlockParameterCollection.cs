using System.Collections;

namespace DbSqlLikeMem;

internal sealed class SqlExecuteBlockParameterCollection : DbParameterCollection
{
    private readonly DbParameterCollection _inner;
    private readonly List<DbParameter> _locals;
    private readonly Dictionary<string, int> _localIndexes = new(StringComparer.OrdinalIgnoreCase);

    private SqlExecuteBlockParameterCollection(
        DbParameterCollection inner,
        IReadOnlyList<DbParameter> locals)
    {
        _inner = inner ?? throw new ArgumentNullException(nameof(inner));
        _locals = [.. locals];

        for (var i = 0; i < _locals.Count; i++)
        {
            var name = NormalizeParameterName(_locals[i].ParameterName);
            if (!string.IsNullOrWhiteSpace(name) && !_localIndexes.ContainsKey(name))
                _localIndexes[name] = i;
        }
    }

    internal static SqlExecuteBlockParameterCollection Create(
        DbParameterCollection inner,
        IReadOnlyList<ProcParam> inputParameters,
        IReadOnlyList<ProcParam>? returnParameters = null)
    {
        var locals = new List<DbParameter>(inputParameters.Count + (returnParameters?.Count ?? 0));

        foreach (var parameter in inputParameters)
            locals.Add(BuildParameter(inner, parameter, ParameterDirection.Input));

        if (returnParameters is not null)
        {
            foreach (var parameter in returnParameters)
                locals.Add(BuildParameter(inner, parameter, ParameterDirection.Output));
        }

        return new SqlExecuteBlockParameterCollection(inner, locals);
    }

    internal bool TrySetLocalParameterValue(string parameterName, object? value)
    {
        var normalized = NormalizeParameterName(parameterName);
        if (!_localIndexes.TryGetValue(normalized, out var index))
            return false;

        _locals[index].Value = value;
        return true;
    }

    internal bool TryGetLocalParameterValue(string parameterName, out object? value)
    {
        var normalized = NormalizeParameterName(parameterName);
        if (_localIndexes.TryGetValue(normalized, out var index))
        {
            value = _locals[index].Value is DBNull ? null : _locals[index].Value;
            return true;
        }

        value = null;
        return false;
    }

    private static DbParameter BuildParameter(
        DbParameterCollection inner,
        ProcParam parameter,
        ParameterDirection direction)
    {
        if (!TryResolveParameterValue(inner, parameter.Name, out var resolvedValue))
        {
            if (parameter.Required && direction == ParameterDirection.Input)
                throw new InvalidOperationException($"EXECUTE BLOCK parameter '{parameter.Name}' requires a value.");

            resolvedValue = parameter.Value;
        }

        return new ScopedDbParameter
        {
            ParameterName = parameter.Name,
            DbType = parameter.DbType,
            Direction = direction,
            Value = resolvedValue,
        };
    }

    private static bool TryResolveParameterValue(
        DbParameterCollection parameters,
        string parameterName,
        out object? value)
    {
        var normalized = NormalizeParameterName(parameterName);

        foreach (DbParameter parameter in parameters)
        {
            var candidate = NormalizeParameterName(parameter.ParameterName);
            if (!string.Equals(candidate, normalized, StringComparison.OrdinalIgnoreCase))
                continue;

            value = parameter.Value is DBNull ? null : parameter.Value;
            return true;
        }

        value = null;
        return false;
    }

    private static string NormalizeParameterName(string? rawName)
        => string.IsNullOrWhiteSpace(rawName)
            ? string.Empty
            : rawName.Trim().TrimStart('@', ':', '?');

    protected override DbParameter GetParameter(int index)
    {
        if (index < _locals.Count)
            return _locals[index];

        var remaining = index - _locals.Count;
        foreach (DbParameter parameter in _inner)
        {
            if (remaining == 0)
                return parameter;

            remaining--;
        }

        throw new IndexOutOfRangeException();
    }

    protected override DbParameter GetParameter(string parameterName)
    {
        var normalized = NormalizeParameterName(parameterName);
        if (_localIndexes.TryGetValue(normalized, out var index))
            return _locals[index];

        var innerIndex = _inner.IndexOf(parameterName);
        if (innerIndex >= 0)
        {
            var remaining = innerIndex;
            foreach (DbParameter parameter in _inner)
            {
                if (remaining == 0)
                    return parameter;

                remaining--;
            }
        }

        throw new ArgumentException(SqlExceptionMessages.ParameterNotFoundInCollection(parameterName), nameof(parameterName));
    }

    protected override void SetParameter(int index, DbParameter value)
        => throw new NotSupportedException("The execute block parameter collection is read-only.");

    protected override void SetParameter(string parameterName, DbParameter value)
        => throw new NotSupportedException("The execute block parameter collection is read-only.");

    public override int Count => _locals.Count + _inner.Count;

    public override object SyncRoot => _inner.SyncRoot;

    public override int Add(object value)
        => throw new NotSupportedException("The execute block parameter collection is read-only.");

    public override void AddRange(Array values)
        => throw new NotSupportedException("The execute block parameter collection is read-only.");

    public override void Clear()
        => throw new NotSupportedException("The execute block parameter collection is read-only.");

    public override bool Contains(object value)
        => value is DbParameter parameter && (_locals.Contains(parameter) || _inner.Contains(value));

    public override bool Contains(string value)
        => IndexOf(value) >= 0;

    public override void CopyTo(Array array, int index)
    {
        foreach (var parameter in _locals)
            array.SetValue(parameter, index++);

        foreach (DbParameter parameter in _inner)
            array.SetValue(parameter, index++);
    }

    public override IEnumerator GetEnumerator()
    {
        foreach (var parameter in _locals)
            yield return parameter;

        foreach (DbParameter parameter in _inner)
            yield return parameter;
    }

    public override int IndexOf(object value)
    {
        if (value is DbParameter parameter)
        {
            var localIndex = _locals.IndexOf(parameter);
            if (localIndex >= 0)
                return localIndex;
        }

        var innerIndex = _inner.IndexOf(value);
        return innerIndex >= 0 ? innerIndex + _locals.Count : -1;
    }

    public override int IndexOf(string parameterName)
    {
        var normalized = NormalizeParameterName(parameterName);
        if (_localIndexes.TryGetValue(normalized, out var index))
            return index;

        var innerIndex = _inner.IndexOf(parameterName);
        return innerIndex >= 0 ? innerIndex + _locals.Count : -1;
    }

    public override void Insert(int index, object value)
        => throw new NotSupportedException("The execute block parameter collection is read-only.");

    public override void Remove(object value)
        => throw new NotSupportedException("The execute block parameter collection is read-only.");

    public override void RemoveAt(int index)
        => throw new NotSupportedException("The execute block parameter collection is read-only.");

    public override void RemoveAt(string parameterName)
        => throw new NotSupportedException("The execute block parameter collection is read-only.");

    private sealed class ScopedDbParameter : DbParameter
    {
        public override DbType DbType { get; set; }

        public override ParameterDirection Direction { get; set; } = ParameterDirection.Input;

        public override bool IsNullable { get; set; }

        public override string ParameterName { get; set; } = string.Empty;

        public override int Size { get; set; }

        public override string SourceColumn { get; set; } = string.Empty;

        public override bool SourceColumnNullMapping { get; set; }

        public override object? Value { get; set; }

        public override void ResetDbType()
        {
        }

        public override byte Precision { get; set; }

        public override byte Scale { get; set; }

        public override DataRowVersion SourceVersion { get; set; } = DataRowVersion.Current;
    }
}
