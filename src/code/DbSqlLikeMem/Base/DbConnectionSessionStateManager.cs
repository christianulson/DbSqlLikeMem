namespace DbSqlLikeMem;

/// <summary>
/// EN: Tracks session-scoped values, context information, and sequence state for a connection.
/// PT: Controla valores de escopo de sessao, informacoes de contexto e estado de sequence para uma conexao.
/// </summary>
internal sealed class DbConnectionSessionStateManager
{
    private readonly Dictionary<string, long> _sessionSequenceValues =
        new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, object?> _sessionContextValues =
        new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, object?> _transactionContextValues =
        new(StringComparer.OrdinalIgnoreCase);
    private byte[]? _contextInfo;
    private string? _lastSessionSequenceKey;

    /// <summary>
    /// EN: Clears all tracked session state.
    /// PT: Limpa todo o estado de sessao controlado.
    /// </summary>
    public void ClearAll()
    {
        _sessionSequenceValues.Clear();
        _sessionContextValues.Clear();
        _transactionContextValues.Clear();
        _contextInfo = null;
        _lastSessionSequenceKey = null;
    }

    /// <summary>
    /// EN: Clears only the transaction-scoped context values.
    /// PT: Limpa apenas os valores de contexto de escopo transacional.
    /// </summary>
    public void ClearTransactionContextValues()
    {
        if (_transactionContextValues.Count > 0)
            _transactionContextValues.Clear();
    }

    /// <summary>
    /// EN: Stores a session sequence value for the given schema and sequence name.
    /// PT: Armazena um valor de sequence de sessao para o schema e sequence informados.
    /// </summary>
    /// <param name="sequenceName">EN: Sequence name to store. PT: Nome da sequence a armazenar.</param>
    /// <param name="value">EN: Sequence value to store. PT: Valor da sequence a armazenar.</param>
    /// <param name="schemaName">EN: Schema name that scopes the sequence. PT: Nome do schema que delimita a sequence.</param>
    public void SetSessionSequenceValue(
        string sequenceName,
        long value,
        string schemaName)
    {
        var key = BuildSessionSequenceKey(sequenceName, schemaName);
        _sessionSequenceValues[key] = value;
        _lastSessionSequenceKey = key;
    }

    /// <summary>
    /// EN: Tries to get a session sequence value by schema and sequence name.
    /// PT: Tenta obter um valor de sequence de sessao por schema e nome da sequence.
    /// </summary>
    /// <param name="sequenceName">EN: Sequence name to look up. PT: Nome da sequence a localizar.</param>
    /// <param name="value">EN: Sequence value when found. PT: Valor da sequence quando encontrado.</param>
    /// <param name="schemaName">EN: Schema name that scopes the sequence. PT: Nome do schema que delimita a sequence.</param>
    public bool TryGetSessionSequenceValue(
        string sequenceName,
        out long value,
        string schemaName)
        => _sessionSequenceValues.TryGetValue(BuildSessionSequenceKey(sequenceName, schemaName), out value);

    /// <summary>
    /// EN: Tries to get the most recently stored session sequence value.
    /// PT: Tenta obter o valor de sequence de sessao mais recentemente armazenado.
    /// </summary>
    /// <param name="value">EN: Sequence value when found. PT: Valor da sequence quando encontrado.</param>
    public bool TryGetLastSessionSequenceValue(out long value)
    {
        value = default;
        return _lastSessionSequenceKey is not null
            && _sessionSequenceValues.TryGetValue(_lastSessionSequenceKey, out value);
    }

    /// <summary>
    /// EN: Stores or removes a session context value.
    /// PT: Armazena ou remove um valor de contexto de sessao.
    /// </summary>
    /// <param name="key">EN: Context key to store. PT: Chave de contexto a armazenar.</param>
    /// <param name="value">EN: Context value to store, or null to remove it. PT: Valor de contexto a armazenar, ou null para remover.</param>
    public void SetSessionContextValue(string key, object? value)
    {
        if (string.IsNullOrWhiteSpace(key))
            return;

        if (value is null)
        {
            _sessionContextValues.Remove(key);
            return;
        }

        _sessionContextValues[key] = value;
    }

    /// <summary>
    /// EN: Tries to get a session context value by key.
    /// PT: Tenta obter um valor de contexto de sessao pela chave.
    /// </summary>
    /// <param name="key">EN: Context key to look up. PT: Chave de contexto a localizar.</param>
    /// <param name="value">EN: Context value when found. PT: Valor de contexto quando encontrado.</param>
    public bool TryGetSessionContextValue(string key, out object? value)
        => _sessionContextValues.TryGetValue(key, out value);

    /// <summary>
    /// EN: Stores or removes a transaction context value.
    /// PT: Armazena ou remove um valor de contexto de transacao.
    /// </summary>
    /// <param name="key">EN: Context key to store. PT: Chave de contexto a armazenar.</param>
    /// <param name="value">EN: Context value to store, or null to remove it. PT: Valor de contexto a armazenar, ou null para remover.</param>
    public void SetTransactionContextValue(string key, object? value)
    {
        if (string.IsNullOrWhiteSpace(key))
            return;

        if (value is null)
        {
            _transactionContextValues.Remove(key);
            return;
        }

        _transactionContextValues[key] = value;
    }

    /// <summary>
    /// EN: Tries to get a transaction context value by key.
    /// PT: Tenta obter um valor de contexto de transacao pela chave.
    /// </summary>
    /// <param name="key">EN: Context key to look up. PT: Chave de contexto a localizar.</param>
    /// <param name="value">EN: Context value when found. PT: Valor de contexto quando encontrado.</param>
    public bool TryGetTransactionContextValue(string key, out object? value)
        => _transactionContextValues.TryGetValue(key, out value);

    /// <summary>
    /// EN: Stores the connection context-info payload.
    /// PT: Armazena o payload de context-info da conexao.
    /// </summary>
    /// <param name="value">EN: Context-info payload to store. PT: Payload de context-info a armazenar.</param>
    public void SetContextInfo(byte[]? value)
        => _contextInfo = value;

    /// <summary>
    /// EN: Gets the stored connection context-info payload.
    /// PT: Obtém o payload de context-info armazenado da conexao.
    /// </summary>
    public byte[]? GetContextInfo()
        => _contextInfo;

    /// <summary>
    /// EN: Clears a stored session sequence value for the given schema and sequence name.
    /// PT: Limpa um valor de sequence de sessao armazenado para o schema e sequence informados.
    /// </summary>
    /// <param name="sequenceName">EN: Sequence name to clear. PT: Nome da sequence a limpar.</param>
    /// <param name="schemaName">EN: Schema name that scopes the sequence. PT: Nome do schema que delimita a sequence.</param>
    public void ClearSessionSequenceValue(
        string sequenceName,
        string schemaName)
    {
        var key = BuildSessionSequenceKey(sequenceName, schemaName);
        _sessionSequenceValues.Remove(key);
        if (string.Equals(_lastSessionSequenceKey, key, StringComparison.OrdinalIgnoreCase))
            _lastSessionSequenceKey = null;
    }

    private string BuildSessionSequenceKey(
        string sequenceName,
        string schemaName)
        => $"{schemaName}:{sequenceName.NormalizeName()}";
}
