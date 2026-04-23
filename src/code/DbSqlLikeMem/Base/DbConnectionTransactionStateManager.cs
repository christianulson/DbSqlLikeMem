namespace DbSqlLikeMem;

/// <summary>
/// EN: Tracks transaction identifiers, savepoints, and replay state for a connection.
/// PT: Controla identificadores de transacao, savepoints e estado de replay para uma conexao.
/// </summary>
internal sealed class DbConnectionTransactionStateManager
{
    private readonly Dictionary<string, int> _savepoints =
        new(StringComparer.OrdinalIgnoreCase);
    private readonly List<string> _savepointOrder = [];

    /// <summary>
    /// EN: Gets or sets the current transaction identifier.
    /// PT: Obtém ou define o identificador da transacao atual.
    /// </summary>
    public int CurrentTransactionId { get; set; }

    /// <summary>
    /// EN: Gets or sets the journal position recorded at transaction begin.
    /// PT: Obtém ou define a posicao do journal registrada no inicio da transacao.
    /// </summary>
    public int TransactionBeginJournalPosition { get; set; }

    /// <summary>
    /// EN: Gets or sets whether the transaction journal is currently being replayed.
    /// PT: Obtém ou define se o journal da transacao esta sendo reproduzido no momento.
    /// </summary>
    public bool IsReplayingTransactionJournal { get; set; }

    /// <summary>
    /// EN: Determines whether any runtime transaction state is still active.
    /// PT: Determina se algum estado transacional de runtime ainda esta ativo.
    /// </summary>
    /// <param name="transactionJournalCount">EN: Number of journal entries currently stored. PT: Quantidade de entradas de journal atualmente armazenadas.</param>
    public bool HasRuntimeState(int transactionJournalCount)
        => transactionJournalCount != 0
            || _savepoints.Count != 0
            || _savepointOrder.Count != 0
            || TransactionBeginJournalPosition != 0
            || IsReplayingTransactionJournal;

    /// <summary>
    /// EN: Tries to get the journal position of a savepoint.
    /// PT: Tenta obter a posicao do journal de um savepoint.
    /// </summary>
    /// <param name="savepointName">EN: Savepoint name to locate. PT: Nome do savepoint a localizar.</param>
    /// <param name="journalPosition">EN: Journal position when found. PT: Posicao do journal quando encontrada.</param>
    public bool TryGetSavepointJournalPosition(
        string savepointName,
        out int journalPosition)
        => _savepoints.TryGetValue(savepointName, out journalPosition);

    /// <summary>
    /// EN: Registers or updates a savepoint at a given journal position.
    /// PT: Registra ou atualiza um savepoint em uma determinada posicao do journal.
    /// </summary>
    /// <param name="savepointName">EN: Savepoint name to store. PT: Nome do savepoint a armazenar.</param>
    /// <param name="journalPosition">EN: Journal position associated with the savepoint. PT: Posicao do journal associada ao savepoint.</param>
    public void SetSavepoint(string savepointName, int journalPosition)
    {
        if (_savepoints.ContainsKey(savepointName))
            RemoveSavepointOrderEntries(savepointName);

        _savepoints[savepointName] = journalPosition;
        _savepointOrder.Add(savepointName);
    }

    /// <summary>
    /// EN: Removes a savepoint from the tracked state.
    /// PT: Remove um savepoint do estado controlado.
    /// </summary>
    /// <param name="savepointName">EN: Savepoint name to remove. PT: Nome do savepoint a remover.</param>
    public void RemoveSavepoint(string savepointName)
    {
        _savepoints.Remove(savepointName);
        RemoveSavepointOrderEntries(savepointName);
    }

    /// <summary>
    /// EN: Finds the last order index for a savepoint name.
    /// PT: Localiza o ultimo indice de ordem para um nome de savepoint.
    /// </summary>
    /// <param name="savepointName">EN: Savepoint name to search. PT: Nome do savepoint a procurar.</param>
    /// <returns>EN: Zero-based order index or -1. PT: Indice de ordem baseado em zero ou -1.</returns>
    public int FindSavepointOrderIndex(string savepointName)
    {
        for (var i = _savepointOrder.Count - 1; i >= 0; i--)
        {
            if (_savepointOrder[i].Equals(savepointName, StringComparison.OrdinalIgnoreCase))
                return i;
        }

        return -1;
    }

    /// <summary>
    /// EN: Removes savepoints that were registered after the specified order index.
    /// PT: Remove savepoints registrados depois do indice de ordem especificado.
    /// </summary>
    /// <param name="savepointIndex">EN: Order index that acts as the cutoff. PT: Indice de ordem que serve como corte.</param>
    public void RemoveSavepointsAfterIndex(int savepointIndex)
    {
        for (var idx = _savepointOrder.Count - 1; idx > savepointIndex; idx--)
        {
            _savepoints.Remove(_savepointOrder[idx]);
            _savepointOrder.RemoveAt(idx);
        }
    }

    /// <summary>
    /// EN: Clears the tracked transaction state.
    /// PT: Limpa o estado transacional controlado.
    /// </summary>
    public void Clear()
    {
        _savepoints.Clear();
        _savepointOrder.Clear();
        TransactionBeginJournalPosition = 0;
        IsReplayingTransactionJournal = false;
    }

    private void RemoveSavepointOrderEntries(string savepointName)
    {
        for (var i = _savepointOrder.Count - 1; i >= 0; i--)
        {
            if (_savepointOrder[i].Equals(savepointName, StringComparison.OrdinalIgnoreCase))
                _savepointOrder.RemoveAt(i);
        }
    }
}
