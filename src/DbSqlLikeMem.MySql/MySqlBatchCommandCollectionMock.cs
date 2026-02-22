
namespace DbSqlLikeMem.MySql;

/// <summary>
/// EN: Represents the collection of commands maintained by MySqlBatchMock.
/// PT: Representa a coleção de comandos mantida por MySqlBatchMock.
/// </summary>
public class MySqlBatchCommandCollectionMock
#if NET6_0_OR_GREATER
    : DbBatchCommandCollection
#endif
{
    internal List<MySqlBatchCommandMock> Commands { get; } = [];

    /// <summary>
    /// EN: Provides collection metadata and operations for MySQL batch commands.
    /// PT: Fornece metadados e operações de coleção para comandos de lote MySQL.
    /// </summary>
    public
#if NET6_0_OR_GREATER
        override
#endif
         int Count => Commands.Count;

    /// <summary>
    /// EN: Provides collection metadata and operations for MySQL batch commands.
    /// PT: Fornece metadados e operações de coleção para comandos de lote MySQL.
    /// </summary>
    public
#if NET6_0_OR_GREATER
        override
#endif
         bool IsReadOnly => false;

#if NET6_0_OR_GREATER
    /// <summary>
    /// EN: Adds a command to the batch collection.
    /// PT: Adiciona um comando à coleção de lote.
    /// </summary>
    public override void Add(DbBatchCommand item)
#else
    /// <summary>
    /// EN: Adds a command to the batch collection.
    /// PT: Adiciona um comando à coleção de lote.
    /// </summary>
    public void Add(MySqlBatchCommandMock item)
#endif
    {
        if (item is MySqlBatchCommandMock b)
            Commands.Add(b);
    }

    /// <summary>
    /// EN: Provides collection metadata and operations for MySQL batch commands.
    /// PT: Fornece metadados e operações de coleção para comandos de lote MySQL.
    /// </summary>
    public
#if NET6_0_OR_GREATER
        override
#endif
          void Clear()
    {
        Commands.Clear();
    }

#if NET6_0_OR_GREATER
    /// <summary>
    /// EN: Determines whether a command is present in the batch collection.
    /// PT: Determina se um comando está presente na coleção de lote.
    /// </summary>
    public override bool Contains(DbBatchCommand item)
#else
    /// <summary>
    /// EN: Determines whether a command is present in the batch collection.
    /// PT: Determina se um comando está presente na coleção de lote.
    /// </summary>
    public bool Contains(MySqlBatchCommandMock item)
#endif

    => Commands.Any(i => i.CommandText == item.CommandText);

#if NET6_0_OR_GREATER
    /// <summary>
    /// EN: Copies commands to the specified array starting at the given index.
    /// PT: Copia os comandos para o array informado a partir do índice indicado.
    /// </summary>
    public override void CopyTo(DbBatchCommand[] array, int arrayIndex)
#else
    /// <summary>
    /// EN: Copies commands to the specified array starting at the given index.
    /// PT: Copia os comandos para o array informado a partir do índice indicado.
    /// </summary>
    public void CopyTo(MySqlBatchCommandMock[] array, int arrayIndex)
#endif
    {
        Commands.CopyTo([.. array.Select(_ => (MySqlBatchCommandMock)_)], arrayIndex);
    }
#if NET6_0_OR_GREATER
    /// <summary>
    /// EN: Returns an enumerator over the batch commands.
    /// PT: Retorna um enumerador sobre os comandos em lote.
    /// </summary>
    public override IEnumerator<DbBatchCommand> GetEnumerator()
        => Commands.Cast<DbBatchCommand>().GetEnumerator();
#else
    /// <summary>
    /// EN: Returns an enumerator over the batch commands.
    /// PT: Retorna um enumerador sobre os comandos em lote.
    /// </summary>
    public IEnumerator<MySqlBatchCommandMock> GetEnumerator()
        => Commands.GetEnumerator();
#endif

#if NET6_0_OR_GREATER
    /// <summary>
    /// EN: Returns the zero-based index of a command in the collection.
    /// PT: Retorna o índice baseado em zero de um comando na coleção.
    /// </summary>
    public override int IndexOf(DbBatchCommand item)
#else
    /// <summary>
    /// EN: Returns the zero-based index of a command in the collection.
    /// PT: Retorna o índice baseado em zero de um comando na coleção.
    /// </summary>
    public int IndexOf(MySqlBatchCommandMock item)
#endif

    => Commands.IndexOf((MySqlBatchCommandMock)item);

#if NET6_0_OR_GREATER
    /// <summary>
    /// EN: Inserts a command at the specified index.
    /// PT: Insere um comando no índice especificado.
    /// </summary>
    public override void Insert(int index, DbBatchCommand item)
#else
    /// <summary>
    /// EN: Inserts a command at the specified index.
    /// PT: Insere um comando no índice especificado.
    /// </summary>
    public void Insert(int index, MySqlBatchCommandMock item)
#endif

    => Commands.Insert(index, (MySqlBatchCommandMock)item);

#if NET6_0_OR_GREATER
    /// <summary>
    /// EN: Removes the specified command from the collection.
    /// PT: Remove o comando especificado da coleção.
    /// </summary>
    public override bool Remove(DbBatchCommand item)
#else
    /// <summary>
    /// EN: Removes the specified command from the collection.
    /// PT: Remove o comando especificado da coleção.
    /// </summary>
    public bool Remove(MySqlBatchCommandMock item)
#endif

    => Commands.Remove((MySqlBatchCommandMock)item);

    /// <summary>
    /// EN: Provides collection metadata and operations for MySQL batch commands.
    /// PT: Fornece metadados e operações de coleção para comandos de lote MySQL.
    /// </summary>
    public
#if NET6_0_OR_GREATER
        override
#endif
          void RemoveAt(int index)
    => Commands.RemoveAt(index);

#if NET6_0_OR_GREATER
    /// <summary>
    /// EN: Returns the command stored at the specified batch index.
    /// PT: Retorna o comando armazenado no índice de lote especificado.
    /// </summary>
    protected override DbBatchCommand GetBatchCommand(int index)
#else
    /// <summary>
    /// EN: Returns the command stored at the specified batch index.
    /// PT: Retorna o comando armazenado no índice de lote especificado.
    /// </summary>
    protected MySqlBatchCommandMock GetBatchCommand(int index)
#endif

    => Commands[index];

#if NET6_0_OR_GREATER
    /// <summary>
    /// EN: Replaces the command stored at the specified batch index.
    /// PT: Substitui o comando armazenado no índice de lote especificado.
    /// </summary>
    protected override void SetBatchCommand(int index, DbBatchCommand batchCommand)
#else
    /// <summary>
    /// EN: Replaces the command stored at the specified batch index.
    /// PT: Substitui o comando armazenado no índice de lote especificado.
    /// </summary>
    protected void SetBatchCommand(int index, MySqlBatchCommandMock batchCommand)
#endif
    => Commands[index] = (MySqlBatchCommandMock)batchCommand;
}
