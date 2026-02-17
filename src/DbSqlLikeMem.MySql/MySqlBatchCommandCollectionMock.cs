
namespace DbSqlLikeMem.MySql;

/// <summary>
/// MySQL mock type used to emulate provider behavior for tests.
/// Tipo de mock MySQL usado para emular o comportamento do provedor em testes.
/// </summary>
public class MySqlBatchCommandCollectionMock
#if NET6_0_OR_GREATER
    : DbBatchCommandCollection
#endif
{
    internal List<MySqlBatchCommandMock> Commands { get; } = [];

    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    public
#if NET6_0_OR_GREATER
        override
#endif
         int Count => Commands.Count;

    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    public
#if NET6_0_OR_GREATER
        override
#endif
         bool IsReadOnly => false;

#if NET6_0_OR_GREATER
    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    public override void Add(DbBatchCommand item)
#else
    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    public void Add(MySqlBatchCommandMock item)
#endif
    {
        if (item is MySqlBatchCommandMock b)
            Commands.Add(b);
    }

    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
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
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    public override bool Contains(DbBatchCommand item)
#else
    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    public bool Contains(MySqlBatchCommandMock item)
#endif

    => Commands.Any(i => i.CommandText == item.CommandText);

#if NET6_0_OR_GREATER
    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    public override void CopyTo(DbBatchCommand[] array, int arrayIndex)
#else
    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    public void CopyTo(MySqlBatchCommandMock[] array, int arrayIndex)
#endif
    {
        Commands.CopyTo([.. array.Select(_ => (MySqlBatchCommandMock)_)], arrayIndex);
    }
#if NET6_0_OR_GREATER
    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    public override IEnumerator<DbBatchCommand> GetEnumerator()
        => Commands.Cast<DbBatchCommand>().GetEnumerator();
#else
    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    public IEnumerator<MySqlBatchCommandMock> GetEnumerator()
        => Commands.GetEnumerator();
#endif

#if NET6_0_OR_GREATER
    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    public override int IndexOf(DbBatchCommand item)
#else
    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    public int IndexOf(MySqlBatchCommandMock item)
#endif

    => Commands.IndexOf((MySqlBatchCommandMock)item);

#if NET6_0_OR_GREATER
    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    public override void Insert(int index, DbBatchCommand item)
#else
    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    public void Insert(int index, MySqlBatchCommandMock item)
#endif

    => Commands.Insert(index, (MySqlBatchCommandMock)item);

#if NET6_0_OR_GREATER
    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    public override bool Remove(DbBatchCommand item)
#else
    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    public bool Remove(MySqlBatchCommandMock item)
#endif

    => Commands.Remove((MySqlBatchCommandMock)item);

    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    public
#if NET6_0_OR_GREATER
        override
#endif
          void RemoveAt(int index)
    => Commands.RemoveAt(index);

#if NET6_0_OR_GREATER
    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    protected override DbBatchCommand GetBatchCommand(int index)
#else
    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    protected MySqlBatchCommandMock GetBatchCommand(int index)
#endif

    => Commands[index];

#if NET6_0_OR_GREATER
    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    protected override void SetBatchCommand(int index, DbBatchCommand batchCommand)
#else
    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    protected void SetBatchCommand(int index, MySqlBatchCommandMock batchCommand)
#endif
    => Commands[index] = (MySqlBatchCommandMock)batchCommand;
}
