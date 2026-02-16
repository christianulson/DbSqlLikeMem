
namespace DbSqlLikeMem.MySql;

public class MySqlBatchCommandCollectionMock
#if NET6_0_OR_GREATER
    : DbBatchCommandCollection
#endif
{
    internal List<MySqlBatchCommandMock> Commands { get; } = [];

    public
#if NET6_0_OR_GREATER
        override
#endif
         int Count => Commands.Count;

    public
#if NET6_0_OR_GREATER
        override
#endif
         bool IsReadOnly => false;

#if NET6_0_OR_GREATER
    public override void Add(DbBatchCommand item)
#else
    public void Add(MySqlBatchCommandMock item)
#endif
    {
        if (item is MySqlBatchCommandMock b)
            Commands.Add(b);
    }

    public
#if NET6_0_OR_GREATER
        override
#endif
          void Clear()
    {
        Commands.Clear();
    }

#if NET6_0_OR_GREATER
    public override bool Contains(DbBatchCommand item)
#else
    public bool Contains(MySqlBatchCommandMock item)
#endif

    => Commands.Any(i => i.CommandText == item.CommandText);

#if NET6_0_OR_GREATER
    public override void CopyTo(DbBatchCommand[] array, int arrayIndex)
#else
    public void CopyTo(MySqlBatchCommandMock[] array, int arrayIndex)
#endif
    {
        Commands.CopyTo([.. array.Select(_ => (MySqlBatchCommandMock)_)], arrayIndex);
    }
#if NET6_0_OR_GREATER
    public override IEnumerator<DbBatchCommand> GetEnumerator()
        => Commands.Cast<DbBatchCommand>().GetEnumerator();
#else
    public IEnumerator<MySqlBatchCommandMock> GetEnumerator()
        => Commands.GetEnumerator();
#endif

#if NET6_0_OR_GREATER
    public override int IndexOf(DbBatchCommand item)
#else
    public int IndexOf(MySqlBatchCommandMock item)
#endif

    => Commands.IndexOf((MySqlBatchCommandMock)item);

#if NET6_0_OR_GREATER
    public override void Insert(int index, DbBatchCommand item)
#else
    public void Insert(int index, MySqlBatchCommandMock item)
#endif

    => Commands.Insert(index, (MySqlBatchCommandMock)item);

#if NET6_0_OR_GREATER
    public override bool Remove(DbBatchCommand item)
#else
    public bool Remove(MySqlBatchCommandMock item)
#endif

    => Commands.Remove((MySqlBatchCommandMock)item);

    public
#if NET6_0_OR_GREATER
        override
#endif
          void RemoveAt(int index)
    => Commands.RemoveAt(index);

#if NET6_0_OR_GREATER
    protected override DbBatchCommand GetBatchCommand(int index)
#else
    protected MySqlBatchCommandMock GetBatchCommand(int index)
#endif

    => Commands[index];

#if NET6_0_OR_GREATER
    protected override void SetBatchCommand(int index, DbBatchCommand batchCommand)
#else
    protected void SetBatchCommand(int index, MySqlBatchCommandMock batchCommand)
#endif
    => Commands[index] = (MySqlBatchCommandMock)batchCommand;
}
