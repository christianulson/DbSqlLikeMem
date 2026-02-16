namespace DbSqlLikeMem.MySql;

public sealed class MySqlBatchCommandMock :
#if NET6_0_OR_GREATER
    DbBatchCommand,
#endif
    IMySqlCommandMock
{
    public MySqlBatchCommandMock()
        : this(null)
    {
    }

    public MySqlBatchCommandMock(string? commandText)
    {
        CommandText = commandText ?? "";
        CommandType = CommandType.Text;
    }

#if NET6_0_OR_GREATER
    public override string CommandText { get; set; }
#else
	public string CommandText { get; set; }
#endif
#if NET6_0_OR_GREATER
    public override CommandType CommandType { get; set; }
#else
	public CommandType CommandType { get; set; }
#endif
#if NET6_0_OR_GREATER
    public override int RecordsAffected =>
#else
	public int RecordsAffected =>
#endif
        0;

#if NET6_0_OR_GREATER
    public new MySqlParameterCollection Parameters =>
#else
	public MySqlParameterCollection Parameters =>
#endif
        m_parameterCollection ?? ( m_parameterCollection = Batch?.Connection.CreateCommand().Parameters as MySqlParameterCollection);

#pragma warning disable CA1822 // Mark members as static
    public
#if NET8_0_OR_GREATER
        override
#endif
        DbParameter CreateParameter() => new MySqlParameter();

    public
#if NET8_0_OR_GREATER
        override
#endif
        bool CanCreateParameter => true;
#pragma warning restore CA1822 // Mark members as static

#if NET6_0_OR_GREATER
    protected override DbParameterCollection DbParameterCollection => Parameters;
#endif

    bool IMySqlCommandMock.AllowUserVariables => false;

    CommandBehavior IMySqlCommandMock.CommandBehavior => Batch!.CurrentCommandBehavior;

    MySqlParameterCollection? IMySqlCommandMock.RawParameters => m_parameterCollection;

    MySqlAttributeCollection? IMySqlCommandMock.RawAttributes => null;

    MySqlConnectionMock? IMySqlCommandMock.Connection => Batch?.Connection;

    long IMySqlCommandMock.LastInsertedId => m_lastInsertedId;

    //PreparedStatements? IMySqlCommand.TryGetPreparedStatements() => null;

    void IMySqlCommandMock.SetLastInsertedId(long lastInsertedId) => m_lastInsertedId = lastInsertedId;

    MySqlParameterCollection? IMySqlCommandMock.OutParameters { get; set; }

    MySqlParameter? IMySqlCommandMock.ReturnParameter { get; set; }

    //ICancellableCommand IMySqlCommandMock.CancellableCommand => Batch!;
    //ILogger IMySqlCommand.Logger => Batch!.Connection!.LoggingConfiguration.CommandLogger;

    internal MySqlBatchMock? Batch { get; set; }

    private MySqlParameterCollection? m_parameterCollection;
    private long m_lastInsertedId;
}
