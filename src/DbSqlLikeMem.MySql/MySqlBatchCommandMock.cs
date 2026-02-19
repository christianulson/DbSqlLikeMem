namespace DbSqlLikeMem.MySql;

/// <summary>
/// MySQL mock type used to emulate provider behavior for tests.
/// Tipo de mock MySQL usado para emular o comportamento do provedor em testes.
/// </summary>
public sealed class MySqlBatchCommandMock :
#if NET6_0_OR_GREATER
    DbBatchCommand,
#endif
    IMySqlCommandMock
{
    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    public MySqlBatchCommandMock()
        : this(null)
    {
    }

    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    public MySqlBatchCommandMock(string? commandText)
    {
        CommandText = commandText ?? "";
        CommandType = CommandType.Text;
    }

#if NET6_0_OR_GREATER
    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    public override string CommandText { get; set; }
#else
	/// <summary>
	/// Mock API member implementation for compatibility with MySQL provider contracts.
	/// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
	/// </summary>
	public string CommandText { get; set; }
#endif
#if NET6_0_OR_GREATER
    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    public override CommandType CommandType { get; set; }
#else
	/// <summary>
	/// Mock API member implementation for compatibility with MySQL provider contracts.
	/// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
	/// </summary>
	public CommandType CommandType { get; set; }
#endif
#if NET6_0_OR_GREATER
    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    public override int RecordsAffected =>
#else
	/// <summary>
	/// Mock API member implementation for compatibility with MySQL provider contracts.
	/// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
	/// </summary>
	public int RecordsAffected =>
#endif
        0;

#if NET6_0_OR_GREATER
    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    public new MySqlParameterCollection Parameters =>
#else
	/// <summary>
	/// Mock API member implementation for compatibility with MySQL provider contracts.
	/// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
	/// </summary>
	public MySqlParameterCollection Parameters =>
#endif
        m_parameterCollection ??= Batch?.Connection?.CreateCommand().Parameters as MySqlParameterCollection
            ?? new MySqlCommand().Parameters;

#pragma warning disable CA1822 // Mark members as static
    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    public
#if NET8_0_OR_GREATER
        override
#endif
        DbParameter CreateParameter() => new MySqlParameter();

    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
    public
#if NET8_0_OR_GREATER
        override
#endif
        bool CanCreateParameter => true;
#pragma warning restore CA1822 // Mark members as static

#if NET6_0_OR_GREATER
    /// <summary>
    /// Mock API member implementation for compatibility with MySQL provider contracts.
    /// Implementação de membro da API mock para compatibilidade com os contratos do provedor MySQL.
    /// </summary>
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
