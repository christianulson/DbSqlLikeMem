namespace DbSqlLikeMem.MySql;

/// <summary>
/// EN: Represents a command entry executed by MySqlBatchMock.
/// PT: Representa uma entrada de comando executada por MySqlBatchMock.
/// </summary>
public sealed class MySqlBatchCommandMock :
#if NET6_0_OR_GREATER
    DbBatchCommand,
#endif
    IMySqlCommandMock
{
    /// <summary>
    /// EN: Represents a command entry executed by MySqlBatchMock.
    /// PT: Representa uma entrada de comando executada por MySqlBatchMock.
    /// </summary>
    public MySqlBatchCommandMock()
        : this(null)
    {
    }

    /// <summary>
    /// EN: Initializes a batch command with the provided SQL text.
    /// PT: Inicializa um comando em lote com o texto SQL informado.
    /// </summary>
    public MySqlBatchCommandMock(string? commandText)
    {
        CommandText = commandText ?? "";
        CommandType = CommandType.Text;
    }

#if NET6_0_OR_GREATER
    /// <summary>
    /// EN: Gets or sets the SQL text executed by this batch command.
    /// PT: Obtém ou define o texto SQL executado por este comando em lote.
    /// </summary>
    public override string CommandText { get; set; }
#else
	/// <summary>
	/// EN: Gets or sets the SQL text executed by this batch command.
	/// PT: Obtém ou define o texto SQL executado por este comando em lote.
	/// </summary>
	public string CommandText { get; set; }
#endif
#if NET6_0_OR_GREATER
    /// <summary>
    /// EN: Gets or sets how CommandText is interpreted.
    /// PT: Obtém ou define como CommandText é interpretado.
    /// </summary>
    public override CommandType CommandType { get; set; }
#else
	/// <summary>
	/// EN: Gets or sets how CommandText is interpreted.
	/// PT: Obtém ou define como CommandText é interpretado.
	/// </summary>
	public CommandType CommandType { get; set; }
#endif
#if NET6_0_OR_GREATER
    /// <summary>
    /// EN: Gets the affected row count reported by this batch command.
    /// PT: Obtém a contagem de linhas afetadas reportada por este comando em lote.
    /// </summary>
    public override int RecordsAffected =>
#else
	/// <summary>
	/// EN: Gets the affected row count reported by this batch command.
	/// PT: Obtém a contagem de linhas afetadas reportada por este comando em lote.
	/// </summary>
	public int RecordsAffected =>
#endif
        0;

#if NET6_0_OR_GREATER
    /// <summary>
    /// EN: Gets the parameter collection used by this batch command.
    /// PT: Obtém a coleção de parâmetros usada por este comando em lote.
    /// </summary>
    public new MySqlParameterCollection Parameters =>
#else
	/// <summary>
	/// EN: Gets the parameter collection used by this batch command.
	/// PT: Obtém a coleção de parâmetros usada por este comando em lote.
	/// </summary>
	public MySqlParameterCollection Parameters =>
#endif
        m_parameterCollection ??= Batch?.Connection?.CreateCommand().Parameters as MySqlParameterCollection
            ?? new MySqlCommand().Parameters;

#pragma warning disable CA1822 // Mark members as static
    /// <summary>
    /// EN: Creates metadata and parameters for this batch command when needed.
    /// PT: Cria metadados e parâmetros para este comando em lote quando necessário.
    /// </summary>
    public
#if NET8_0_OR_GREATER
        override
#endif
        DbParameter CreateParameter() => new MySqlParameter();

    /// <summary>
    /// EN: Creates metadata and parameters for this batch command when needed.
    /// PT: Cria metadados e parâmetros para este comando em lote quando necessário.
    /// </summary>
    public
#if NET8_0_OR_GREATER
        override
#endif
        bool CanCreateParameter => true;
#pragma warning restore CA1822 // Mark members as static

#if NET6_0_OR_GREATER
    /// <summary>
    /// EN: Gets the base ADO.NET parameter collection view for this command.
    /// PT: Obtém a visão da coleção de parâmetros base do ADO.NET para este comando.
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
