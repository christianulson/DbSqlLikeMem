namespace DbSqlLikeMem.Sqlite.HNibernate;

/// <summary>
/// EN: NHibernate driver bound to DbSqlLikeMem Sqlite mock ADO.NET types.
/// PT: Driver NHibernate ligado aos tipos ADO.NET mock Sqlite do DbSqlLikeMem.
/// </summary>
public sealed class SqliteNhMockDriver : ReflectionBasedDriver
{
    /// <summary>
    /// EN: Initializes a NHibernate mock driver for DbSqlLikeMem Sqlite provider types.
    /// PT: Inicializa um driver mock do NHibernate para os tipos do provedor Sqlite do DbSqlLikeMem.
    /// </summary>
    public SqliteNhMockDriver()
        : base(
            "DbSqlLikeMem.Sqlite",
            "DbSqlLikeMem.Sqlite",
            "DbSqlLikeMem.Sqlite.SqliteConnectionMock",
            "DbSqlLikeMem.Sqlite.SqliteCommandMock")
    {
    }

    /// <summary>
    /// EN: Indicates that named parameter prefixes must be rendered in generated SQL text.
    /// PT: Indica que os prefixos de parâmetros nomeados devem ser renderizados no SQL gerado.
    /// </summary>
    public override bool UseNamedPrefixInSql => true;

    /// <summary>
    /// EN: Indicates that parameter names are expected to include the named prefix.
    /// PT: Indica que os nomes dos parâmetros devem incluir o prefixo nomeado.
    /// </summary>
    public override bool UseNamedPrefixInParameter => true;

    /// <summary>
    /// EN: Gets the named parameter prefix used by this driver.
    /// PT: Obtém o prefixo de parâmetro nomeado usado por este driver.
    /// </summary>
    public override string NamedPrefix => "@";
}
