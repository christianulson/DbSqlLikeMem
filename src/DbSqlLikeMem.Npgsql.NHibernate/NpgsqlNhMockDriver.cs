namespace DbSqlLikeMem.Npgsql.NHibernate;

/// <summary>
/// EN: NHibernate driver bound to DbSqlLikeMem Npgsql mock ADO.NET types.
/// PT: Driver NHibernate ligado aos tipos ADO.NET simulado Npgsql do DbSqlLikeMem.
/// </summary>
public sealed class NpgsqlNhMockDriver : ReflectionBasedDriver
{
    /// <summary>
    /// EN: Initializes a NHibernate mock driver for DbSqlLikeMem Npgsql provider types.
    /// PT: Inicializa um driver simulado do NHibernate para os tipos do provedor Npgsql do DbSqlLikeMem.
    /// </summary>
    public NpgsqlNhMockDriver()
        : base(
            "DbSqlLikeMem.Npgsql",
            "DbSqlLikeMem.Npgsql",
            "DbSqlLikeMem.Npgsql.NpgsqlConnectionMock",
            "DbSqlLikeMem.Npgsql.NpgsqlCommandMock")
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
