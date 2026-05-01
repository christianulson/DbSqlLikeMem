namespace DbSqlLikeMem.Oracle.NHibernate;

/// <summary>
/// EN: NHibernate driver bound to DbSqlLikeMem Oracle mock ADO.NET types.
/// PT-br: Driver NHibernate ligado aos tipos ADO.NET simulado Oracle do DbSqlLikeMem.
/// </summary>
public sealed class OracleNhMockDriver : ReflectionBasedDriver
{
    /// <summary>
    /// EN: Initializes a NHibernate mock driver for DbSqlLikeMem Oracle provider types.
    /// PT-br: Inicializa um driver simulado do NHibernate para os tipos do provedor Oracle do DbSqlLikeMem.
    /// </summary>
    public OracleNhMockDriver()
        : base(
            "DbSqlLikeMem.Oracle",
            "DbSqlLikeMem.Oracle",
            "DbSqlLikeMem.Oracle.OracleConnectionMock",
            "DbSqlLikeMem.Oracle.OracleCommandMock")
    {
    }

    /// <summary>
    /// EN: Indicates that named parameter prefixes must be rendered in generated SQL text.
    /// PT-br: Indica que os prefixos de parâmetros nomeados devem ser renderizados no SQL gerado.
    /// </summary>
    public override bool UseNamedPrefixInSql => true;

    /// <summary>
    /// EN: Indicates that parameter names are expected to include the named prefix.
    /// PT-br: Indica que os nomes dos parâmetros devem incluir o prefixo nomeado.
    /// </summary>
    public override bool UseNamedPrefixInParameter => true;

    /// <summary>
    /// EN: Gets the named parameter prefix used by this driver.
    /// PT-br: Obtém o prefixo de parâmetro nomeado usado por este driver.
    /// </summary>
    public override string NamedPrefix => "@";
}
