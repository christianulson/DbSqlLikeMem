namespace DbSqlLikeMem.Db2.NHibernate;

/// <summary>
/// EN: NHibernate driver bound to DbSqlLikeMem DB2 mock ADO.NET types.
/// PT-br: Driver NHibernate ligado aos tipos ADO.NET simulado DB2 do DbSqlLikeMem.
/// </summary>
public sealed class Db2NhMockDriver : ReflectionBasedDriver
{
    /// <summary>
    /// EN: Initializes a NHibernate mock driver for DbSqlLikeMem DB2 provider types.
    /// PT-br: Inicializa um driver simulado do NHibernate para os tipos do provedor DB2 do DbSqlLikeMem.
    /// </summary>
    public Db2NhMockDriver()
        : base(
            "DbSqlLikeMem.Db2",
            "DbSqlLikeMem.Db2",
            "DbSqlLikeMem.Db2.Db2ConnectionMock",
            "DbSqlLikeMem.Db2.Db2CommandMock")
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
