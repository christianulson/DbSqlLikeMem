namespace DbSqlLikeMem.MySql;

/// <summary>
/// EN: SQL dialect implementation for MariaDB built on top of the shared MySQL family behavior.
/// PT: Implementacao de dialeto SQL para MariaDB construida sobre o comportamento compartilhado da familia MySQL.
/// </summary>
internal sealed class MariaDbDialect : MySqlDialect
{
    internal const string DialectName = "mariadb";
    internal const int SequenceMinVersion = MariaDbDbVersions.Version10_3;
    internal const int ReturningMinVersion = MariaDbDbVersions.Version10_5;
    internal const int JsonTableMinVersion = MariaDbDbVersions.Version10_6;

    /// <summary>
    /// EN: Initializes the MariaDB dialect for the requested simulated version.
    /// PT: Inicializa o dialeto MariaDB para a versao simulada informada.
    /// </summary>
    /// <param name="version">EN: Simulated MariaDB version. PT: Versao simulada do MariaDB.</param>
    internal MariaDbDialect(int version)
        : base(DialectName, version)
    {
    }

    /// <inheritdoc />
    public override bool SupportsReturning => SupportsInsertReturning || SupportsDeleteReturning;

    /// <inheritdoc />
    public override bool SupportsInsertReturning => Version >= ReturningMinVersion;

    /// <inheritdoc />
    public override bool SupportsUpdateReturning => false;

    /// <inheritdoc />
    public override bool SupportsDeleteReturning => Version >= ReturningMinVersion;

    /// <inheritdoc />
    public override bool SupportsSequenceDdl => Version >= SequenceMinVersion;

    /// <inheritdoc />
    public override bool SupportsNextValueForSequenceExpression => Version >= SequenceMinVersion;

    /// <inheritdoc />
    public override bool SupportsPreviousValueForSequenceExpression => Version >= SequenceMinVersion;

    /// <inheritdoc />
    public override bool SupportsJsonTableFunction => Version >= JsonTableMinVersion;
}
