namespace DbSqlLikeMem.Db2.TestTools;

/// <summary>
/// EN: Provides DB2-specific SQL snippets used by the shared benchmark and fidelity helpers.
/// PT: Fornece trechos SQL especificos de DB2 usados pelos helpers compartilhados de benchmark e fidelidade.
/// </summary>
public sealed class Db2ProviderSqlDialect : ProviderSqlDialect
{
    /// <inheritdoc />
    public override ProviderId Provider => ProviderId.Db2;

    /// <inheritdoc />
    public override string DisplayName => "DB2";

    /// <inheritdoc />
    public override bool SupportsUpsert => true;

    /// <inheritdoc />
    public override bool SupportsSequence => true;

    /// <inheritdoc />
    public override bool SupportsGroupByOrdinal => false;

    /// <inheritdoc />
    public override bool SupportsGuidInputOutputParameters => false;

    /// <inheritdoc />
    public override string CreateUsersTable(FidelityTestContext context) =>
        $@"
CREATE TABLE {context.TbUsersFullName} (
    Id INT NOT NULL PRIMARY KEY,
    Name VARCHAR(100) NOT NULL,
    Email VARCHAR(150) NULL,
    IsActive BOOLEAN NOT NULL DEFAULT TRUE,
    Age SMALLINT NULL,
    Balance DECIMAL(12,2) NOT NULL DEFAULT 0.00,
    CreatedAt TIMESTAMP NOT NULL DEFAULT CURRENT TIMESTAMP,
    UpdatedAt TIMESTAMP NULL,
    BirthDate DATE NULL,
    ProfileJson CLOB(2000) NULL,
    FixedCode CHAR(4) NULL,
    BigCount BIGINT NULL,
    PrecisionValue DECIMAL(18,4) NULL,
    DoubleValue DOUBLE NULL,
    GuidValue CHAR(36) NULL,
    BinaryValue VARBINARY(16) NULL,
    TimeValue TIME NULL,
    DateTimeOffsetValue VARCHAR(40) NULL
)";

    /// <inheritdoc />
    public override string CreateOrdersTable(FidelityTestContext context) =>
        $@"
CREATE TABLE {context.TbOrdersFullName} (
    Id INT NOT NULL PRIMARY KEY,
    {context.TbUsers}Id INT NOT NULL,
    Note VARCHAR(100) NOT NULL,
    OrderNumber VARCHAR(40) NOT NULL,
    Amount DECIMAL(12,2) NOT NULL DEFAULT 0.00,
    Quantity INT NOT NULL DEFAULT 1,
    IsPaid BOOLEAN NOT NULL DEFAULT FALSE,
    OrderedAt TIMESTAMP NOT NULL DEFAULT CURRENT TIMESTAMP,
    DeliveredAt TIMESTAMP NULL,
    ExtraJson CLOB(2000) NULL,
    CONSTRAINT FK_{context.TbOrdersFullName}_{context.TbUsersFullName} FOREIGN KEY ({context.TbUsers}Id) REFERENCES {context.TbUsersFullName}(Id)
);
CREATE INDEX IX_{context.TbOrdersFullName}_{context.TbUsers}Id ON {context.TbOrdersFullName} ({context.TbUsers}Id);
CREATE UNIQUE INDEX UX_{context.TbOrdersFullName}_OrderNumber ON {context.TbOrdersFullName} (OrderNumber)";

    /// <inheritdoc />
    public override string CreateTemporaryUsersTable(FidelityTestContext context) => $@"
DECLARE GLOBAL TEMPORARY TABLE SESSION.{TemporaryUsersTableName(context)} (
    Id INT,
    Name VARCHAR(100),
    TenantId INT
) ON COMMIT PRESERVE ROWS NOT LOGGED";

    /// <inheritdoc />
    public override string DropTemporaryUsersTable(FidelityTestContext context) =>
        $"DROP TABLE SESSION.{TemporaryUsersTableName(context)}";

    /// <inheritdoc />
    public override string InsertUser(FidelityTestContext context, int id, string name) =>
        $"INSERT INTO {context.TbUsersFullName} (Id, Name, IsActive, Balance, CreatedAt) VALUES ({id}, '{name}', TRUE, 0.00, CURRENT TIMESTAMP)";
    /// <inheritdoc />
    public override string InsertUsers(FidelityTestContext context, params (int id, string name)[] values) =>
        $"INSERT INTO {context.TbUsersFullName} (Id, Name, IsActive, Balance, CreatedAt) VALUES {string.Join(",", values.Select(_ => $"({_.id}, '{_.name}', TRUE, 0.00, CURRENT TIMESTAMP)"))}";

    /// <inheritdoc />
    public override string InsertOrder(
        FidelityTestContext context,
        int id,
        int userId,
        string note,
        string orderNumber,
        decimal amount,
        int quantity,
        bool isPaid,
        string orderedAtLiteral) =>
        $"INSERT INTO {context.TbOrdersFullName} (Id, {context.TbUsers}Id, Note, OrderNumber, Amount, Quantity, IsPaid, OrderedAt) VALUES ({id}, {userId}, '{note}', '{orderNumber}', {amount.ToString("0.##", System.Globalization.CultureInfo.InvariantCulture)}, {quantity}, {(isPaid ? "TRUE" : "FALSE")}, {orderedAtLiteral})";

    /// <inheritdoc />
    public override string SelectUserNameById(FidelityTestContext context, int id) =>
        $"SELECT Name FROM {context.TbUsersFullName} WHERE Id = {id}";
    /// <inheritdoc />
    public override string Parameter(string name) =>
        "?";

    /// <inheritdoc />
    public override string CommandParameter(string name) =>
        "@" + name;

    /// <inheritdoc />
    protected override bool TryCreateSpecialParameter(DbCommand command, string name, DbType dbType, object? value, out DbParameter parameter)
    {
        if (dbType == DbType.Currency)
        {
            parameter = CreateDb2CurrencyParameter(command, CommandParameter(name), value);
            return true;
        }

        parameter = null!;
        return false;
    }

    /// <inheritdoc />
    public override void AddParameter(DbCommand command, string name, DbType dbType, object? value)
    {
        if (TryCreateSpecialParameter(command, name, dbType, value, out var specialParameter))
        {
            AddParameterToCollection(command, specialParameter);
            return;
        }

        var parameter = command.CreateParameter();
        parameter.ParameterName = CommandParameter(name);
        ConfigureParameter(parameter, dbType);
        parameter.Value = NormalizeParameterValue(dbType, value) ?? DBNull.Value;
        ApplyParameterSize(parameter, parameter.Value);

        AddParameterToCollection(command, parameter);
    }

    /// <inheritdoc />
    protected override bool TryCreateSpecialParameter(DbCommand command, string name, DbType dbType, object? value, ParameterDirection direction, out DbParameter parameter)
    {
        if (dbType == DbType.Currency)
        {
            parameter = CreateDb2CurrencyParameter(command, CommandParameter(name), value, direction);
            return true;
        }

        parameter = null!;
        return false;
    }

    /// <inheritdoc />
    protected override void ConfigureParameter(DbParameter parameter, DbType dbType)
    {
        if (dbType == DbType.Guid
            || dbType == DbType.DateTimeOffset
            || dbType == DbType.Time
            || dbType == DbType.DateTime
            || dbType == DbType.DateTime2)
        {
            parameter.DbType = DbType.String;
            return;
        }

        parameter.DbType = dbType;
    }

    /// <inheritdoc />
    protected override object? NormalizeParameterValue(DbType dbType, object? value) =>
        NormalizeDb2ParameterValue(dbType, value);

    /// <inheritdoc />
    protected override void ApplyParameterSize(DbParameter parameter, object? value) =>
        SetDb2ParameterSize(parameter, value);

    /// <inheritdoc />
    public override string SelectParameterProjection(string projectionList) =>
        $"SELECT {projectionList} FROM SYSIBM.SYSDUMMY1";

    /// <inheritdoc />
    public override string SelectScalarSubquery(FidelityTestContext context) =>
        $"SELECT (SELECT COUNT(*) FROM {context.TbOrdersFullName} o WHERE o.{context.TbUsers}Id = 1) FROM SYSIBM.SYSDUMMY1";

    /// <inheritdoc />
    public override string CountJoinForUser(FidelityTestContext context, int userId) =>
        $"SELECT COUNT(*) FROM {context.TbUsersFullName} u INNER JOIN {context.TbOrdersFullName} o ON o.{context.TbUsers}Id = u.Id WHERE u.Id = {userId}";

    /// <inheritdoc />
    public override string UpdateUserNameById(FidelityTestContext context, int id, string newName) =>
        $"UPDATE {context.TbUsersFullName} SET Name = '{newName}' WHERE Id = {id}";
    /// <inheritdoc />
    public override string DeleteUserById(FidelityTestContext context, int id) =>
        $"DELETE FROM {context.TbUsersFullName} WHERE Id = {id}";

    /// <inheritdoc />
    public override string CountRows(string tableName) =>
        $"SELECT COUNT(*) FROM {tableName}";

    /// <inheritdoc />
    public override string DateScalar() =>
        "SELECT CURRENT TIMESTAMP FROM SYSIBM.SYSDUMMY1";

    /// <inheritdoc />
    public override string StringAggregate(FidelityTestContext context) =>
        $"SELECT LISTAGG(Name, ',') WITHIN GROUP (ORDER BY Name) FROM {context.TbUsersFullName}";

    /// <inheritdoc />
    public override string Upsert(FidelityTestContext context, int id, string newName) =>
        $@"
MERGE INTO {context.TbUsersFullName} target
USING (VALUES ({id}, '{newName}')) source (Id, Name)
ON target.Id = source.Id
WHEN MATCHED THEN
    UPDATE SET Name = source.Name
WHEN NOT MATCHED THEN
    INSERT (Id, Name) VALUES (source.Id, source.Name)";

    /// <inheritdoc />
    public override string CreateSequence(FidelityTestContext context) =>
        $"CREATE SEQUENCE {context.Seq} START WITH 10 INCREMENT BY 1";

    /// <inheritdoc />
    public override string NextSequenceValue(FidelityTestContext context) =>
        $"VALUES NEXT VALUE FOR {context.Seq}";
    /// <inheritdoc />
    public override string NextSequenceValueExpression(FidelityTestContext context) =>
        $"NEXT VALUE FOR {context.Seq}";

    /// <inheritdoc />
    public override string SelectNextSequenceValue(FidelityTestContext context) =>
        $"SELECT NEXT VALUE FOR {context.Seq} AS SeqValue FROM SYSIBM.SYSDUMMY1";
    /// <inheritdoc />
    public override string CurrentSequenceValue(FidelityTestContext context) =>
        $"VALUES PREVIOUS VALUE FOR {context.Seq}";

    /// <inheritdoc />
    public override string Savepoint(string savepointName) =>
        $"SAVEPOINT {savepointName} ON ROLLBACK RETAIN CURSORS";

    /// <inheritdoc />
    public override string StringAggregateOrdered(FidelityTestContext context) =>
        $"SELECT LISTAGG(Name, ',') WITHIN GROUP (ORDER BY Name) FROM {context.TbUsersFullName}";

    /// <inheritdoc />
    public override bool SupportsJsonScalarRead => true;

    /// <inheritdoc />
    public override string JsonScalarRead(string jsonLiteral) =>
        $"VALUES JSON_VALUE('{jsonLiteral}', 'strict $.name')";

    /// <inheritdoc />
    public override string StringAggregateDistinct(FidelityTestContext context  ) =>
        $"SELECT LISTAGG(Name, ',') WITHIN GROUP (ORDER BY Name) FROM (SELECT DISTINCT Name FROM {context.TbUsersFullName}) t";

    /// <inheritdoc />
    public override string StringAggregateCustomSeparator(FidelityTestContext context, string separator) =>
        $"SELECT LISTAGG(Name, '{separator}') WITHIN GROUP (ORDER BY Name) FROM {context.TbUsersFullName}";
    /// <inheritdoc />
    public override string StringAggregateLargeGroup(FidelityTestContext context) =>
        $"SELECT LISTAGG(Name, ',') WITHIN GROUP (ORDER BY Name) FROM {context.TbUsersFullName}";

    /// <inheritdoc />
    public override string JsonPathRead(string jsonLiteral) =>
        $"VALUES JSON_VALUE('{jsonLiteral}', 'strict $.user.name')";

    /// <inheritdoc />
    public override string JsonProfileNameExpression(string jsonColumn) =>
        $"JSON_VALUE({jsonColumn}, 'strict $.profile.name')";

    /// <inheritdoc />
    public override string StringLengthExpression(string expression) =>
        $"LENGTH({expression})";

    /// <inheritdoc />
    public override string StringCastExpression(string expression, int length = 10) =>
        $"TO_CHAR({expression})";

    /// <inheritdoc />
    public override string TemporalDateAdd() =>
        "VALUES CURRENT TIMESTAMP + 1 DAY";

    /// <inheritdoc />
    public override string TemporalCurrentTimestampExpression() => "CURRENT TIMESTAMP";

    /// <inheritdoc />
    public override string TemporalDateAddExpression() =>
        "CURRENT TIMESTAMP + 1 DAY";

    /// <inheritdoc />
    public override string StringPrefixExpression(string expression, int length) =>
        $"SUBSTR({expression}, 1, {length})";

    /// <inheritdoc />
    public override string TemporalNowWhere(FidelityTestContext context) =>
        $"SELECT COUNT(*) FROM {context.TbUsersFullName} WHERE CURRENT TIMESTAMP IS NOT NULL";

    /// <inheritdoc />
    public override string TemporalNowOrderBy(FidelityTestContext context) =>
        $"SELECT Name FROM {context.TbUsersFullName} ORDER BY Name FETCH FIRST 1 ROW ONLY";

    /// <inheritdoc />
    public override string CrossApplyProjection(FidelityTestContext context) =>
        $"""
SELECT
    u.Id AS UserId,
    u.Name AS UserName,
    x.Note AS Note
FROM {context.TbUsersFullName} u
JOIN LATERAL (
    SELECT o.Note
    FROM {context.TbOrdersFullName} o
    WHERE o.{context.TbUsers}Id = u.Id
    ORDER BY o.Id DESC
    FETCH FIRST 1 ROW ONLY
) x ON 1 = 1
ORDER BY u.Id
""";

    /// <inheritdoc />
    public override string OuterApplyProjection(FidelityTestContext context) =>
        $"""
SELECT
    u.Id AS UserId,
    u.Name AS UserName,
    x.Note AS Note
FROM {context.TbUsersFullName} u
LEFT JOIN LATERAL (
    SELECT o.Note
    FROM {context.TbOrdersFullName} o
    WHERE o.{context.TbUsers}Id = u.Id
    ORDER BY o.Id DESC
    FETCH FIRST 1 ROW ONLY
) x ON 1 = 1
ORDER BY u.Id
""";
}
