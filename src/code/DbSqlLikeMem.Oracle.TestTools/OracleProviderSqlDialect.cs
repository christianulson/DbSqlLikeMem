namespace DbSqlLikeMem.Oracle.TestTools;

/// <summary>
/// EN: Provides Oracle-specific SQL snippets used by the shared benchmark and fidelity helpers.
/// PT: Fornece trechos SQL especificos de Oracle usados pelos helpers compartilhados de benchmark e fidelidade.
/// </summary>
public sealed class OracleProviderSqlDialect : ProviderSqlDialect
{
    /// <inheritdoc />
    public override ProviderId Provider => ProviderId.Oracle;

    /// <inheritdoc />
    public override string DisplayName => "Oracle";

    /// <inheritdoc />
    public override bool SupportsUpsert => true;

    /// <inheritdoc />
    public override bool SupportsSequence => true;

    /// <inheritdoc />
    public override bool SupportsJsonScalarRead => true;

    /// <inheritdoc />
    public override bool SupportsReleaseSavepoints => false;

    /// <inheritdoc />
    public override bool GlobalTemporaryTablesShareDefinitionAcrossConnections => true;

    /// <inheritdoc />
    public override bool GlobalTemporaryTablesShareRowsAcrossConnections => false;

    /// <inheritdoc />
    public override string CreateTemporaryUsersTable(FidelityTestContext context) => $@"
CREATE GLOBAL TEMPORARY TABLE {TemporaryUsersTableName(context)} (
    Id NUMBER(10),
    Name VARCHAR2(100),
    TenantId NUMBER(10)
) ON COMMIT PRESERVE ROWS";

    /// <inheritdoc />
    public override string CreateUsersTable(FidelityTestContext context) =>
        $@"
CREATE TABLE {context.TbUsersFullName} (
    Id NUMBER(10) PRIMARY KEY,
    Name VARCHAR2(100) NOT NULL,
    Email VARCHAR2(150) NULL,
    IsActive NUMBER(1) DEFAULT 1 NOT NULL,
    Age NUMBER(5) NULL,
    Balance NUMBER(12,2) DEFAULT 0.00 NOT NULL,
    CreatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    UpdatedAt TIMESTAMP NULL,
    BirthDate DATE NULL,
    ProfileJson CLOB NULL,
    FixedCode CHAR(4) NULL,
    BigCount NUMBER(19) NULL,
    PrecisionValue NUMBER(18,4) NULL,
    DoubleValue BINARY_DOUBLE NULL,
    GuidValue VARCHAR2(36) NULL,
    BinaryValue RAW(16) NULL,
    TimeValue VARCHAR2(32) NULL,
    DateTimeOffsetValue TIMESTAMP WITH TIME ZONE NULL,
    CONSTRAINT CK_{context.TbUsersFullName}_ProfileJson CHECK (ProfileJson IS JSON)
)";

    /// <inheritdoc />
    public override string CreateOrdersTable(FidelityTestContext context) => $@"
CREATE TABLE {context.TbOrdersFullName} (
    Id NUMBER(10) PRIMARY KEY,
    {context.TbUsers}Id NUMBER(10) NOT NULL,
    Note VARCHAR2(100) NOT NULL,
    OrderNumber VARCHAR2(40) NOT NULL,
    Amount NUMBER(12,2) DEFAULT 0.00 NOT NULL,
    Quantity NUMBER(10) DEFAULT 1 NOT NULL,
    IsPaid NUMBER(1) DEFAULT 0 NOT NULL,
    OrderedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    DeliveredAt TIMESTAMP NULL,
    ExtraJson CLOB NULL,
    CONSTRAINT CK_{context.TbOrdersFullName}_ExtraJson CHECK (ExtraJson IS JSON),
    CONSTRAINT FK_{context.TbOrdersFullName}_UserId FOREIGN KEY ({context.TbUsers}Id) REFERENCES {context.TbUsersFullName}(Id)
)";

    /// <inheritdoc />
    public override string DropTable(string tableName)
    {
        return $"DROP TABLE {tableName}";
    }

    /// <inheritdoc />
    public override string DropTemporaryUsersTable(FidelityTestContext context) =>
        $"DROP TABLE {TemporaryUsersTableName(context)}";

    /// <inheritdoc />
    public override string Parameter(string name) =>
        $":{name}";

    /// <inheritdoc />
    public override string JsonParameter(string name) =>
        $"TO_CLOB({Parameter(name)})";

    /// <inheritdoc />
    public override string SelectParameterProjection(string projectionList) =>
        $"SELECT {projectionList} FROM DUAL";

    /// <inheritdoc />
    public override string InsertUser(FidelityTestContext context, int id, string name) =>
        $@"INSERT INTO {context.TbUsersFullName} (
    Id,
    Name
) VALUES (
    {id},
    '{name}'
)";

    /// <inheritdoc />
    public override string InsertUsers(FidelityTestContext context, params (int id, string name)[] values) =>
        $@"INSERT INTO {context.TbUsersFullName} (
    Id,
    Name
) VALUES {string.Join(",",
            values.Select(_ =>
                $"({_.id}, '{_.name}')"))}";

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
        $"INSERT INTO {context.TbOrdersFullName} (Id, {context.TbUsers}Id, Note, OrderNumber, Amount, Quantity, IsPaid, OrderedAt) VALUES ({id}, {userId}, '{note}', '{orderNumber}', {amount.ToString("0.##", System.Globalization.CultureInfo.InvariantCulture)}, {quantity}, {(isPaid ? "1" : "0")}, {orderedAtLiteral})";

    /// <inheritdoc />
    public override string SelectUserNameById(FidelityTestContext context, int id) =>
        $"select name from {context.TbUsersFullName} where Id = {id}";
    /// <inheritdoc />
    public override string CountJoinForUser(FidelityTestContext context, int userId) =>
        $"SELECT COUNT(*) FROM {context.TbUsersFullName} u INNER JOIN {context.TbOrdersFullName} o ON o.{context.TbUsers}Id = u.Id WHERE u.Id = {userId}";

    /// <inheritdoc />
    public override string SelectExistsPredicate(FidelityTestContext context) =>
        $"SELECT COUNT(*) FROM {context.TbUsersFullName} u WHERE EXISTS (SELECT 1 FROM {context.TbOrdersFullName} o WHERE o.{context.TbUsers}Id = u.Id)";
    /// <inheritdoc />
    public override string SelectNotExistsPredicate(FidelityTestContext context) =>
        $"SELECT COUNT(*) FROM {context.TbUsersFullName} u WHERE NOT EXISTS (SELECT 1 FROM {context.TbOrdersFullName} o WHERE o.{context.TbUsers}Id = u.Id)";

    /// <inheritdoc />
    public override string SelectLeftJoinAntiJoin(FidelityTestContext context) =>
        $"SELECT COUNT(*) FROM {context.TbUsersFullName} u LEFT JOIN {context.TbOrdersFullName} o ON o.{context.TbUsers}Id = u.Id WHERE o.{context.TbUsers}Id IS NULL";

    /// <inheritdoc />
    public override string SelectCorrelatedCount(FidelityTestContext context) =>
        $"SELECT COUNT(*) FROM {context.TbUsersFullName} u WHERE (SELECT COUNT(*) FROM {context.TbOrdersFullName} o WHERE o.{context.TbUsers}Id = u.Id) > 0";

    /// <inheritdoc />
    public override string GroupByHaving(FidelityTestContext context) =>
        $"SELECT COUNT(*) FROM (SELECT u.Id FROM {context.TbUsersFullName} u INNER JOIN {context.TbOrdersFullName} o ON o.{context.TbUsers}Id = u.Id GROUP BY u.Id HAVING COUNT(*) >= 2) q";

    /// <inheritdoc />
    public override string MultiJoinAggregate(FidelityTestContext context) =>
        $"SELECT COUNT(*) FROM {context.TbUsersFullName} u INNER JOIN {context.TbOrdersFullName} o1 ON o1.{context.TbUsers}Id = u.Id INNER JOIN {context.TbOrdersFullName} o2 ON o2.{context.TbUsers}Id = u.Id WHERE u.Id = 1";

    /// <inheritdoc />
    public override string SelectScalarSubquery(FidelityTestContext context) =>
        $"SELECT (SELECT COUNT(*) FROM {context.TbOrdersFullName} o WHERE o.{context.TbUsers}Id = 1) FROM DUAL";
    /// <inheritdoc />
    public override string SelectInSubquery(FidelityTestContext context) =>
        $"SELECT COUNT(*) FROM {context.TbUsersFullName} WHERE Id IN (SELECT {context.TbUsers}Id FROM {context.TbOrdersFullName})";

    /// <inheritdoc />
    public override string SelectNotInSubquery(FidelityTestContext context) =>
        $"SELECT COUNT(*) FROM {context.TbUsersFullName} WHERE Id NOT IN (SELECT {context.TbUsers}Id FROM {context.TbOrdersFullName})";

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
        "SELECT CURRENT_TIMESTAMP FROM DUAL";

    /// <inheritdoc />
    public override string StringAggregate(FidelityTestContext context) =>
        $"SELECT LISTAGG(Name, ',') WITHIN GROUP (ORDER BY Name) FROM {context.TbUsersFullName}";

    /// <inheritdoc />
    public override string Upsert(FidelityTestContext context, int id, string newName) => $@"
MERGE INTO {context.TbUsersFullName} target
USING (SELECT {id} Id, '{newName}' Name FROM DUAL) source
ON (target.Id = source.Id)
WHEN MATCHED THEN UPDATE SET target.Name = source.Name
WHEN NOT MATCHED THEN INSERT (Id, Name) VALUES (source.Id, source.Name)";

    /// <inheritdoc />
    public override string CreateSequence(FidelityTestContext context) =>
        $"CREATE SEQUENCE {context.Seq} START WITH 10 INCREMENT BY 1";

    /// <inheritdoc />
    public override string NextSequenceValue(FidelityTestContext context) =>
        $"SELECT {context.Seq}.NEXTVAL FROM DUAL";

    /// <inheritdoc />
    public override string NextSequenceValueExpression(FidelityTestContext context) =>
        $"{context.Seq}.NEXTVAL";

    /// <inheritdoc />
    public override string CurrentSequenceValue(FidelityTestContext context) =>
        $"SELECT {context.Seq}.CURRVAL FROM DUAL";
    /// <inheritdoc />
    public override string StringAggregateOrdered(FidelityTestContext context) =>
        $"SELECT LISTAGG(Name, ',') WITHIN GROUP (ORDER BY Name) FROM {context.TbUsersFullName}";

    /// <inheritdoc />
    public override string JsonScalarRead(string jsonLiteral) =>
        $"SELECT JSON_VALUE('{jsonLiteral}', '$.name') FROM DUAL";

    /// <inheritdoc />
    public override string StringAggregateDistinct(FidelityTestContext context) =>
        $"SELECT LISTAGG(Name, ',') WITHIN GROUP (ORDER BY Name) FROM (SELECT DISTINCT Name FROM {context.TbUsersFullName}) t";

    /// <inheritdoc />
    public override string StringAggregateCustomSeparator(FidelityTestContext context, string separator) =>
        $"SELECT LISTAGG(Name, '{separator}') WITHIN GROUP (ORDER BY Name) FROM {context.TbUsersFullName}";

    /// <inheritdoc />
    public override string StringAggregateLargeGroup(FidelityTestContext context) =>
        $"SELECT LISTAGG(Name, ',') WITHIN GROUP (ORDER BY Name) FROM {context.TbUsersFullName}";

    /// <inheritdoc />
    public override string JsonPathRead(string jsonLiteral) =>
        $"SELECT JSON_VALUE('{jsonLiteral}', '$.user.name') FROM DUAL";

    /// <inheritdoc />
    public override string JsonProfileNameExpression(string jsonColumn) =>
        $"JSON_VALUE({jsonColumn}, '$.profile.name')";

    /// <inheritdoc />
    public override string StringLengthExpression(string expression) =>
        $"LENGTH({expression})";

    /// <inheritdoc />
    public override string StringCastExpression(string expression, int length = 10)
    {
        _ = length;
        return $"CAST({expression} AS VARCHAR2({length}))";
    }

    /// <inheritdoc />
    public override string TemporalDateAdd() =>
        "SELECT CAST(CURRENT_TIMESTAMP AS DATE) + 1 FROM DUAL";

    /// <inheritdoc />
    public override string TemporalCurrentTimestampExpression() => "CURRENT_TIMESTAMP";

    /// <inheritdoc />
    public override string TemporalDateAddExpression() =>
        "CAST(CURRENT_TIMESTAMP AS DATE) + 1";

    /// <inheritdoc />
    public override string StringPrefixExpression(string expression, int length) =>
        $"SUBSTR({expression}, 1, {length})";

    /// <inheritdoc />
    public override string CteSimple(FidelityTestContext context) =>
        "SELECT 1 FROM DUAL";

    /// <inheritdoc />
    public override string TemporalNowWhere(FidelityTestContext context) =>
        $"SELECT COUNT(*) FROM {context.TbUsersFullName} WHERE CURRENT_TIMESTAMP IS NOT NULL";

    /// <inheritdoc />
    public override string TemporalNowOrderBy(FidelityTestContext context) =>
        $"SELECT Name FROM {context.TbUsersFullName} ORDER BY CURRENT_TIMESTAMP, Name FETCH FIRST 1 ROW ONLY";

    /// <summary>
    /// EN: Returns a valid Oracle no-op command for the release-savepoint benchmark.
    /// PT: Retorna um comando Oracle sem efeito valido para o benchmark de release-savepoint.
    /// </summary>
    public override string ReleaseSavepoint(string savepointName) =>
        $"RELEASE SAVEPOINT {savepointName}";

    /// <inheritdoc />
    public override string CrossApplyProjection(FidelityTestContext context) =>
        $"SELECT COUNT(*) FROM {context.TbUsersFullName} u JOIN LATERAL (SELECT o.Note FROM {context.TbOrdersFullName} o WHERE o.{context.TbUsers}Id = u.Id ORDER BY o.Id DESC FETCH FIRST 1 ROW ONLY) x ON 1 = 1";

    /// <inheritdoc />
    public override string OuterApplyProjection(FidelityTestContext context) =>
        $"SELECT COUNT(*) FROM {context.TbUsersFullName} u LEFT JOIN LATERAL (SELECT o.Note FROM {context.TbOrdersFullName} o WHERE o.{context.TbUsers}Id = u.Id ORDER BY o.Id DESC FETCH FIRST 1 ROW ONLY) x ON 1 = 1";
}
