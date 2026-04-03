namespace DbSqlLikeMem.Oracle.TestTools;

/// <summary>
/// EN: Provides Oracle-specific SQL snippets used by the shared benchmark and fidelity helpers.
/// PT: Fornece trechos SQL especificos de Oracle usados pelos helpers compartilhados de benchmark e fidelidade.
/// </summary>
public sealed class OracleProviderSqlDialect : ProviderSqlDialect
{
    private static string NormalizeScenarioTableName(string tableName)
    {
        if (string.IsNullOrWhiteSpace(tableName))
            return tableName;

        return tableName.Trim().ToLowerInvariant();
    }

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
    public override string CreateTemporaryUsersTable(string tableName) =>
        $@"
CREATE GLOBAL TEMPORARY TABLE {TemporaryUsersTableName(tableName)} (
    Id NUMBER(10),
    Name VARCHAR2(100)
) ON COMMIT PRESERVE ROWS";

    /// <inheritdoc />
    public override string CreateUsersTable(string tableName, string uId) =>
        $@"
CREATE TABLE {NormalizeScenarioTableName(tableName)}_{uId} (
    Id NUMBER(10) PRIMARY KEY,
    Name VARCHAR2(100) NOT NULL,
    Email VARCHAR2(150) NULL,
    IsActive NUMBER(1) DEFAULT 1 NOT NULL,
    Age NUMBER(5) NULL,
    Balance NUMBER(12,2) DEFAULT 0.00 NOT NULL,
    CreatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    UpdatedAt TIMESTAMP NULL,
    ProfileJson CLOB NULL,
    CONSTRAINT CK_{NormalizeScenarioTableName(tableName)}_{uId}_ProfileJson CHECK (ProfileJson IS JSON)
)";

    /// <inheritdoc />
    public override string CreateOrdersTable(string tableName, string usersTableName, string uId) =>
        $@"
CREATE TABLE {NormalizeScenarioTableName(tableName)}_{uId} (
    Id NUMBER(10) PRIMARY KEY,
    userid NUMBER(10) NOT NULL,
    Note VARCHAR2(100) NOT NULL,
    OrderNumber VARCHAR2(40) NOT NULL,
    Amount NUMBER(12,2) DEFAULT 0.00 NOT NULL,
    Quantity NUMBER(10) DEFAULT 1 NOT NULL,
    IsPaid NUMBER(1) DEFAULT 0 NOT NULL,
    OrderedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    DeliveredAt TIMESTAMP NULL,
    ExtraJson CLOB NULL,
    CONSTRAINT CK_{NormalizeScenarioTableName(tableName)}_{uId}_ExtraJson CHECK (ExtraJson IS JSON),
    CONSTRAINT FK_{NormalizeScenarioTableName(tableName)}_{uId}_{NormalizeScenarioTableName(usersTableName)}_{uId} FOREIGN KEY (userid) REFERENCES {NormalizeScenarioTableName(usersTableName)}_{uId}(Id)
)";

    /// <inheritdoc />
    public override string DropTable(string tableName, string uId)
    {
        return $"DROP TABLE {NormalizeScenarioTableName(tableName)}_{uId}";
    }

    /// <inheritdoc />
    public override string DropTemporaryUsersTable(string tableName) =>
        $"DROP TABLE {TemporaryUsersTableName(tableName)}";

    /// <inheritdoc />
    public override string Parameter(string name) =>
        $":{name}";

    /// <inheritdoc />
    public override string SelectParameterProjection(string projectionList) =>
        $"SELECT {projectionList} FROM DUAL";

    /// <inheritdoc />
    public override string InsertUser(string tableName, int id, string name) =>
        $@"INSERT INTO {NormalizeScenarioTableName(tableName)} (
    Id,
    Name
) VALUES (
    {id},
    '{name}'
)";

    /// <inheritdoc />
    public override string InsertUsers(string tableName, params (int id, string name)[] values) =>
        $@"INSERT INTO {NormalizeScenarioTableName(tableName)} (
    Id,
    Name
) VALUES {string.Join(",",
            values.Select(_ =>
                $"({_.id}, '{_.name}')"))}";

    /// <inheritdoc />
    public override string InsertOrder(
        string tableName,
        string usersTableName,
        int id,
        int userId,
        string note,
        string orderNumber,
        decimal amount,
        int quantity,
        bool isPaid,
        string orderedAtLiteral) =>
        $"INSERT INTO {NormalizeScenarioTableName(tableName)} (Id, userid, Note, OrderNumber, Amount, Quantity, IsPaid, OrderedAt) VALUES ({id}, {userId}, '{note}', '{orderNumber}', {amount.ToString("0.##", System.Globalization.CultureInfo.InvariantCulture)}, {quantity}, {(isPaid ? "1" : "0")}, {orderedAtLiteral})";

    /// <inheritdoc />
    public override string SelectUserNameById(string tableName, int id) =>
        $"select name from {NormalizeScenarioTableName(tableName)} where Id = {id}";

    /// <inheritdoc />
    public override string CountJoinForUser(string usersTable, string ordersTable, int userId) =>
        $"SELECT COUNT(*) FROM {NormalizeScenarioTableName(usersTable)} u INNER JOIN {NormalizeScenarioTableName(ordersTable)} o ON o.userid = u.Id WHERE u.Id = {userId}";

    /// <inheritdoc />
    public override string UpdateUserNameById(string tableName, int id, string newName) =>
        $"UPDATE {NormalizeScenarioTableName(tableName)} SET Name = '{newName}' WHERE Id = {id}";

    /// <inheritdoc />
    public override string DeleteUserById(string tableName, int id) =>
        $"DELETE FROM {NormalizeScenarioTableName(tableName)} WHERE Id = {id}";

    /// <inheritdoc />
    public override string CountRows(string tableName) =>
        $"SELECT COUNT(*) FROM {NormalizeScenarioTableName(tableName)}";

    /// <inheritdoc />
    public override string DateScalar() =>
        "SELECT CURRENT_TIMESTAMP FROM DUAL";

    /// <inheritdoc />
    public override string StringAggregate(string tableName) =>
        $"SELECT LISTAGG(Name, ',') WITHIN GROUP (ORDER BY Name) FROM {NormalizeScenarioTableName(tableName)}";

    /// <inheritdoc />
    public override string Upsert(string tableName, int id, string newName) => $@"
MERGE INTO {NormalizeScenarioTableName(tableName)} target
USING (SELECT {id} Id, '{newName}' Name FROM DUAL) source
ON (target.Id = source.Id)
WHEN MATCHED THEN UPDATE SET target.Name = source.Name
WHEN NOT MATCHED THEN INSERT (Id, Name) VALUES (source.Id, source.Name)";

    /// <inheritdoc />
    public override string CreateSequence(string sequenceName) =>
        $"CREATE SEQUENCE {sequenceName} START WITH 10 INCREMENT BY 1";

    /// <inheritdoc />
    public override string NextSequenceValue(string sequenceName) =>
        $"SELECT {sequenceName}.NEXTVAL FROM DUAL";

    /// <inheritdoc />
    public override string NextSequenceValueExpression(string sequenceName) =>
        $"{sequenceName}.NEXTVAL";

    /// <inheritdoc />
    public override string CurrentSequenceValue(string sequenceName) =>
        $"SELECT {sequenceName}.CURRVAL FROM DUAL";

    /// <inheritdoc />
    public override string StringAggregateOrdered(string tableName) =>
        $"SELECT LISTAGG(Name, ',') WITHIN GROUP (ORDER BY Name) FROM {NormalizeScenarioTableName(tableName)}";

    /// <inheritdoc />
    public override string JsonScalarRead(string jsonLiteral) =>
        $"SELECT JSON_VALUE('{jsonLiteral}', '$.name') FROM DUAL";

    /// <inheritdoc />
    public override string StringAggregateDistinct(string tableName) =>
        $"SELECT LISTAGG(Name, ',') WITHIN GROUP (ORDER BY Name) FROM (SELECT DISTINCT Name FROM {NormalizeScenarioTableName(tableName)}) t";

    /// <inheritdoc />
    public override string StringAggregateCustomSeparator(string tableName, string separator) =>
        $"SELECT LISTAGG(Name, '{separator}') WITHIN GROUP (ORDER BY Name) FROM {NormalizeScenarioTableName(tableName)}";

    /// <inheritdoc />
    public override string StringAggregateLargeGroup(string tableName) =>
        $"SELECT LISTAGG(Name, ',') WITHIN GROUP (ORDER BY Name) FROM {NormalizeScenarioTableName(tableName)}";

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
    public override string CteSimple(string tableName) =>
        "SELECT 1 FROM DUAL";

    /// <inheritdoc />
    public override string TemporalNowWhere(string tableName) =>
        $"SELECT COUNT(*) FROM {NormalizeScenarioTableName(tableName)} WHERE CURRENT_TIMESTAMP IS NOT NULL";

    /// <inheritdoc />
    public override string TemporalNowOrderBy(string tableName) =>
        $"SELECT Name FROM (SELECT Name FROM {tableName} ORDER BY CURRENT_TIMESTAMP, Name) WHERE ROWNUM = 1";

    /// <summary>
    /// EN: Returns a valid Oracle no-op command for the release-savepoint benchmark.
    /// PT: Retorna um comando Oracle sem efeito valido para o benchmark de release-savepoint.
    /// </summary>
    public override string ReleaseSavepoint(string savepointName) =>
        $"RELEASE SAVEPOINT {savepointName}";

    /// <inheritdoc />
    public override string CrossApplyProjection(string usersTable, string ordersTable) =>
        $"SELECT COUNT(*) FROM {usersTable} u JOIN LATERAL (SELECT o.Note FROM {ordersTable} o WHERE o.{usersTable}Id = u.Id ORDER BY o.Id DESC FETCH FIRST 1 ROW ONLY) x ON 1 = 1";

    /// <inheritdoc />
    public override string OuterApplyProjection(string usersTable, string ordersTable) =>
        $"SELECT COUNT(*) FROM {usersTable} u LEFT JOIN LATERAL (SELECT o.Note FROM {ordersTable} o WHERE o.{usersTable}Id = u.Id ORDER BY o.Id DESC FETCH FIRST 1 ROW ONLY) x ON 1 = 1";
}
