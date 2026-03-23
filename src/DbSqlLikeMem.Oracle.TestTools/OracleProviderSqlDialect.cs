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
    public override string CreateTemporaryUsersTable(string tableName) =>
        $@"
CREATE GLOBAL TEMPORARY TABLE {TemporaryUsersTableName(tableName)} (
    Id NUMBER(10),
    Name VARCHAR2(100)
) ON COMMIT PRESERVE ROWS";

    /// <inheritdoc />
    public override string CreateUsersTable(string tableName, string uId) =>
        $@"
CREATE TABLE {tableName}_{uId} (
    Id NUMBER(10) PRIMARY KEY,
    Name VARCHAR2(100) NOT NULL,
    Email VARCHAR2(150) NULL,
    IsActive NUMBER(1) DEFAULT 1 NOT NULL,
    Age NUMBER(5) NULL,
    Balance NUMBER(12,2) DEFAULT 0.00 NOT NULL,
    CreatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    UpdatedAt TIMESTAMP NULL,
    ProfileJson CLOB NULL,
    CONSTRAINT CK_{tableName}_{uId}_ProfileJson CHECK (ProfileJson IS JSON)
)";

    /// <inheritdoc />
    public override string CreateOrdersTable(string tableName, string usersTableName, string uId) =>
        $@"
CREATE TABLE {tableName}_{uId} (
    Id NUMBER(10) PRIMARY KEY,
    {usersTableName}Id NUMBER(10) NOT NULL,
    Note VARCHAR2(100) NOT NULL,
    OrderNumber VARCHAR2(40) NOT NULL,
    Amount NUMBER(12,2) DEFAULT 0.00 NOT NULL,
    Quantity NUMBER(10) DEFAULT 1 NOT NULL,
    IsPaid NUMBER(1) DEFAULT 0 NOT NULL,
    OrderedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    DeliveredAt TIMESTAMP NULL,
    ExtraJson CLOB NULL,
    CONSTRAINT CK_{tableName}_{uId}_ExtraJson CHECK (ExtraJson IS JSON),
    CONSTRAINT FK_{tableName}_{uId}_{usersTableName} FOREIGN KEY ({usersTableName}Id) REFERENCES {usersTableName}(Id)
)";

    /// <inheritdoc />
    public override string DropTemporaryUsersTable(string tableName) =>
        $"DROP TABLE {TemporaryUsersTableName(tableName)}";

    /// <inheritdoc />
    public override string InsertUser(string tableName, int id, string name) =>
        $@"INSERT INTO {tableName} (
    Id,
    Name,
    Email,
    IsActive,
    Age,
    Balance,
    CreatedAt,
    UpdatedAt,
    ProfileJson
) VALUES (
    {id},
    '{name}',
    NULL,
    1,
    NULL,
    0.00,
    CURRENT_TIMESTAMP,
    NULL,
    NULL
)";

    /// <inheritdoc />
    public override string InsertUsers(string tableName, params (int id, string name)[] values) =>
        $@"INSERT INTO {tableName} (
    Id,
    Name,
    Email,
    IsActive,
    Age,
    Balance,
    CreatedAt,
    UpdatedAt,
    ProfileJson
) VALUES {string.Join(",",
            values.Select(_ =>
                $"({_.id}, '{_.name}', NULL, 1, NULL, 0.00, CURRENT_TIMESTAMP, NULL, NULL)"))}";

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
        $"INSERT INTO {tableName} (Id, {usersTableName}Id, Note, OrderNumber, Amount, Quantity, IsPaid, OrderedAt) VALUES ({id}, {userId}, '{note}', '{orderNumber}', {amount.ToString("0.##", System.Globalization.CultureInfo.InvariantCulture)}, {quantity}, {(isPaid ? "1" : "0")}, {orderedAtLiteral})";

    /// <inheritdoc />
    public override string SelectUserNameById(string tableName, int id) =>
        $"select name from {tableName} where id = {id}";

    /// <inheritdoc />
    public override string CountJoinForUser(string usersTable, string ordersTable, int userId) =>
        $"SELECT COUNT(*) FROM {usersTable} u INNER JOIN {ordersTable} o ON o.{usersTable}Id = u.Id WHERE u.Id = {userId}";

    /// <inheritdoc />
    public override string UpdateUserNameById(string tableName, int id, string newName) =>
        $"UPDATE {tableName} SET Name = '{newName}' WHERE Id = {id}";

    /// <inheritdoc />
    public override string DeleteUserById(string tableName, int id) =>
        $"DELETE FROM {tableName} WHERE Id = {id}";

    /// <inheritdoc />
    public override string CountRows(string tableName) =>
        $"SELECT COUNT(*) FROM {tableName}";

    /// <inheritdoc />
    public override string DateScalar() =>
        "SELECT CURRENT_TIMESTAMP FROM DUAL";

    /// <inheritdoc />
    public override string StringAggregate(string tableName) =>
        $"SELECT LISTAGG(Name, ',') WITHIN GROUP (ORDER BY Name) FROM {tableName}";

    /// <inheritdoc />
    public override string Upsert(string tableName, int id, string newName) => $@"
MERGE INTO {tableName} target
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
    public override string StringAggregateOrdered(string tableName) =>
        $"SELECT LISTAGG(Name, ',') WITHIN GROUP (ORDER BY Name) FROM {tableName}";

    /// <inheritdoc />
    public override string JsonScalarRead(string jsonLiteral) =>
        $"SELECT JSON_VALUE('{jsonLiteral}', '$.name') FROM DUAL";

    /// <inheritdoc />
    public override string StringAggregateDistinct(string tableName) =>
        $"SELECT LISTAGG(Name, ',') WITHIN GROUP (ORDER BY Name) FROM (SELECT DISTINCT Name FROM {tableName}) t";

    /// <inheritdoc />
    public override string StringAggregateCustomSeparator(string tableName, string separator) =>
        $"SELECT LISTAGG(Name, '{separator}') WITHIN GROUP (ORDER BY Name) FROM {tableName}";

    /// <inheritdoc />
    public override string StringAggregateLargeGroup(string tableName) =>
        $"SELECT LISTAGG(Name, ',') WITHIN GROUP (ORDER BY Name) FROM {tableName}";

    /// <inheritdoc />
    public override string JsonPathRead(string jsonLiteral) =>
        $"SELECT JSON_VALUE('{jsonLiteral}', '$.user.name') FROM DUAL";

    /// <inheritdoc />
    public override string TemporalDateAdd() =>
        "SELECT TO_TIMESTAMP('2024-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS') + INTERVAL '1' DAY FROM DUAL";

    /// <inheritdoc />
    public override string CteSimple(string tableName) =>
        "SELECT 1 FROM DUAL";

    /// <inheritdoc />
    public override string TemporalNowWhere(string tableName) =>
        $"SELECT COUNT(*) FROM {tableName} WHERE CURRENT_TIMESTAMP IS NOT NULL";

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
