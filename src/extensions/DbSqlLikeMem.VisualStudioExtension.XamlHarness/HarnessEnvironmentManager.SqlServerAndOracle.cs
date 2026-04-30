using System.Data.Common;

namespace DbSqlLikeMem.VisualStudioExtension.XamlHarness;

internal sealed partial class HarnessEnvironmentManager
{
    private static async partial Task SeedSqlServerAsync(DbConnection dbConnection, CancellationToken cancellationToken)
    {
        const string schemaName = "dbo";
        var customersTable = Qualify(schemaName, "xh_customers");
        var ordersTable = Qualify(schemaName, "xh_orders");
        var viewName = Qualify(schemaName, "xh_customer_totals_v");
        var functionName = Qualify(schemaName, "xh_add_one_fn");
        var procedureName = Qualify(schemaName, "xh_inc_proc");
        var sequenceName = Qualify(schemaName, "xh_order_seq");

        await TryExecuteNonQueryAsync(dbConnection, $"""
CREATE TABLE {customersTable} (
    id INT NOT NULL PRIMARY KEY,
    name NVARCHAR(100) NOT NULL
);
""", cancellationToken, "create SqlServer customers table").ConfigureAwait(false);

        await TryExecuteNonQueryAsync(dbConnection, $"""
CREATE TABLE {ordersTable} (
    id INT NOT NULL PRIMARY KEY,
    customer_id INT NOT NULL,
    total_amount DECIMAL(18,2) NOT NULL
);
""", cancellationToken, "create SqlServer orders table").ConfigureAwait(false);

        await TryExecuteNonQueryAsync(dbConnection, $"CREATE INDEX ix_xh_orders_customer_id ON {ordersTable} (customer_id);", cancellationToken, "create SqlServer index").ConfigureAwait(false);
        await TryExecuteNonQueryAsync(dbConnection, $"ALTER TABLE {ordersTable} ADD CONSTRAINT fk_xh_orders_customer FOREIGN KEY (customer_id) REFERENCES {customersTable} (id);", cancellationToken, "create SqlServer foreign key").ConfigureAwait(false);

        await InsertSampleDataAsync(dbConnection, customersTable, ordersTable, cancellationToken).ConfigureAwait(false);
        await TryExecuteNonQueryAsync(dbConnection, $"CREATE SEQUENCE {sequenceName} AS BIGINT START WITH 100 INCREMENT BY 1;", cancellationToken, "create SqlServer sequence").ConfigureAwait(false);

        await TryExecuteNonQueryAsync(dbConnection, $"""
CREATE VIEW {viewName} AS
SELECT c.id AS customer_id
     , c.name AS customer_name
     , COUNT(o.id) AS order_count
     , COALESCE(SUM(o.total_amount), 0) AS total_amount
  FROM {customersTable} c
  LEFT JOIN {ordersTable} o ON o.customer_id = c.id
 GROUP BY c.id, c.name;
""", cancellationToken, "create SqlServer view").ConfigureAwait(false);

        await TryExecuteNonQueryAsync(dbConnection, $"IF OBJECT_ID(N'{functionName}', N'FN') IS NOT NULL DROP FUNCTION {functionName};", cancellationToken, "remove SqlServer function before create").ConfigureAwait(false);
        await TryExecuteNonQueryAsync(dbConnection, $"""
CREATE FUNCTION {functionName}(@baseValue INT)
RETURNS INT
AS
BEGIN
    RETURN @baseValue + 1;
END;
""", cancellationToken, "create SqlServer function").ConfigureAwait(false);

        await TryExecuteNonQueryAsync(dbConnection, $"IF OBJECT_ID(N'{procedureName}', N'P') IS NOT NULL DROP PROCEDURE {procedureName};", cancellationToken, "remove SqlServer procedure before create").ConfigureAwait(false);
        await TryExecuteNonQueryAsync(dbConnection, $"""
CREATE PROCEDURE {procedureName}
    @value INT,
    @result INT OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    SET @result = @value + 1;
END;
""", cancellationToken, "create SqlServer procedure").ConfigureAwait(false);
    }

    private static async partial Task CleanupSqlServerAsync(DbConnection dbConnection, CancellationToken cancellationToken)
    {
        const string schemaName = "dbo";
        var customersTable = Qualify(schemaName, "xh_customers");
        var ordersTable = Qualify(schemaName, "xh_orders");
        var viewName = Qualify(schemaName, "xh_customer_totals_v");
        var functionName = Qualify(schemaName, "xh_add_one_fn");
        var procedureName = Qualify(schemaName, "xh_inc_proc");
        var sequenceName = Qualify(schemaName, "xh_order_seq");

        await TryExecuteNonQueryAsync(dbConnection, $"IF OBJECT_ID(N'{viewName}', N'V') IS NOT NULL DROP VIEW {viewName};", cancellationToken, "drop SqlServer view").ConfigureAwait(false);
        await TryExecuteNonQueryAsync(dbConnection, $"IF OBJECT_ID(N'{procedureName}', N'P') IS NOT NULL DROP PROCEDURE {procedureName};", cancellationToken, "drop SqlServer procedure").ConfigureAwait(false);
        await TryExecuteNonQueryAsync(dbConnection, $"IF OBJECT_ID(N'{functionName}', N'FN') IS NOT NULL DROP FUNCTION {functionName};", cancellationToken, "drop SqlServer function").ConfigureAwait(false);
        await TryExecuteNonQueryAsync(dbConnection, $"IF OBJECT_ID(N'{sequenceName}', N'SO') IS NOT NULL DROP SEQUENCE {sequenceName};", cancellationToken, "drop SqlServer sequence").ConfigureAwait(false);
        await TryExecuteNonQueryAsync(dbConnection, $"IF OBJECT_ID(N'{ordersTable}', N'U') IS NOT NULL DROP TABLE {ordersTable};", cancellationToken, "drop SqlServer orders table").ConfigureAwait(false);
        await TryExecuteNonQueryAsync(dbConnection, $"IF OBJECT_ID(N'{customersTable}', N'U') IS NOT NULL DROP TABLE {customersTable};", cancellationToken, "drop SqlServer customers table").ConfigureAwait(false);
    }

    private static async partial Task SeedOracleAsync(DbConnection dbConnection, CancellationToken cancellationToken)
    {
        const string schemaName = "benchmark";
        var customersTable = Qualify(schemaName, "xh_customers");
        var ordersTable = Qualify(schemaName, "xh_orders");
        var viewName = Qualify(schemaName, "xh_customer_totals_v");
        var functionName = Qualify(schemaName, "xh_add_one_fn");
        var procedureName = Qualify(schemaName, "xh_inc_proc");
        var sequenceName = Qualify(schemaName, "xh_order_seq");

        await TryExecuteNonQueryAsync(dbConnection, $"""
CREATE TABLE {customersTable} (
    id NUMBER(10) NOT NULL PRIMARY KEY,
    name VARCHAR2(100) NOT NULL
)
""", cancellationToken, "create Oracle customers table").ConfigureAwait(false);

        await TryExecuteNonQueryAsync(dbConnection, $"""
CREATE TABLE {ordersTable} (
    id NUMBER(10) NOT NULL PRIMARY KEY,
    customer_id NUMBER(10) NOT NULL,
    total_amount NUMBER(18,2) NOT NULL
)
""", cancellationToken, "create Oracle orders table").ConfigureAwait(false);

        await TryExecuteNonQueryAsync(dbConnection, $"CREATE INDEX ix_xh_orders_customer_id ON {ordersTable} (customer_id)", cancellationToken, "create Oracle index").ConfigureAwait(false);
        await TryExecuteNonQueryAsync(dbConnection, $"ALTER TABLE {ordersTable} ADD CONSTRAINT fk_xh_orders_customer FOREIGN KEY (customer_id) REFERENCES {customersTable} (id)", cancellationToken, "create Oracle foreign key").ConfigureAwait(false);

        await InsertSampleDataAsync(dbConnection, customersTable, ordersTable, cancellationToken).ConfigureAwait(false);
        await TryExecuteNonQueryAsync(dbConnection, $"CREATE SEQUENCE {sequenceName} START WITH 100 INCREMENT BY 1", cancellationToken, "create Oracle sequence").ConfigureAwait(false);

        await TryExecuteNonQueryAsync(dbConnection, $"""
CREATE OR REPLACE VIEW {viewName} AS
SELECT c.id AS customer_id
     , c.name AS customer_name
     , COUNT(o.id) AS order_count
     , COALESCE(SUM(o.total_amount), 0) AS total_amount
  FROM {customersTable} c
  LEFT JOIN {ordersTable} o ON o.customer_id = c.id
 GROUP BY c.id, c.name
""", cancellationToken, "create Oracle view").ConfigureAwait(false);

        await TryExecuteNonQueryAsync(dbConnection, $"DROP FUNCTION {functionName}", cancellationToken, "remove Oracle function before create", ignoreOracleObjectNotFound: true).ConfigureAwait(false);
        await TryExecuteNonQueryAsync(dbConnection, $"""
CREATE OR REPLACE FUNCTION {functionName}(baseValue NUMBER)
RETURN NUMBER
IS
BEGIN
    RETURN baseValue + 1;
END;
""", cancellationToken, "create Oracle function").ConfigureAwait(false);

        await TryExecuteNonQueryAsync(dbConnection, $"DROP PROCEDURE {procedureName}", cancellationToken, "remove Oracle procedure before create", ignoreOracleObjectNotFound: true).ConfigureAwait(false);
        await TryExecuteNonQueryAsync(dbConnection, $"""
CREATE OR REPLACE PROCEDURE {procedureName}(p_value IN NUMBER, p_result OUT NUMBER)
AS
BEGIN
    p_result := p_value + 1;
END;
""", cancellationToken, "create Oracle procedure").ConfigureAwait(false);
    }

    private static async partial Task CleanupOracleAsync(DbConnection dbConnection, CancellationToken cancellationToken)
    {
        const string schemaName = "benchmark";
        var customersTable = Qualify(schemaName, "xh_customers");
        var ordersTable = Qualify(schemaName, "xh_orders");
        var viewName = Qualify(schemaName, "xh_customer_totals_v");
        var functionName = Qualify(schemaName, "xh_add_one_fn");
        var procedureName = Qualify(schemaName, "xh_inc_proc");
        var sequenceName = Qualify(schemaName, "xh_order_seq");

        await TryExecuteNonQueryAsync(dbConnection, $"DROP VIEW {viewName}", cancellationToken, "drop Oracle view", ignoreOracleObjectNotFound: true).ConfigureAwait(false);
        await TryExecuteNonQueryAsync(dbConnection, $"DROP PROCEDURE {procedureName}", cancellationToken, "drop Oracle procedure", ignoreOracleObjectNotFound: true).ConfigureAwait(false);
        await TryExecuteNonQueryAsync(dbConnection, $"DROP FUNCTION {functionName}", cancellationToken, "drop Oracle function", ignoreOracleObjectNotFound: true).ConfigureAwait(false);
        await TryExecuteNonQueryAsync(dbConnection, $"DROP SEQUENCE {sequenceName}", cancellationToken, "drop Oracle sequence", ignoreOracleObjectNotFound: true).ConfigureAwait(false);
        await TryExecuteNonQueryAsync(dbConnection, $"DROP TABLE {ordersTable}", cancellationToken, "drop Oracle orders table", ignoreOracleObjectNotFound: true).ConfigureAwait(false);
        await TryExecuteNonQueryAsync(dbConnection, $"DROP TABLE {customersTable}", cancellationToken, "drop Oracle customers table", ignoreOracleObjectNotFound: true).ConfigureAwait(false);
    }
}
