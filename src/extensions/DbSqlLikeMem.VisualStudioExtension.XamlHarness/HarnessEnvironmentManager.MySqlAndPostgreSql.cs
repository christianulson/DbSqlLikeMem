using FirebirdSql.Data.FirebirdClient;
using System.Data.Common;

namespace DbSqlLikeMem.VisualStudioExtension.XamlHarness;

internal sealed partial class HarnessEnvironmentManager
{
    private static async partial Task InsertSampleDataAsync(DbConnection dbConnection, string customersTable, string ordersTable, CancellationToken cancellationToken)
    {
        if (dbConnection is FbConnection)
        {
            await InsertSampleDataRowByRowAsync(dbConnection, customersTable, ordersTable, cancellationToken).ConfigureAwait(false);
            return;
        }

        await TryExecuteNonQueryAsync(dbConnection, $"""
INSERT INTO {customersTable} (id, name) VALUES
    (1, 'Alice'),
    (2, 'Bruno'),
    (3, 'Carla')
""", cancellationToken, "insert sample customers").ConfigureAwait(false);

        await TryExecuteNonQueryAsync(dbConnection, $"""
INSERT INTO {ordersTable} (id, customer_id, total_amount) VALUES
    (1001, 1, 120.50),
    (1002, 1, 80.00),
    (1003, 2, 19.99),
    (1004, 3, 45.00)
""", cancellationToken, "insert sample orders").ConfigureAwait(false);
    }

    private static async Task InsertSampleDataRowByRowAsync(DbConnection dbConnection, string customersTable, string ordersTable, CancellationToken cancellationToken)
    {
        await TryExecuteNonQueryAsync(dbConnection, $"INSERT INTO {customersTable} (id, name) VALUES (1, 'Alice')", cancellationToken, "insert sample customer").ConfigureAwait(false);
        await TryExecuteNonQueryAsync(dbConnection, $"INSERT INTO {customersTable} (id, name) VALUES (2, 'Bruno')", cancellationToken, "insert sample customer").ConfigureAwait(false);
        await TryExecuteNonQueryAsync(dbConnection, $"INSERT INTO {customersTable} (id, name) VALUES (3, 'Carla')", cancellationToken, "insert sample customer").ConfigureAwait(false);

        await TryExecuteNonQueryAsync(dbConnection, $"INSERT INTO {ordersTable} (id, customer_id, total_amount) VALUES (1001, 1, 120.50)", cancellationToken, "insert sample order").ConfigureAwait(false);
        await TryExecuteNonQueryAsync(dbConnection, $"INSERT INTO {ordersTable} (id, customer_id, total_amount) VALUES (1002, 1, 80.00)", cancellationToken, "insert sample order").ConfigureAwait(false);
        await TryExecuteNonQueryAsync(dbConnection, $"INSERT INTO {ordersTable} (id, customer_id, total_amount) VALUES (1003, 2, 19.99)", cancellationToken, "insert sample order").ConfigureAwait(false);
        await TryExecuteNonQueryAsync(dbConnection, $"INSERT INTO {ordersTable} (id, customer_id, total_amount) VALUES (1004, 3, 45.00)", cancellationToken, "insert sample order").ConfigureAwait(false);
    }

    private static async partial Task SeedMySqlFamilyAsync(DbConnection dbConnection, bool includeSequence, CancellationToken cancellationToken)
    {
        string schemaName = string.Empty;
        var customersTable = Qualify(schemaName, "xh_customers");
        var ordersTable = Qualify(schemaName, "xh_orders");
        var viewName = Qualify(schemaName, "xh_customer_totals_v");
        var functionName = Qualify(schemaName, "xh_add_one_fn");
        var procedureName = Qualify(schemaName, "xh_inc_proc");
        var sequenceName = Qualify(schemaName, "xh_order_seq");

        await TryExecuteNonQueryAsync(dbConnection, $"""
CREATE TABLE {customersTable} (
    id INT NOT NULL PRIMARY KEY,
    name VARCHAR(100) NOT NULL
)
""", cancellationToken, "create MySql-family customers table").ConfigureAwait(false);

        await TryExecuteNonQueryAsync(dbConnection, $"""
CREATE TABLE {ordersTable} (
    id INT NOT NULL PRIMARY KEY,
    customer_id INT NOT NULL,
    total_amount DECIMAL(18,2) NOT NULL
)
""", cancellationToken, "create MySql-family orders table").ConfigureAwait(false);

        await TryExecuteNonQueryAsync(dbConnection, $"CREATE INDEX ix_xh_orders_customer_id ON {ordersTable} (customer_id)", cancellationToken, "create MySql-family index").ConfigureAwait(false);
        await TryExecuteNonQueryAsync(dbConnection, $"ALTER TABLE {ordersTable} ADD CONSTRAINT fk_xh_orders_customer FOREIGN KEY (customer_id) REFERENCES {customersTable} (id)", cancellationToken, "create MySql-family foreign key").ConfigureAwait(false);

        await InsertSampleDataAsync(dbConnection, customersTable, ordersTable, cancellationToken).ConfigureAwait(false);

        await TryExecuteNonQueryAsync(dbConnection, $"""
CREATE VIEW {viewName} AS
SELECT c.id AS customer_id
     , c.name AS customer_name
     , COUNT(o.id) AS order_count
     , COALESCE(SUM(o.total_amount), 0) AS total_amount
  FROM {customersTable} c
  LEFT JOIN {ordersTable} o ON o.customer_id = c.id
 GROUP BY c.id, c.name
""", cancellationToken, "create MySql-family view").ConfigureAwait(false);

        await TryExecuteNonQueryAsync(dbConnection, $"DROP FUNCTION IF EXISTS {functionName}", cancellationToken, "remove MySql-family function before create").ConfigureAwait(false);
        await TryExecuteNonQueryAsync(dbConnection, $"""
CREATE FUNCTION {functionName}(base_value INT)
RETURNS INT
DETERMINISTIC
NO SQL
RETURN base_value + 1
""", cancellationToken, "create MySql-family function").ConfigureAwait(false);

        await TryExecuteNonQueryAsync(dbConnection, $"DROP PROCEDURE IF EXISTS {procedureName}", cancellationToken, "remove MySql-family procedure before create").ConfigureAwait(false);
        await TryExecuteNonQueryAsync(dbConnection, $"""
CREATE PROCEDURE {procedureName}(IN p_value INT, OUT p_result INT)
BEGIN
    SET p_result = p_value + 1;
END
""", cancellationToken, "create MySql-family procedure").ConfigureAwait(false);

        if (includeSequence)
        {
            await TryExecuteNonQueryAsync(dbConnection, $"CREATE SEQUENCE {sequenceName} START WITH 100 INCREMENT BY 1", cancellationToken, "create MariaDb sequence").ConfigureAwait(false);
        }
    }

    private static async partial Task CleanupMySqlFamilyAsync(DbConnection dbConnection, bool includeSequence, CancellationToken cancellationToken)
    {
        string schemaName = string.Empty;
        await TryExecuteNonQueryAsync(dbConnection, $"DROP VIEW IF EXISTS {Qualify(schemaName, "xh_customer_totals_v")}", cancellationToken, "drop MySql-family view").ConfigureAwait(false);
        await TryExecuteNonQueryAsync(dbConnection, $"DROP PROCEDURE IF EXISTS {Qualify(schemaName, "xh_inc_proc")}", cancellationToken, "drop MySql-family procedure").ConfigureAwait(false);
        await TryExecuteNonQueryAsync(dbConnection, $"DROP FUNCTION IF EXISTS {Qualify(schemaName, "xh_add_one_fn")}", cancellationToken, "drop MySql-family function").ConfigureAwait(false);

        if (includeSequence)
        {
            await TryExecuteNonQueryAsync(dbConnection, $"DROP SEQUENCE IF EXISTS {Qualify(schemaName, "xh_order_seq")}", cancellationToken, "drop MariaDb sequence").ConfigureAwait(false);
        }

        await TryExecuteNonQueryAsync(dbConnection, $"DROP TABLE IF EXISTS {Qualify(schemaName, "xh_orders")}", cancellationToken, "drop MySql-family orders table").ConfigureAwait(false);
        await TryExecuteNonQueryAsync(dbConnection, $"DROP TABLE IF EXISTS {Qualify(schemaName, "xh_customers")}", cancellationToken, "drop MySql-family customers table").ConfigureAwait(false);
    }

    private static async partial Task SeedPostgreSqlAsync(DbConnection dbConnection, CancellationToken cancellationToken)
    {
        const string schemaName = "public";
        var customersTable = Qualify(schemaName, "xh_customers");
        var ordersTable = Qualify(schemaName, "xh_orders");
        var viewName = Qualify(schemaName, "xh_customer_totals_v");
        var functionName = Qualify(schemaName, "xh_add_one_fn");
        var procedureName = Qualify(schemaName, "xh_inc_proc");
        var sequenceName = Qualify(schemaName, "xh_order_seq");

        await TryExecuteNonQueryAsync(dbConnection, $"""
CREATE TABLE {customersTable} (
    id INT NOT NULL PRIMARY KEY,
    name VARCHAR(100) NOT NULL
)
""", cancellationToken, "create PostgreSql customers table").ConfigureAwait(false);

        await TryExecuteNonQueryAsync(dbConnection, $"""
CREATE TABLE {ordersTable} (
    id INT NOT NULL PRIMARY KEY,
    customer_id INT NOT NULL,
    total_amount DECIMAL(18,2) NOT NULL
)
""", cancellationToken, "create PostgreSql orders table").ConfigureAwait(false);

        await TryExecuteNonQueryAsync(dbConnection, $"CREATE INDEX ix_xh_orders_customer_id ON {ordersTable} (customer_id)", cancellationToken, "create PostgreSql index").ConfigureAwait(false);
        await TryExecuteNonQueryAsync(dbConnection, $"ALTER TABLE {ordersTable} ADD CONSTRAINT fk_xh_orders_customer FOREIGN KEY (customer_id) REFERENCES {customersTable} (id)", cancellationToken, "create PostgreSql foreign key").ConfigureAwait(false);

        await InsertSampleDataAsync(dbConnection, customersTable, ordersTable, cancellationToken).ConfigureAwait(false);
        await TryExecuteNonQueryAsync(dbConnection, $"CREATE SEQUENCE {sequenceName} START WITH 100 INCREMENT BY 1", cancellationToken, "create PostgreSql sequence").ConfigureAwait(false);

        await TryExecuteNonQueryAsync(dbConnection, $"""
CREATE VIEW {viewName} AS
SELECT c.id AS customer_id
     , c.name AS customer_name
     , COUNT(o.id) AS order_count
     , COALESCE(SUM(o.total_amount), 0) AS total_amount
  FROM {customersTable} c
  LEFT JOIN {ordersTable} o ON o.customer_id = c.id
 GROUP BY c.id, c.name
""", cancellationToken, "create PostgreSql view").ConfigureAwait(false);

        await TryExecuteNonQueryAsync(dbConnection, $"DROP FUNCTION IF EXISTS {functionName}(integer)", cancellationToken, "remove PostgreSql function before create").ConfigureAwait(false);
        await TryExecuteNonQueryAsync(dbConnection, $"""
CREATE FUNCTION {functionName}(base_value integer)
RETURNS integer
LANGUAGE SQL
AS 'SELECT base_value + 1';
""", cancellationToken, "create PostgreSql function").ConfigureAwait(false);

        await TryExecuteNonQueryAsync(dbConnection, $"DROP PROCEDURE IF EXISTS {procedureName}(integer)", cancellationToken, "remove PostgreSql procedure before create").ConfigureAwait(false);
        await TryExecuteNonQueryAsync(dbConnection, $"""
CREATE PROCEDURE {procedureName}(IN base_value integer)
LANGUAGE plpgsql
AS $$
BEGIN
    NULL;
END;
$$;
""", cancellationToken, "create PostgreSql procedure").ConfigureAwait(false);
    }

    private static async partial Task CleanupPostgreSqlAsync(DbConnection dbConnection, CancellationToken cancellationToken)
    {
        const string schemaName = "public";
        var customersTable = Qualify(schemaName, "xh_customers");
        var ordersTable = Qualify(schemaName, "xh_orders");
        var viewName = Qualify(schemaName, "xh_customer_totals_v");
        var functionName = Qualify(schemaName, "xh_add_one_fn");
        var procedureName = Qualify(schemaName, "xh_inc_proc");
        var sequenceName = Qualify(schemaName, "xh_order_seq");

        await TryExecuteNonQueryAsync(dbConnection, $"DROP VIEW IF EXISTS {viewName}", cancellationToken, "drop PostgreSql view").ConfigureAwait(false);
        await TryExecuteNonQueryAsync(dbConnection, $"DROP PROCEDURE IF EXISTS {procedureName}(integer)", cancellationToken, "drop PostgreSql procedure").ConfigureAwait(false);
        await TryExecuteNonQueryAsync(dbConnection, $"DROP FUNCTION IF EXISTS {functionName}(integer)", cancellationToken, "drop PostgreSql function").ConfigureAwait(false);
        await TryExecuteNonQueryAsync(dbConnection, $"DROP SEQUENCE IF EXISTS {sequenceName}", cancellationToken, "drop PostgreSql sequence").ConfigureAwait(false);
        await TryExecuteNonQueryAsync(dbConnection, $"DROP TABLE IF EXISTS {ordersTable}", cancellationToken, "drop PostgreSql orders table").ConfigureAwait(false);
        await TryExecuteNonQueryAsync(dbConnection, $"DROP TABLE IF EXISTS {customersTable}", cancellationToken, "drop PostgreSql customers table").ConfigureAwait(false);
    }
}
