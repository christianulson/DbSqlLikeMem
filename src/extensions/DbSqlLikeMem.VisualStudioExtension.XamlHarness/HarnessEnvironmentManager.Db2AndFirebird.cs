using System;
using System.Diagnostics;
using System.Data.Common;

namespace DbSqlLikeMem.VisualStudioExtension.XamlHarness;

internal sealed partial class HarnessEnvironmentManager
{
    private static async partial Task SeedDb2Async(DbConnection dbConnection, CancellationToken cancellationToken)
    {
        var schemaName = await ResolveDb2SchemaNameAsync(dbConnection, cancellationToken).ConfigureAwait(false);
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
""", cancellationToken, "create Db2 customers table").ConfigureAwait(false);

        await TryExecuteNonQueryAsync(dbConnection, $"""
CREATE TABLE {ordersTable} (
    id INT NOT NULL PRIMARY KEY,
    customer_id INT NOT NULL,
    total_amount DECIMAL(18,2) NOT NULL
)
""", cancellationToken, "create Db2 orders table").ConfigureAwait(false);

        await TryExecuteNonQueryAsync(dbConnection, $"CREATE INDEX ix_xh_orders_customer_id ON {ordersTable} (customer_id)", cancellationToken, "create Db2 index").ConfigureAwait(false);
        await TryExecuteNonQueryAsync(dbConnection, $"ALTER TABLE {ordersTable} ADD CONSTRAINT fk_xh_orders_customer FOREIGN KEY (customer_id) REFERENCES {customersTable} (id)", cancellationToken, "create Db2 foreign key").ConfigureAwait(false);

        await InsertSampleDataAsync(dbConnection, customersTable, ordersTable, cancellationToken).ConfigureAwait(false);
        await TryExecuteNonQueryAsync(dbConnection, $"CREATE SEQUENCE {sequenceName} START WITH 100 INCREMENT BY 1", cancellationToken, "create Db2 sequence").ConfigureAwait(false);

        await TryExecuteNonQueryAsync(dbConnection, $"""
CREATE OR REPLACE VIEW {viewName} AS
SELECT c.id AS customer_id
     , c.name AS customer_name
     , COUNT(o.id) AS order_count
     , COALESCE(SUM(o.total_amount), 0) AS total_amount
  FROM {customersTable} c
  LEFT JOIN {ordersTable} o ON o.customer_id = c.id
 GROUP BY c.id, c.name
""", cancellationToken, "create Db2 view").ConfigureAwait(false);

        await TryExecuteNonQueryAsync(dbConnection, $"CREATE OR REPLACE FUNCTION {functionName}(baseValue INT) RETURNS INT RETURN baseValue + 1", cancellationToken, "create Db2 function").ConfigureAwait(false);
        await TryExecuteNonQueryAsync(dbConnection, $"CREATE OR REPLACE PROCEDURE {procedureName}(IN p_value INT, OUT p_result INT) BEGIN END", cancellationToken, "create Db2 procedure").ConfigureAwait(false);
    }

    private static async partial Task CleanupDb2Async(DbConnection dbConnection, CancellationToken cancellationToken)
    {
        var schemaName = await ResolveDb2SchemaNameAsync(dbConnection, cancellationToken).ConfigureAwait(false);
        var customersTable = Qualify(schemaName, "xh_customers");
        var ordersTable = Qualify(schemaName, "xh_orders");
        var viewName = Qualify(schemaName, "xh_customer_totals_v");
        var functionName = Qualify(schemaName, "xh_add_one_fn");
        var procedureName = Qualify(schemaName, "xh_inc_proc");
        var sequenceName = Qualify(schemaName, "xh_order_seq");

        await TryExecuteNonQueryAsync(dbConnection, $"DROP VIEW {viewName}", cancellationToken, "drop Db2 view", ignoreDb2UndefinedName: true).ConfigureAwait(false);
        await TryExecuteNonQueryAsync(dbConnection, $"DROP PROCEDURE {procedureName}", cancellationToken, "drop Db2 procedure", ignoreDb2UndefinedName: true).ConfigureAwait(false);
        await TryExecuteNonQueryAsync(dbConnection, $"DROP FUNCTION {functionName}(INT)", cancellationToken, "drop Db2 function", ignoreDb2UndefinedName: true).ConfigureAwait(false);
        await TryExecuteNonQueryAsync(dbConnection, $"DROP SEQUENCE {sequenceName}", cancellationToken, "drop Db2 sequence", ignoreDb2UndefinedName: true).ConfigureAwait(false);
        await TryExecuteNonQueryAsync(dbConnection, $"DROP TABLE {ordersTable}", cancellationToken, "drop Db2 orders table", ignoreDb2UndefinedName: true).ConfigureAwait(false);
        await TryExecuteNonQueryAsync(dbConnection, $"DROP TABLE {customersTable}", cancellationToken, "drop Db2 customers table", ignoreDb2UndefinedName: true).ConfigureAwait(false);
    }

    private static async Task<string> ResolveDb2SchemaNameAsync(DbConnection dbConnection, CancellationToken cancellationToken)
    {
        try
        {
            using var command = dbConnection.CreateCommand();
            command.CommandText = "VALUES CURRENT SCHEMA";
            var value = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
            var schemaName = Convert.ToString(value)?.Trim();
            if (!string.IsNullOrWhiteSpace(schemaName))
            {
                return schemaName!;
            }
        }
        catch (Exception ex)
        {
            Trace.WriteLine(BuildExceptionReport("DbSqlLikeMem XAML harness could not resolve Db2 current schema", ex));
        }

        return "BENCH";
    }

    private static async partial Task SeedFirebirdAsync(DbConnection dbConnection, CancellationToken cancellationToken)
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
""", cancellationToken, "create Firebird customers table").ConfigureAwait(false);

        await TryExecuteNonQueryAsync(dbConnection, $"""
CREATE TABLE {ordersTable} (
    id INT NOT NULL PRIMARY KEY,
    customer_id INT NOT NULL,
    total_amount DECIMAL(18,2) NOT NULL
)
""", cancellationToken, "create Firebird orders table").ConfigureAwait(false);

        await TryExecuteNonQueryAsync(dbConnection, $"CREATE INDEX ix_xh_orders_customer_id ON {ordersTable} (customer_id)", cancellationToken, "create Firebird index").ConfigureAwait(false);
        await TryExecuteNonQueryAsync(dbConnection, $"ALTER TABLE {ordersTable} ADD CONSTRAINT fk_xh_orders_customer FOREIGN KEY (customer_id) REFERENCES {customersTable} (id)", cancellationToken, "create Firebird foreign key").ConfigureAwait(false);

        await InsertSampleDataAsync(dbConnection, customersTable, ordersTable, cancellationToken).ConfigureAwait(false);
        await TryExecuteNonQueryAsync(dbConnection, $"CREATE SEQUENCE {sequenceName}", cancellationToken, "create Firebird sequence").ConfigureAwait(false);

        await TryExecuteNonQueryAsync(dbConnection, $"""
CREATE VIEW {viewName} AS
SELECT c.id AS customer_id
     , c.name AS customer_name
     , COUNT(o.id) AS order_count
     , COALESCE(SUM(o.total_amount), 0) AS total_amount
  FROM {customersTable} c
  LEFT JOIN {ordersTable} o ON o.customer_id = c.id
 GROUP BY c.id, c.name
""", cancellationToken, "create Firebird view").ConfigureAwait(false);

        await TryExecuteNonQueryAsync(dbConnection, $"CREATE FUNCTION {functionName}(baseValue INT) RETURNS INT AS BEGIN RETURN baseValue + 1; END", cancellationToken, "create Firebird function").ConfigureAwait(false);
        await TryExecuteNonQueryAsync(dbConnection, $"""
CREATE OR ALTER PROCEDURE {procedureName}(tenantId INT)
RETURNS (tenantEcho INT)
AS
BEGIN
    tenantEcho = tenantId + 1;
    SUSPEND;
END
""", cancellationToken, "create Firebird procedure").ConfigureAwait(false);
    }

    private static async partial Task CleanupFirebirdAsync(DbConnection dbConnection, CancellationToken cancellationToken)
    {
        string schemaName = string.Empty;
        var customersTable = Qualify(schemaName, "xh_customers");
        var ordersTable = Qualify(schemaName, "xh_orders");
        var viewName = Qualify(schemaName, "xh_customer_totals_v");
        var functionName = Qualify(schemaName, "xh_add_one_fn");
        var procedureName = Qualify(schemaName, "xh_inc_proc");
        var sequenceName = Qualify(schemaName, "xh_order_seq");

        await TryExecuteNonQueryAsync(dbConnection, $"DROP VIEW {viewName}", cancellationToken, "drop Firebird view").ConfigureAwait(false);
        await TryExecuteNonQueryAsync(dbConnection, $"DROP PROCEDURE {procedureName}", cancellationToken, "drop Firebird procedure").ConfigureAwait(false);
        await TryExecuteNonQueryAsync(dbConnection, $"DROP FUNCTION {functionName}", cancellationToken, "drop Firebird function").ConfigureAwait(false);
        await TryExecuteNonQueryAsync(dbConnection, $"DROP SEQUENCE {sequenceName}", cancellationToken, "drop Firebird sequence").ConfigureAwait(false);
        await TryExecuteNonQueryAsync(dbConnection, $"DROP TABLE {ordersTable}", cancellationToken, "drop Firebird orders table").ConfigureAwait(false);
        await TryExecuteNonQueryAsync(dbConnection, $"DROP TABLE {customersTable}", cancellationToken, "drop Firebird customers table").ConfigureAwait(false);
    }
}
