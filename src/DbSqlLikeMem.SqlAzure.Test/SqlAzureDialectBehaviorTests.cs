namespace DbSqlLikeMem.SqlAzure.Test;

/// <summary>
/// EN: Covers SQL Azure dialect behaviors inherited from SQL Server semantics.
/// PT: Cobre comportamentos de dialeto do SQL Azure herdados da semântica do SQL Server.
/// </summary>
public sealed class SqlAzureDialectBehaviorTests
{
    private static SqlAzureConnectionMock CreateOpenConnection(int? compatibilityLevel = null)
    {
        var db = new SqlAzureDbMock(compatibilityLevel);
        db.AddTable("Users", [
            new("Id", DbType.Int32, false),
            new("Name", DbType.String, false),
            new("Email", DbType.String, true)
        ]);
        db.AddTable("Orders", [
            new("OrderId", DbType.Int32, false),
            new("UserId", DbType.Int32, false),
            new("Amount", DbType.Decimal, false, decimalPlaces:2)
        ]);

        var connection = new SqlAzureConnectionMock(db);
        connection.Open();
        return connection;
    }

    /// <summary>
    /// EN: Ensures SQL Server-style table hints remain accepted through SQL Azure provider mocks.
    /// PT: Garante que table hints no estilo SQL Server continuem aceitos pelos mocks do provedor SQL Azure.
    /// </summary>
    [Fact]
    public void Select_WithSqlServerTableHints_ShouldExecute()
    {
        using var connection = CreateOpenConnection();
        using var seed = new SqlAzureCommandMock(connection)
        {
            CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (10, 'Hint User', 'hint@example.com')"
        };
        seed.ExecuteNonQuery();

        using var command = new SqlAzureCommandMock(connection)
        {
            CommandText = "SELECT Name FROM Users WITH (NOLOCK, INDEX([IX_Users_Name])) WHERE Id = 10"
        };

        var name = command.ExecuteScalar();
        Assert.Equal("Hint User", name);
    }

    /// <summary>
    /// EN: Ensures TOP and @@ROWCOUNT semantics behave consistently for SQL Azure compatibility tests.
    /// PT: Garante que a semântica de TOP e @@ROWCOUNT se comporte de forma consistente para testes de compatibilidade SQL Azure.
    /// </summary>
    [Fact]
    public void Select_TopThenRowCount_ShouldReturnLastSelectCount()
    {
        using var connection = CreateOpenConnection();
        using (var seed = new SqlAzureCommandMock(connection))
        {
            seed.CommandText = """
                INSERT INTO Users (Id, Name, Email) VALUES (101, 'Ana', NULL);
                INSERT INTO Users (Id, Name, Email) VALUES (102, 'Bia', NULL);
                INSERT INTO Users (Id, Name, Email) VALUES (103, 'Caio', NULL);
                """;
            seed.ExecuteNonQuery();
        }

        using var command = new SqlAzureCommandMock(connection)
        {
            CommandText = "SELECT TOP 1 Name FROM Users ORDER BY Id; SELECT @@ROWCOUNT;"
        };

        using var reader = command.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal("Ana", reader.GetString(0));
        Assert.False(reader.Read());
        Assert.True(reader.NextResult());
        Assert.True(reader.Read());
        Assert.Equal(1L, Convert.ToInt64(reader.GetValue(0), CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Ensures INSERT OUTPUT returns inserted projection through SQL Azure mocks.
    /// PT: Garante que INSERT OUTPUT retorne a projeção inserida nos mocks SQL Azure.
    /// </summary>
    [Fact]
    public void ExecuteReader_InsertOutput_ShouldReturnInsertedProjection()
    {
        using var connection = CreateOpenConnection();
        using var command = new SqlAzureCommandMock(connection)
        {
            CommandText = "INSERT INTO Users (Id, Name, Email) OUTPUT inserted.Id, inserted.Name AS user_name VALUES (701, 'Output Insert', 'insert@test.local')"
        };

        using var reader = command.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal(701, reader.GetInt32(reader.GetOrdinal("Id")));
        Assert.Equal("Output Insert", reader.GetString(reader.GetOrdinal("user_name")));
        Assert.False(reader.Read());
    }

    /// <summary>
    /// EN: Ensures SQL Azure compatibility mode executes an Azure SQL reference query with CTE, JOIN, LEFT JOIN, CROSS APPLY, OUTER APPLY, EXISTS, STRING_AGG, DATEADD, DATEDIFF, CASE, CAST and ROW_NUMBER.
    /// PT: Garante que o modo de compatibilidade SQL Azure execute uma query de referencia do Azure SQL com CTE, JOIN, LEFT JOIN, CROSS APPLY, OUTER APPLY, EXISTS, STRING_AGG, DATEADD, DATEDIFF, CASE, CAST e ROW_NUMBER.
    /// </summary>
    [Theory]
    [MemberDataSqlAzureCompatibilityLevel]
    public void ExecuteReader_ProviderSignatureComplexQuery_ShouldReturnExpectedRows(int compatibilityLevel)
    {
        using var connection = CreateOpenConnection(compatibilityLevel);
        using (var seed = new SqlAzureCommandMock(connection))
        {
            seed.CommandText = """
                INSERT INTO Users (Id, Name, Email) VALUES (811, 'Ana', NULL);
                INSERT INTO Users (Id, Name, Email) VALUES (812, 'Bia', NULL);
                INSERT INTO Users (Id, Name, Email) VALUES (813, 'Caio', NULL);
                INSERT INTO Orders (OrderId, UserId, Amount) VALUES (9101, 811, 10.50);
                INSERT INTO Orders (OrderId, UserId, Amount) VALUES (9102, 811, 5.25);
                INSERT INTO Orders (OrderId, UserId, Amount) VALUES (9103, 812, 7.75);
                """;
            seed.ExecuteNonQuery();
        }

        using var command = new SqlAzureCommandMock(connection)
        {
            CommandText = """
                WITH base_users AS (
                    SELECT u.Id,
                           u.Name,
                           CASE
                               WHEN u.Id IN (811, 812) THEN 10
                               ELSE 20
                           END AS TenantId
                    FROM Users u
                ),
                tenant_scope AS (
                    SELECT 10 AS TenantId
                    UNION ALL
                    SELECT 20
                ),
                order_rollup AS (
                    SELECT o.UserId,
                           COUNT(*) AS OrderCount,
                           SUM(CAST(o.Amount AS DECIMAL(10,2))) AS TotalAmount,
                           STRING_AGG(CAST(o.OrderId AS NVARCHAR(20)), '|') WITHIN GROUP (ORDER BY o.OrderId DESC) AS OrderIds
                    FROM Orders o
                    GROUP BY o.UserId
                ),
                latest_orders AS (
                    SELECT bu.Id,
                           bu.Name,
                           bu.TenantId,
                           calc.NormalizedId,
                           calc.SnapshotPlusOneDay,
                           calc.DaysFromAnchor,
                           calc.UserCode,
                           ISNULL(rollup.OrderCount, CAST(0 AS INT)) AS OrderCount,
                           ISNULL(rollup.TotalAmount, CAST(0 AS DECIMAL(10,2))) AS TotalAmount,
                           ISNULL(rollup.OrderIds, CAST('' AS NVARCHAR(20))) AS OrderIds,
                           latest.LastOrderId,
                           ISNULL(latest.LastAmount, CAST(0 AS DECIMAL(10,2))) AS LastAmount,
                           CASE
                               WHEN EXISTS (SELECT 1 FROM Orders ox WHERE ox.UserId = bu.Id AND ox.Amount >= CAST(10 AS DECIMAL(10,2))) THEN CAST(1 AS BIT)
                               ELSE CAST(0 AS BIT)
                           END AS HasBigOrder,
                           ROW_NUMBER() OVER (
                               PARTITION BY bu.TenantId
                               ORDER BY ISNULL(latest.LastAmount, CAST(0 AS DECIMAL(10,2))) DESC, bu.Id
                           ) AS Rn
                    FROM base_users bu
                    JOIN tenant_scope scope ON scope.TenantId = bu.TenantId
                    LEFT JOIN order_rollup rollup ON rollup.UserId = bu.Id
                    CROSS APPLY (
                        SELECT CAST(bu.Id AS INT) AS NormalizedId,
                               DATEADD(DAY, 1, CURRENT_TIMESTAMP) AS SnapshotPlusOneDay,
                               DATEDIFF(DAY, CAST('2020-01-01' AS DATETIME), CAST('2020-01-01' AS DATETIME) + (bu.Id - 811)) AS DaysFromAnchor,
                               CONCAT(CAST(bu.TenantId AS NVARCHAR(10)), '-', CAST(bu.Id AS NVARCHAR(10))) AS UserCode
                    ) calc
                    OUTER APPLY (
                        SELECT TOP 1
                               CAST(o.OrderId AS INT) AS LastOrderId,
                               CAST(o.Amount AS DECIMAL(10,2)) AS LastAmount
                        FROM Orders o
                        WHERE o.UserId = bu.Id
                        ORDER BY o.OrderId DESC
                    ) latest
                )
                SELECT Id, Name, TenantId, NormalizedId, SnapshotPlusOneDay, DaysFromAnchor, UserCode, OrderCount, TotalAmount, OrderIds, LastOrderId, LastAmount, HasBigOrder, Rn
                FROM latest_orders
                ORDER BY TenantId, Rn, Id
                """
        };

        if (compatibilityLevel < SqlAzureDbCompatibilityLevels.SqlServer2017)
        {
            var ex = Assert.Throws<NotSupportedException>(() => command.ExecuteReader());
            Assert.Contains("STRING_AGG", ex.Message, StringComparison.OrdinalIgnoreCase);
            return;
        }

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(812, reader.GetInt32(reader.GetOrdinal("Id")));
        Assert.Equal("Bia", reader.GetString(reader.GetOrdinal("Name")));
        Assert.Equal(10, reader.GetInt32(reader.GetOrdinal("TenantId")));
        Assert.Equal(812, reader.GetInt32(reader.GetOrdinal("NormalizedId")));
        Assert.NotNull(reader.GetValue(reader.GetOrdinal("SnapshotPlusOneDay")));
        Assert.Equal(1, Convert.ToInt32(reader.GetValue(reader.GetOrdinal("DaysFromAnchor"))));
        Assert.Equal("10-812", Convert.ToString(reader.GetValue(reader.GetOrdinal("UserCode"))));
        Assert.Equal(1, Convert.ToInt32(reader.GetValue(reader.GetOrdinal("OrderCount"))));
        Assert.Equal(7.75m, Convert.ToDecimal(reader.GetValue(reader.GetOrdinal("TotalAmount"))));
        Assert.Equal("9103", Convert.ToString(reader.GetValue(reader.GetOrdinal("OrderIds"))));
        Assert.Equal(9103, Convert.ToInt32(reader.GetValue(reader.GetOrdinal("LastOrderId"))));
        Assert.Equal(7.75m, Convert.ToDecimal(reader.GetValue(reader.GetOrdinal("LastAmount"))));
        Assert.False(Convert.ToBoolean(reader.GetValue(reader.GetOrdinal("HasBigOrder"))));
        Assert.Equal(1L, Convert.ToInt64(reader.GetValue(reader.GetOrdinal("Rn"))));

        Assert.True(reader.Read());
        Assert.Equal(811, reader.GetInt32(reader.GetOrdinal("Id")));
        Assert.Equal("Ana", reader.GetString(reader.GetOrdinal("Name")));
        Assert.Equal(10, reader.GetInt32(reader.GetOrdinal("TenantId")));
        Assert.Equal(811, reader.GetInt32(reader.GetOrdinal("NormalizedId")));
        Assert.NotNull(reader.GetValue(reader.GetOrdinal("SnapshotPlusOneDay")));
        Assert.Equal(0, Convert.ToInt32(reader.GetValue(reader.GetOrdinal("DaysFromAnchor"))));
        Assert.Equal("10-811", Convert.ToString(reader.GetValue(reader.GetOrdinal("UserCode"))));
        Assert.Equal(2, Convert.ToInt32(reader.GetValue(reader.GetOrdinal("OrderCount"))));
        Assert.Equal(15.75m, Convert.ToDecimal(reader.GetValue(reader.GetOrdinal("TotalAmount"))));
        Assert.Equal("9102|9101", Convert.ToString(reader.GetValue(reader.GetOrdinal("OrderIds"))));
        Assert.Equal(9102, Convert.ToInt32(reader.GetValue(reader.GetOrdinal("LastOrderId"))));
        Assert.Equal(5.25m, Convert.ToDecimal(reader.GetValue(reader.GetOrdinal("LastAmount"))));
        Assert.True(Convert.ToBoolean(reader.GetValue(reader.GetOrdinal("HasBigOrder"))));
        Assert.Equal(2L, Convert.ToInt64(reader.GetValue(reader.GetOrdinal("Rn"))));

        Assert.True(reader.Read());
        Assert.Equal(813, reader.GetInt32(reader.GetOrdinal("Id")));
        Assert.Equal("Caio", reader.GetString(reader.GetOrdinal("Name")));
        Assert.Equal(20, reader.GetInt32(reader.GetOrdinal("TenantId")));
        Assert.Equal(813, reader.GetInt32(reader.GetOrdinal("NormalizedId")));
        Assert.NotNull(reader.GetValue(reader.GetOrdinal("SnapshotPlusOneDay")));
        Assert.Equal(2, Convert.ToInt32(reader.GetValue(reader.GetOrdinal("DaysFromAnchor"))));
        Assert.Equal("20-813", Convert.ToString(reader.GetValue(reader.GetOrdinal("UserCode"))));
        Assert.Equal(0, Convert.ToInt32(reader.GetValue(reader.GetOrdinal("OrderCount"))));
        Assert.Equal(0m, Convert.ToDecimal(reader.GetValue(reader.GetOrdinal("TotalAmount"))));
        Assert.Equal(string.Empty, Convert.ToString(reader.GetValue(reader.GetOrdinal("OrderIds"))));
        Assert.True(reader.IsDBNull(reader.GetOrdinal("LastOrderId")));
        Assert.Equal(0m, Convert.ToDecimal(reader.GetValue(reader.GetOrdinal("LastAmount"))));
        Assert.False(Convert.ToBoolean(reader.GetValue(reader.GetOrdinal("HasBigOrder"))));
        Assert.Equal(1L, Convert.ToInt64(reader.GetValue(reader.GetOrdinal("Rn"))));
        Assert.False(reader.Read());
    }

    /// <summary>
    /// EN: Ensures UPDATE OUTPUT exposes deleted/inserted values for SQL Azure compatibility.
    /// PT: Garante que UPDATE OUTPUT exponha valores deleted/inserted para compatibilidade SQL Azure.
    /// </summary>
    [Fact]
    public void ExecuteReader_UpdateOutput_ShouldReturnDeletedAndInsertedValues()
    {
        using var connection = CreateOpenConnection();
        using (var setup = new SqlAzureCommandMock(connection)
        {
            CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (702, 'Before Update', 'before@test.local')"
        })
        {
            setup.ExecuteNonQuery();
        }

        using var command = new SqlAzureCommandMock(connection)
        {
            CommandText = "UPDATE Users SET Name = 'After Update' OUTPUT deleted.Name AS old_name, inserted.Name AS new_name WHERE Id = 702"
        };

        using var reader = command.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal("Before Update", reader.GetString(reader.GetOrdinal("old_name")));
        Assert.Equal("After Update", reader.GetString(reader.GetOrdinal("new_name")));
        Assert.False(reader.Read());
    }

    /// <summary>
    /// EN: Ensures DELETE OUTPUT returns deleted snapshot through SQL Azure mocks.
    /// PT: Garante que DELETE OUTPUT retorne o snapshot excluído nos mocks SQL Azure.
    /// </summary>
    [Fact]
    public void ExecuteReader_DeleteOutput_ShouldReturnDeletedSnapshot()
    {
        using var connection = CreateOpenConnection();
        using (var setup = new SqlAzureCommandMock(connection)
        {
            CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (703, 'To Delete', 'delete@test.local')"
        })
        {
            setup.ExecuteNonQuery();
        }

        using var command = new SqlAzureCommandMock(connection)
        {
            CommandText = "DELETE FROM Users OUTPUT deleted.Id, deleted.Name WHERE Id = 703"
        };

        using var reader = command.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal(703, reader.GetInt32(reader.GetOrdinal("Id")));
        Assert.Equal("To Delete", reader.GetString(reader.GetOrdinal("Name")));
        Assert.False(reader.Read());
        Assert.DoesNotContain(connection.GetTable("Users"), row => Convert.ToInt32(row[0], CultureInfo.InvariantCulture) == 703);
    }

    /// <summary>
    /// EN: Ensures SQL Azure compatibility mode executes CROSS APPLY with correlated derived subqueries through the shared SQL Server runtime path.
    /// PT: Garante que o modo de compatibilidade SQL Azure execute CROSS APPLY com subqueries derivadas correlacionadas pelo caminho compartilhado do SQL Server.
    /// </summary>
    [Fact]
    public void ExecuteReader_CrossApply_WithCorrelatedDerivedSubquery_ShouldReturnMatchingRows()
    {
        using var connection = CreateOpenConnection();
        using (var seed = new SqlAzureCommandMock(connection))
        {
            seed.CommandText = """
                INSERT INTO Users (Id, Name, Email) VALUES (801, 'Ana', NULL);
                INSERT INTO Users (Id, Name, Email) VALUES (802, 'Bia', NULL);
                INSERT INTO Users (Id, Name, Email) VALUES (803, 'Caio', NULL);
                INSERT INTO Orders (OrderId, UserId, Amount) VALUES (9001, 801, 10.50);
                INSERT INTO Orders (OrderId, UserId, Amount) VALUES (9002, 801, 19.75);
                INSERT INTO Orders (OrderId, UserId, Amount) VALUES (9003, 802, 7.00);
                """;
            seed.ExecuteNonQuery();
        }

        using var command = new SqlAzureCommandMock(connection)
        {
            CommandText = """
                SELECT u.Id AS UserId, latest.OrderId AS LatestOrderId
                FROM Users u
                CROSS APPLY (
                    SELECT TOP 1 o.OrderId
                    FROM Orders o
                    WHERE o.UserId = u.Id
                    ORDER BY o.OrderId DESC
                ) latest
                ORDER BY u.Id
                """
        };

        using var reader = command.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal(801, reader.GetInt32(reader.GetOrdinal("UserId")));
        Assert.Equal(9002, reader.GetInt32(reader.GetOrdinal("LatestOrderId")));
        Assert.True(reader.Read());
        Assert.Equal(802, reader.GetInt32(reader.GetOrdinal("UserId")));
        Assert.Equal(9003, reader.GetInt32(reader.GetOrdinal("LatestOrderId")));
        Assert.False(reader.Read());
    }

    /// <summary>
    /// EN: Ensures SQL Azure compatibility mode expands OPENJSON rows through CROSS APPLY on the shared SQL Server runtime path.
    /// PT: Garante que o modo de compatibilidade SQL Azure expanda linhas de OPENJSON via CROSS APPLY no caminho compartilhado de runtime do SQL Server.
    /// </summary>
    [Theory]
    [MemberDataSqlAzureCompatibilityLevel]
    public void ExecuteReader_CrossApply_OpenJson_ShouldExpandRows(int compatibilityLevel)
    {
        using var connection = CreateOpenConnection(compatibilityLevel);
        using (var seed = new SqlAzureCommandMock(connection))
        {
            seed.CommandText = """
                INSERT INTO Users (Id, Name, Email) VALUES (821, 'Ana', '["red","blue"]');
                INSERT INTO Users (Id, Name, Email) VALUES (822, 'Bia', '[]');
                """;
            seed.ExecuteNonQuery();
        }

        using var command = new SqlAzureCommandMock(connection)
        {
            CommandText = """
                SELECT u.Id AS UserId, tags.[value] AS TagValue
                FROM Users u
                CROSS APPLY OPENJSON(u.Email) tags
                ORDER BY u.Id, tags.[key]
                """
        };

        if (compatibilityLevel < SqlAzureDbCompatibilityLevels.SqlServer2016)
        {
            var ex = Assert.Throws<NotSupportedException>(() => command.ExecuteReader());
            Assert.Contains("OPENJSON", ex.Message, StringComparison.OrdinalIgnoreCase);
            return;
        }

        using var reader = command.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal(821, reader.GetInt32(reader.GetOrdinal("UserId")));
        Assert.Equal("red", reader.GetString(reader.GetOrdinal("TagValue")));
        Assert.True(reader.Read());
        Assert.Equal(821, reader.GetInt32(reader.GetOrdinal("UserId")));
        Assert.Equal("blue", reader.GetString(reader.GetOrdinal("TagValue")));
        Assert.False(reader.Read());
    }

    /// <summary>
    /// EN: Ensures SQL Azure compatibility mode projects OPENJSON WITH explicit schema through the shared SQL Server runtime path.
    /// PT: Garante que o modo de compatibilidade SQL Azure projete OPENJSON WITH com schema explicito pelo caminho compartilhado de runtime do SQL Server.
    /// </summary>
    [Theory]
    [MemberDataSqlAzureCompatibilityLevel]
    public void ExecuteReader_CrossApply_OpenJsonWithSchema_ShouldProjectTypedColumns(int compatibilityLevel)
    {
        using var connection = CreateOpenConnection(compatibilityLevel);
        using (var seed = new SqlAzureCommandMock(connection))
        {
            seed.CommandText = """
                INSERT INTO Users (Id, Name, Email) VALUES (851, 'Ana', '[{"Name":"red","Qty":2,"Payload":{"kind":"primary"}},{"Name":"blue","Qty":5,"Payload":[1,2]}]');
                """;
            seed.ExecuteNonQuery();
        }

        using var command = new SqlAzureCommandMock(connection)
        {
            CommandText = """
                SELECT data.Name AS ColorName, data.Qty AS ColorQty, data.PayloadJson AS PayloadJson
                FROM Users u
                CROSS APPLY OPENJSON(u.Email) WITH (
                    Name NVARCHAR(20) '$.Name',
                    Qty INT '$.Qty',
                    PayloadJson NVARCHAR(MAX) '$.Payload' AS JSON
                ) data
                WHERE u.Id = 851
                ORDER BY data.Qty
                """
        };

        if (compatibilityLevel < SqlAzureDbCompatibilityLevels.SqlServer2016)
        {
            var ex = Assert.Throws<NotSupportedException>(() => command.ExecuteReader());
            Assert.Contains("OPENJSON", ex.Message, StringComparison.OrdinalIgnoreCase);
            return;
        }

        using var reader = command.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal("red", reader.GetString(reader.GetOrdinal("ColorName")));
        Assert.Equal(2, reader.GetInt32(reader.GetOrdinal("ColorQty")));
        Assert.Equal("""{"kind":"primary"}""", reader.GetString(reader.GetOrdinal("PayloadJson")));
        Assert.True(reader.Read());
        Assert.Equal("blue", reader.GetString(reader.GetOrdinal("ColorName")));
        Assert.Equal(5, reader.GetInt32(reader.GetOrdinal("ColorQty")));
        Assert.Equal("[1,2]", reader.GetString(reader.GetOrdinal("PayloadJson")));
        Assert.False(reader.Read());
    }

    /// <summary>
    /// EN: Ensures SQL Azure compatibility mode supports quoted-key paths and array indexes in OPENJSON.
    /// PT: Garante que o modo de compatibilidade SQL Azure suporte paths com chave entre aspas e indices de array em OPENJSON.
    /// </summary>
    [Theory]
    [MemberDataSqlAzureCompatibilityLevel]
    public void ExecuteReader_CrossApply_OpenJsonWithQuotedKeyAndIndexPath_ShouldProjectValue(int compatibilityLevel)
    {
        using var connection = CreateOpenConnection(compatibilityLevel);
        using (var seed = new SqlAzureCommandMock(connection))
        {
            seed.CommandText = """
                INSERT INTO Users (Id, Name, Email) VALUES (861, 'Ana', '{"items":[{"Name.With.Dot":"red"},{"Name.With.Dot":"blue"}]}');
                """;
            seed.ExecuteNonQuery();
        }

        using var command = new SqlAzureCommandMock(connection)
        {
            CommandText = """
                SELECT data.Color AS ColorName
                FROM Users u
                CROSS APPLY OPENJSON(u.Email, 'lax $.items[1]') WITH (
                    Color NVARCHAR(20) '$."Name.With.Dot"'
                ) data
                WHERE u.Id = 861
                """
        };

        if (compatibilityLevel < SqlAzureDbCompatibilityLevels.SqlServer2016)
        {
            var ex = Assert.Throws<NotSupportedException>(() => command.ExecuteReader());
            Assert.Contains("OPENJSON", ex.Message, StringComparison.OrdinalIgnoreCase);
            return;
        }

        using var reader = command.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal("blue", reader.GetString(reader.GetOrdinal("ColorName")));
        Assert.False(reader.Read());
    }

    /// <summary>
    /// EN: Ensures SQL Azure compatibility mode expands UNPIVOT rows and skips NULL source values through the shared SQL Server runtime path.
    /// PT: Garante que o modo de compatibilidade SQL Azure expanda linhas de UNPIVOT e ignore valores NULL da fonte pelo caminho compartilhado de runtime do SQL Server.
    /// </summary>
    [Fact]
    public void ExecuteReader_WithUnpivot_ShouldExpandRowsAndSkipNulls()
    {
        using var connection = CreateOpenConnection();
        using (var seed = new SqlAzureCommandMock(connection))
        {
            seed.CommandText = """
                INSERT INTO Users (Id, Name, Email) VALUES (871, 'Ana', 'ana@example.com');
                INSERT INTO Users (Id, Name, Email) VALUES (872, 'Bia', NULL);
                """;
            seed.ExecuteNonQuery();
        }

        using var command = new SqlAzureCommandMock(connection)
        {
            CommandText = """
                SELECT up.Id AS UserId, up.FieldName AS FieldName, up.FieldValue AS FieldValue
                FROM (SELECT Id, Name, Email FROM Users WHERE Id IN (871, 872)) src
                UNPIVOT (FieldValue FOR FieldName IN (Name, Email)) up
                ORDER BY up.Id, up.FieldName
                """
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(871, reader.GetInt32(reader.GetOrdinal("UserId")));
        Assert.Equal("Email", reader.GetString(reader.GetOrdinal("FieldName")));
        Assert.Equal("ana@example.com", reader.GetString(reader.GetOrdinal("FieldValue")));

        Assert.True(reader.Read());
        Assert.Equal(871, reader.GetInt32(reader.GetOrdinal("UserId")));
        Assert.Equal("Name", reader.GetString(reader.GetOrdinal("FieldName")));
        Assert.Equal("Ana", reader.GetString(reader.GetOrdinal("FieldValue")));

        Assert.True(reader.Read());
        Assert.Equal(872, reader.GetInt32(reader.GetOrdinal("UserId")));
        Assert.Equal("Name", reader.GetString(reader.GetOrdinal("FieldName")));
        Assert.Equal("Bia", reader.GetString(reader.GetOrdinal("FieldValue")));

        Assert.False(reader.Read());
    }

    /// <summary>
    /// EN: Ensures SQL Azure compatibility mode preserves source column metadata for copied PIVOT and UNPIVOT columns on the shared SQL Server runtime path.
    /// PT: Garante que o modo de compatibilidade SQL Azure preserve o metadata das colunas de origem para colunas copiadas de PIVOT e UNPIVOT no caminho compartilhado de runtime do SQL Server.
    /// </summary>
    [Fact]
    public void ExecuteReader_WithPivotAndUnpivotCopiedColumns_ShouldExposeSourceFieldTypes()
    {
        using var connection = CreateOpenConnection();

        using var pivotCommand = new SqlAzureCommandMock(connection)
        {
            CommandText = """
                SELECT p.Category, p.T10
                FROM (
                    SELECT CAST('A' AS NVARCHAR(10)) AS Category, 10 AS TenantId, CAST(2.0 AS FLOAT) AS Amount
                    UNION ALL SELECT CAST('A' AS NVARCHAR(10)), 10, CAST(4.0 AS FLOAT)
                ) src
                PIVOT (
                    MAX(Amount) FOR TenantId IN (10 AS T10)
                ) p
                """
        };

        using var pivotReader = pivotCommand.ExecuteReader();
        Assert.Equal(typeof(string), pivotReader.GetFieldType(pivotReader.GetOrdinal("Category")));
        Assert.Equal("String", pivotReader.GetDataTypeName(pivotReader.GetOrdinal("Category")));

        using var unpivotCommand = new SqlAzureCommandMock(connection)
        {
            CommandText = """
                SELECT up.Id, up.FieldName, up.FieldValue
                FROM (
                    SELECT 1 AS Id, CAST('Ana' AS NVARCHAR(50)) AS Name, CAST('ana@example.com' AS NVARCHAR(100)) AS Email
                ) src
                UNPIVOT (FieldValue FOR FieldName IN (Name, Email)) up
                """
        };

        using var unpivotReader = unpivotCommand.ExecuteReader();
        Assert.Equal(typeof(int), unpivotReader.GetFieldType(unpivotReader.GetOrdinal("Id")));
        Assert.Equal("Int32", unpivotReader.GetDataTypeName(unpivotReader.GetOrdinal("Id")));
        Assert.Equal(typeof(string), unpivotReader.GetFieldType(unpivotReader.GetOrdinal("FieldName")));
        Assert.Equal("String", unpivotReader.GetDataTypeName(unpivotReader.GetOrdinal("FieldName")));
        Assert.Equal(typeof(string), unpivotReader.GetFieldType(unpivotReader.GetOrdinal("FieldValue")));
        Assert.Equal("String", unpivotReader.GetDataTypeName(unpivotReader.GetOrdinal("FieldValue")));

        using var mixedUnpivotCommand = new SqlAzureCommandMock(connection)
        {
            CommandText = """
                SELECT up.Id, up.FieldValue
                FROM (
                    SELECT 1 AS Id, CAST('Ana' AS NVARCHAR(50)) AS Name, CAST(42 AS INT) AS Score
                ) src
                UNPIVOT (FieldValue FOR FieldName IN (Name, Score)) up
                """
        };

        using var mixedUnpivotReader = mixedUnpivotCommand.ExecuteReader();
        Assert.Equal(typeof(object), mixedUnpivotReader.GetFieldType(mixedUnpivotReader.GetOrdinal("FieldValue")));
        Assert.Equal("Object", mixedUnpivotReader.GetDataTypeName(mixedUnpivotReader.GetOrdinal("FieldValue")));

        using var schemaCommand = new SqlAzureCommandMock(connection)
        {
            CommandText = """
                SELECT up.Id, up.FieldName, up.FieldValue
                FROM (
                    SELECT 1 AS Id, CAST('Ana' AS NVARCHAR(50)) AS Name, CAST('ana@example.com' AS NVARCHAR(100)) AS Email
                ) src
                UNPIVOT (FieldValue FOR FieldName IN (Name, Email)) up
                """
        };

        using var schemaReader = schemaCommand.ExecuteReader();
        var schema = schemaReader.GetSchemaTable();
        Assert.NotNull(schema);
        Assert.False(schema!.Columns["FieldValue"]!.AllowDBNull);
        Assert.False(schema.Columns["FieldName"]!.AllowDBNull);
    }

    /// <summary>
    /// EN: Ensures SQL Azure compatibility mode computes STDEV, STDEVP, VAR, and VARP in PIVOT through the shared SQL Server runtime path.
    /// PT: Garante que o modo de compatibilidade SQL Azure calcule STDEV, STDEVP, VAR e VARP em PIVOT pelo caminho compartilhado de runtime do SQL Server.
    /// </summary>
    [Fact]
    public void ExecuteReader_WithPivotVarianceAggregates_ShouldReturnExpectedNumbers()
    {
        using var connection = CreateOpenConnection();

        static string BuildPivotSql(string aggregateName)
            => $"""
                SELECT p.T10, p.T20
                FROM (
                    SELECT 10 AS TenantId, CAST(2.0 AS FLOAT) AS Amount
                    UNION ALL SELECT 10, CAST(4.0 AS FLOAT)
                    UNION ALL SELECT 20, CAST(1.0 AS FLOAT)
                    UNION ALL SELECT 20, CAST(5.0 AS FLOAT)
                ) src
                PIVOT (
                    {aggregateName}(Amount) FOR TenantId IN (10 AS T10, 20 AS T20)
                ) p
                """;

        static void AssertPivotAggregate(
            SqlAzureConnectionMock connection,
            string aggregateName,
            double expectedTenant10,
            double expectedTenant20)
        {
            using var command = new SqlAzureCommandMock(connection)
            {
                CommandText = BuildPivotSql(aggregateName)
            };

            using var reader = command.ExecuteReader();
            Assert.True(reader.Read());
            Assert.Equal(expectedTenant10, reader.GetDouble(reader.GetOrdinal("T10")), 10);
            Assert.Equal(expectedTenant20, reader.GetDouble(reader.GetOrdinal("T20")), 10);
            Assert.False(reader.Read());
        }

        AssertPivotAggregate(connection, "STDEV", Math.Sqrt(2d), Math.Sqrt(8d));
        AssertPivotAggregate(connection, "STDEVP", 1d, 2d);
        AssertPivotAggregate(connection, "VAR", 2d, 8d);
        AssertPivotAggregate(connection, "VARP", 1d, 4d);
    }

    /// <summary>
    /// EN: Ensures SQL Azure compatibility mode computes COUNT_BIG in PIVOT with bigint-shaped results on the shared SQL Server runtime path.
    /// PT: Garante que o modo de compatibilidade SQL Azure calcule COUNT_BIG em PIVOT com resultado no shape bigint no caminho compartilhado de runtime do SQL Server.
    /// </summary>
    [Fact]
    public void ExecuteReader_WithPivotCountBig_ShouldReturnInt64Counts()
    {
        using var connection = CreateOpenConnection();
        using var command = new SqlAzureCommandMock(connection)
        {
            CommandText = """
                SELECT p.T10, p.T20
                FROM (
                    SELECT 10 AS TenantId, CAST(2.0 AS FLOAT) AS Amount
                    UNION ALL SELECT 10, CAST(NULL AS FLOAT)
                    UNION ALL SELECT 10, CAST(4.0 AS FLOAT)
                    UNION ALL SELECT 20, CAST(1.0 AS FLOAT)
                    UNION ALL SELECT 20, CAST(5.0 AS FLOAT)
                ) src
                PIVOT (
                    COUNT_BIG(Amount) FOR TenantId IN (10 AS T10, 20 AS T20)
                ) p
                """
        };

        using var reader = command.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal(2L, reader.GetInt64(reader.GetOrdinal("T10")));
        Assert.Equal(2L, reader.GetInt64(reader.GetOrdinal("T20")));
        Assert.False(reader.Read());
    }

    /// <summary>
    /// EN: Ensures SQL Azure compatibility mode exposes PIVOT aggregate metadata aligned with COUNT_BIG and statistical return types on the shared SQL Server runtime path.
    /// PT: Garante que o modo de compatibilidade SQL Azure exponha metadados de agregacao do PIVOT alinhados aos tipos de retorno de COUNT_BIG e agregadores estatisticos no caminho compartilhado de runtime do SQL Server.
    /// </summary>
    [Fact]
    public void ExecuteReader_WithPivotAggregateMetadata_ShouldExposeExpectedFieldTypes()
    {
        using var connection = CreateOpenConnection();

        using var countBigCommand = new SqlAzureCommandMock(connection)
        {
            CommandText = """
                SELECT p.T10
                FROM (
                    SELECT 10 AS TenantId, CAST(2.0 AS FLOAT) AS Amount
                    UNION ALL SELECT 10, CAST(4.0 AS FLOAT)
                ) src
                PIVOT (
                    COUNT_BIG(Amount) FOR TenantId IN (10 AS T10)
                ) p
                """
        };

        using var countBigReader = countBigCommand.ExecuteReader();
        Assert.Equal(typeof(long), countBigReader.GetFieldType(countBigReader.GetOrdinal("T10")));
        Assert.Equal("Int64", countBigReader.GetDataTypeName(countBigReader.GetOrdinal("T10")));
        var countBigSchema = countBigReader.GetSchemaTable();
        Assert.NotNull(countBigSchema);
        Assert.True(countBigSchema!.Columns["T10"]!.AllowDBNull);

        using var countCommand = new SqlAzureCommandMock(connection)
        {
            CommandText = """
                SELECT p.T10
                FROM (
                    SELECT 10 AS TenantId, CAST(2.0 AS FLOAT) AS Amount
                    UNION ALL SELECT 10, CAST(4.0 AS FLOAT)
                ) src
                PIVOT (
                    COUNT(Amount) FOR TenantId IN (10 AS T10)
                ) p
                """
        };

        using var countReader = countCommand.ExecuteReader();
        Assert.Equal(typeof(int), countReader.GetFieldType(countReader.GetOrdinal("T10")));
        Assert.Equal("Int32", countReader.GetDataTypeName(countReader.GetOrdinal("T10")));
        var countSchema = countReader.GetSchemaTable();
        Assert.NotNull(countSchema);
        Assert.True(countSchema!.Columns["T10"]!.AllowDBNull);

        using var countStarCommand = new SqlAzureCommandMock(connection)
        {
            CommandText = """
                SELECT p.T10
                FROM (
                    SELECT 10 AS TenantId, CAST(2.0 AS FLOAT) AS Amount
                    UNION ALL SELECT 10, CAST(NULL AS FLOAT)
                ) src
                PIVOT (
                    COUNT(*) FOR TenantId IN (10 AS T10)
                ) p
                """
        };

        using var countStarReader = countStarCommand.ExecuteReader();
        Assert.Equal(typeof(int), countStarReader.GetFieldType(countStarReader.GetOrdinal("T10")));
        Assert.Equal("Int32", countStarReader.GetDataTypeName(countStarReader.GetOrdinal("T10")));
        var countStarSchema = countStarReader.GetSchemaTable();
        Assert.NotNull(countStarSchema);
        Assert.True(countStarSchema!.Columns["T10"]!.AllowDBNull);

        using var stdevCommand = new SqlAzureCommandMock(connection)
        {
            CommandText = """
                SELECT p.T10
                FROM (
                    SELECT 10 AS TenantId, CAST(2.0 AS FLOAT) AS Amount
                    UNION ALL SELECT 10, CAST(4.0 AS FLOAT)
                ) src
                PIVOT (
                    STDEV(Amount) FOR TenantId IN (10 AS T10)
                ) p
                """
        };

        using var stdevReader = stdevCommand.ExecuteReader();
        Assert.Equal(typeof(double), stdevReader.GetFieldType(stdevReader.GetOrdinal("T10")));
        Assert.Equal("Double", stdevReader.GetDataTypeName(stdevReader.GetOrdinal("T10")));

        using var maxCommand = new SqlAzureCommandMock(connection)
        {
            CommandText = """
                SELECT p.T10
                FROM (
                    SELECT 10 AS TenantId, CAST(2.0 AS FLOAT) AS Amount
                    UNION ALL SELECT 10, CAST(4.0 AS FLOAT)
                ) src
                PIVOT (
                    MAX(Amount) FOR TenantId IN (10 AS T10)
                ) p
                """
        };

        using var maxReader = maxCommand.ExecuteReader();
        Assert.Equal(typeof(double), maxReader.GetFieldType(maxReader.GetOrdinal("T10")));
        Assert.Equal("Double", maxReader.GetDataTypeName(maxReader.GetOrdinal("T10")));

        using var sumCommand = new SqlAzureCommandMock(connection)
        {
            CommandText = """
                SELECT p.T10
                FROM (
                    SELECT 10 AS TenantId, CAST(2.0 AS FLOAT) AS Amount
                    UNION ALL SELECT 10, CAST(4.0 AS FLOAT)
                ) src
                PIVOT (
                    SUM(Amount) FOR TenantId IN (10 AS T10)
                ) p
                """
        };

        using var sumReader = sumCommand.ExecuteReader();
        Assert.Equal(typeof(double), sumReader.GetFieldType(sumReader.GetOrdinal("T10")));
        Assert.Equal("Double", sumReader.GetDataTypeName(sumReader.GetOrdinal("T10")));

        using var sumSmallIntCommand = new SqlAzureCommandMock(connection)
        {
            CommandText = """
                SELECT p.T10
                FROM (
                    SELECT 10 AS TenantId, CAST(2 AS SMALLINT) AS Amount
                    UNION ALL SELECT 10, CAST(4 AS SMALLINT)
                ) src
                PIVOT (
                    SUM(Amount) FOR TenantId IN (10 AS T10)
                ) p
                """
        };

        using var sumSmallIntReader = sumSmallIntCommand.ExecuteReader();
        Assert.Equal(typeof(int), sumSmallIntReader.GetFieldType(sumSmallIntReader.GetOrdinal("T10")));
        Assert.Equal("Int32", sumSmallIntReader.GetDataTypeName(sumSmallIntReader.GetOrdinal("T10")));
        Assert.True(sumSmallIntReader.Read());
        Assert.Equal(6, sumSmallIntReader.GetInt32(sumSmallIntReader.GetOrdinal("T10")));
        Assert.IsType<int>(sumSmallIntReader.GetValue(sumSmallIntReader.GetOrdinal("T10")));
        Assert.False(sumSmallIntReader.Read());

        using var sumTinyIntCommand = new SqlAzureCommandMock(connection)
        {
            CommandText = """
                SELECT p.T10
                FROM (
                    SELECT 10 AS TenantId, CAST(2 AS TINYINT) AS Amount
                    UNION ALL SELECT 10, CAST(4 AS TINYINT)
                ) src
                PIVOT (
                    SUM(Amount) FOR TenantId IN (10 AS T10)
                ) p
                """
        };

        using var sumTinyIntReader = sumTinyIntCommand.ExecuteReader();
        Assert.Equal(typeof(int), sumTinyIntReader.GetFieldType(sumTinyIntReader.GetOrdinal("T10")));
        Assert.Equal("Int32", sumTinyIntReader.GetDataTypeName(sumTinyIntReader.GetOrdinal("T10")));
        Assert.True(sumTinyIntReader.Read());
        Assert.Equal(6, sumTinyIntReader.GetInt32(sumTinyIntReader.GetOrdinal("T10")));
        Assert.IsType<int>(sumTinyIntReader.GetValue(sumTinyIntReader.GetOrdinal("T10")));
        Assert.False(sumTinyIntReader.Read());

        using var avgCommand = new SqlAzureCommandMock(connection)
        {
            CommandText = """
                SELECT p.T10
                FROM (
                    SELECT 10 AS TenantId, CAST(2.0 AS FLOAT) AS Amount
                    UNION ALL SELECT 10, CAST(4.0 AS FLOAT)
                ) src
                PIVOT (
                    AVG(Amount) FOR TenantId IN (10 AS T10)
                ) p
                """
        };

        using var avgReader = avgCommand.ExecuteReader();
        Assert.Equal(typeof(double), avgReader.GetFieldType(avgReader.GetOrdinal("T10")));
        Assert.Equal("Double", avgReader.GetDataTypeName(avgReader.GetOrdinal("T10")));
        var avgSchema = avgReader.GetSchemaTable();
        Assert.NotNull(avgSchema);
        Assert.True(avgSchema!.Columns["T10"]!.AllowDBNull);

        using var avgSmallIntCommand = new SqlAzureCommandMock(connection)
        {
            CommandText = """
                SELECT p.T10
                FROM (
                    SELECT 10 AS TenantId, CAST(2 AS SMALLINT) AS Amount
                    UNION ALL SELECT 10, CAST(4 AS SMALLINT)
                ) src
                PIVOT (
                    AVG(Amount) FOR TenantId IN (10 AS T10)
                ) p
                """
        };

        using var avgSmallIntReader = avgSmallIntCommand.ExecuteReader();
        Assert.Equal(typeof(int), avgSmallIntReader.GetFieldType(avgSmallIntReader.GetOrdinal("T10")));
        Assert.Equal("Int32", avgSmallIntReader.GetDataTypeName(avgSmallIntReader.GetOrdinal("T10")));
        Assert.True(avgSmallIntReader.Read());
        Assert.Equal(3, avgSmallIntReader.GetInt32(avgSmallIntReader.GetOrdinal("T10")));
        Assert.IsType<int>(avgSmallIntReader.GetValue(avgSmallIntReader.GetOrdinal("T10")));
        Assert.False(avgSmallIntReader.Read());

        using var avgTinyIntCommand = new SqlAzureCommandMock(connection)
        {
            CommandText = """
                SELECT p.T10
                FROM (
                    SELECT 10 AS TenantId, CAST(2 AS TINYINT) AS Amount
                    UNION ALL SELECT 10, CAST(4 AS TINYINT)
                ) src
                PIVOT (
                    AVG(Amount) FOR TenantId IN (10 AS T10)
                ) p
                """
        };

        using var avgTinyIntReader = avgTinyIntCommand.ExecuteReader();
        Assert.Equal(typeof(int), avgTinyIntReader.GetFieldType(avgTinyIntReader.GetOrdinal("T10")));
        Assert.Equal("Int32", avgTinyIntReader.GetDataTypeName(avgTinyIntReader.GetOrdinal("T10")));
        Assert.True(avgTinyIntReader.Read());
        Assert.Equal(3, avgTinyIntReader.GetInt32(avgTinyIntReader.GetOrdinal("T10")));
        Assert.IsType<int>(avgTinyIntReader.GetValue(avgTinyIntReader.GetOrdinal("T10")));
        Assert.False(avgTinyIntReader.Read());

        using var avgTinyIntFractionCommand = new SqlAzureCommandMock(connection)
        {
            CommandText = """
                SELECT p.T10
                FROM (
                    SELECT 10 AS TenantId, CAST(1 AS TINYINT) AS Amount
                    UNION ALL SELECT 10, CAST(2 AS TINYINT)
                ) src
                PIVOT (
                    AVG(Amount) FOR TenantId IN (10 AS T10)
                ) p
                """
        };

        using var avgTinyIntFractionReader = avgTinyIntFractionCommand.ExecuteReader();
        Assert.True(avgTinyIntFractionReader.Read());
        Assert.Equal(1, avgTinyIntFractionReader.GetInt32(avgTinyIntFractionReader.GetOrdinal("T10")));
        Assert.IsType<int>(avgTinyIntFractionReader.GetValue(avgTinyIntFractionReader.GetOrdinal("T10")));
        Assert.False(avgTinyIntFractionReader.Read());

        using var avgIntCommand = new SqlAzureCommandMock(connection)
        {
            CommandText = """
                SELECT p.T10
                FROM (
                    SELECT 10 AS TenantId, CAST(2 AS INT) AS Amount
                    UNION ALL SELECT 10, CAST(4 AS INT)
                ) src
                PIVOT (
                    AVG(Amount) FOR TenantId IN (10 AS T10)
                ) p
                """
        };

        using var avgIntReader = avgIntCommand.ExecuteReader();
        Assert.Equal(typeof(int), avgIntReader.GetFieldType(avgIntReader.GetOrdinal("T10")));
        Assert.Equal("Int32", avgIntReader.GetDataTypeName(avgIntReader.GetOrdinal("T10")));
        Assert.True(avgIntReader.Read());
        Assert.Equal(3, avgIntReader.GetInt32(avgIntReader.GetOrdinal("T10")));
        Assert.IsType<int>(avgIntReader.GetValue(avgIntReader.GetOrdinal("T10")));
        Assert.False(avgIntReader.Read());

        using var avgIntFractionCommand = new SqlAzureCommandMock(connection)
        {
            CommandText = """
                SELECT p.T10
                FROM (
                    SELECT 10 AS TenantId, CAST(1 AS INT) AS Amount
                    UNION ALL SELECT 10, CAST(2 AS INT)
                ) src
                PIVOT (
                    AVG(Amount) FOR TenantId IN (10 AS T10)
                ) p
                """
        };

        using var avgIntFractionReader = avgIntFractionCommand.ExecuteReader();
        Assert.True(avgIntFractionReader.Read());
        Assert.Equal(1, avgIntFractionReader.GetInt32(avgIntFractionReader.GetOrdinal("T10")));
        Assert.IsType<int>(avgIntFractionReader.GetValue(avgIntFractionReader.GetOrdinal("T10")));
        Assert.False(avgIntFractionReader.Read());

        using var avgIntNegativeFractionCommand = new SqlAzureCommandMock(connection)
        {
            CommandText = """
                SELECT p.T10
                FROM (
                    SELECT 10 AS TenantId, CAST(-1 AS INT) AS Amount
                    UNION ALL SELECT 10, CAST(-2 AS INT)
                ) src
                PIVOT (
                    AVG(Amount) FOR TenantId IN (10 AS T10)
                ) p
                """
        };

        using var avgIntNegativeFractionReader = avgIntNegativeFractionCommand.ExecuteReader();
        Assert.True(avgIntNegativeFractionReader.Read());
        Assert.Equal(-1, avgIntNegativeFractionReader.GetInt32(avgIntNegativeFractionReader.GetOrdinal("T10")));
        Assert.IsType<int>(avgIntNegativeFractionReader.GetValue(avgIntNegativeFractionReader.GetOrdinal("T10")));
        Assert.False(avgIntNegativeFractionReader.Read());

        using var avgBigIntCommand = new SqlAzureCommandMock(connection)
        {
            CommandText = """
                SELECT p.T10
                FROM (
                    SELECT 10 AS TenantId, CAST(2 AS BIGINT) AS Amount
                    UNION ALL SELECT 10, CAST(4 AS BIGINT)
                ) src
                PIVOT (
                    AVG(Amount) FOR TenantId IN (10 AS T10)
                ) p
                """
        };

        using var avgBigIntReader = avgBigIntCommand.ExecuteReader();
        Assert.Equal(typeof(long), avgBigIntReader.GetFieldType(avgBigIntReader.GetOrdinal("T10")));
        Assert.Equal("Int64", avgBigIntReader.GetDataTypeName(avgBigIntReader.GetOrdinal("T10")));
        Assert.True(avgBigIntReader.Read());
        Assert.Equal(3L, avgBigIntReader.GetInt64(avgBigIntReader.GetOrdinal("T10")));
        Assert.IsType<long>(avgBigIntReader.GetValue(avgBigIntReader.GetOrdinal("T10")));
        Assert.False(avgBigIntReader.Read());

        using var avgBigIntFractionCommand = new SqlAzureCommandMock(connection)
        {
            CommandText = """
                SELECT p.T10
                FROM (
                    SELECT 10 AS TenantId, CAST(1 AS BIGINT) AS Amount
                    UNION ALL SELECT 10, CAST(2 AS BIGINT)
                ) src
                PIVOT (
                    AVG(Amount) FOR TenantId IN (10 AS T10)
                ) p
                """
        };

        using var avgBigIntFractionReader = avgBigIntFractionCommand.ExecuteReader();
        Assert.True(avgBigIntFractionReader.Read());
        Assert.Equal(1L, avgBigIntFractionReader.GetInt64(avgBigIntFractionReader.GetOrdinal("T10")));
        Assert.IsType<long>(avgBigIntFractionReader.GetValue(avgBigIntFractionReader.GetOrdinal("T10")));
        Assert.False(avgBigIntFractionReader.Read());

        using var avgBigIntNegativeFractionCommand = new SqlAzureCommandMock(connection)
        {
            CommandText = """
                SELECT p.T10
                FROM (
                    SELECT 10 AS TenantId, CAST(-1 AS BIGINT) AS Amount
                    UNION ALL SELECT 10, CAST(-2 AS BIGINT)
                ) src
                PIVOT (
                    AVG(Amount) FOR TenantId IN (10 AS T10)
                ) p
                """
        };

        using var avgBigIntNegativeFractionReader = avgBigIntNegativeFractionCommand.ExecuteReader();
        Assert.True(avgBigIntNegativeFractionReader.Read());
        Assert.Equal(-1L, avgBigIntNegativeFractionReader.GetInt64(avgBigIntNegativeFractionReader.GetOrdinal("T10")));
        Assert.IsType<long>(avgBigIntNegativeFractionReader.GetValue(avgBigIntNegativeFractionReader.GetOrdinal("T10")));
        Assert.False(avgBigIntNegativeFractionReader.Read());
    }

    /// <summary>
    /// EN: Ensures SQL Azure compatibility mode serializes FOR JSON AUTO with INCLUDE_NULL_VALUES and WITHOUT_ARRAY_WRAPPER on the shared SQL Server runtime path.
    /// PT: Garante que o modo de compatibilidade SQL Azure serialize FOR JSON AUTO com INCLUDE_NULL_VALUES e WITHOUT_ARRAY_WRAPPER no caminho compartilhado de runtime do SQL Server.
    /// </summary>
    [Theory]
    [MemberDataSqlAzureCompatibilityLevel]
    public void ExecuteScalar_ForJsonAutoWithOptions_ShouldSerializeSingleObject(int compatibilityLevel)
    {
        using var connection = CreateOpenConnection(compatibilityLevel);
        using (var seed = new SqlAzureCommandMock(connection))
        {
            seed.CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (881, 'Ana', NULL)";
            seed.ExecuteNonQuery();
        }

        using var command = new SqlAzureCommandMock(connection)
        {
            CommandText = """
                SELECT u.Id, u.Name, u.Email
                FROM Users u
                WHERE u.Id = 881
                FOR JSON AUTO, INCLUDE_NULL_VALUES, WITHOUT_ARRAY_WRAPPER
                """
        };

        if (compatibilityLevel < SqlAzureDbCompatibilityLevels.SqlServer2016)
        {
            var ex = Assert.Throws<NotSupportedException>(() => command.ExecuteScalar());
            Assert.Contains("FOR JSON", ex.Message, StringComparison.OrdinalIgnoreCase);
            return;
        }
        var json = Assert.IsType<string>(command.ExecuteScalar());
        using var document = System.Text.Json.JsonDocument.Parse(json);
        var root = document.RootElement;

        Assert.Equal(System.Text.Json.JsonValueKind.Object, root.ValueKind);
        Assert.Equal(881, root.GetProperty("Id").GetInt32());
        Assert.Equal("Ana", root.GetProperty("Name").GetString());
        Assert.True(root.TryGetProperty("Email", out var email));
        Assert.Equal(System.Text.Json.JsonValueKind.Null, email.ValueKind);
    }

    /// <summary>
    /// EN: Ensures SQL Azure compatibility mode preserves OPENJSON AS JSON fragments when FOR JSON PATH serializes the final rowset.
    /// PT: Garante que o modo de compatibilidade SQL Azure preserve fragmentos de OPENJSON AS JSON quando FOR JSON PATH serializa o rowset final.
    /// </summary>
    [Theory]
    [MemberDataSqlAzureCompatibilityLevel]
    public void ExecuteScalar_ForJsonPath_WithOpenJsonAsJson_ShouldEmbedJsonFragment(int compatibilityLevel)
    {
        using var connection = CreateOpenConnection(compatibilityLevel);
        using (var seed = new SqlAzureCommandMock(connection))
        {
            seed.CommandText = """
                INSERT INTO Users (Id, Name, Email) VALUES (882, 'Ana', '{"profile":{"active":true,"roles":["admin","ops"]}}');
                """;
            seed.ExecuteNonQuery();
        }

        using var command = new SqlAzureCommandMock(connection)
        {
            CommandText = """
                SELECT profile.Profile AS [User.Profile]
                FROM Users u
                CROSS APPLY OPENJSON(u.Email) WITH (
                    Profile nvarchar(max) '$.profile' AS JSON
                ) profile
                WHERE u.Id = 882
                FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
                """
        };

        if (compatibilityLevel < SqlAzureDbCompatibilityLevels.SqlServer2016)
        {
            var ex = Assert.Throws<NotSupportedException>(() => command.ExecuteScalar());
            Assert.Contains("OPENJSON", ex.Message, StringComparison.OrdinalIgnoreCase);
            return;
        }
        var json = Assert.IsType<string>(command.ExecuteScalar());
        using var document = System.Text.Json.JsonDocument.Parse(json);
        var profile = document.RootElement.GetProperty("User").GetProperty("Profile");

        Assert.Equal(System.Text.Json.JsonValueKind.Object, profile.ValueKind);
        Assert.True(profile.GetProperty("active").GetBoolean());
        Assert.Equal("ops", profile.GetProperty("roles")[1].GetString());
    }

    /// <summary>
    /// EN: Ensures SQL Azure compatibility mode rejects conflicting nested alias order in FOR JSON PATH instead of silently merging incompatible object paths.
    /// PT: Garante que o modo de compatibilidade SQL Azure rejeite ordem conflitante de aliases aninhados em FOR JSON PATH em vez de mesclar silenciosamente caminhos de objeto incompativeis.
    /// </summary>
    [Theory]
    [MemberDataSqlAzureCompatibilityLevel]
    public void ExecuteScalar_ForJsonPath_WithConflictingNestedAliasOrder_ShouldThrow(int compatibilityLevel)
    {
        using var connection = CreateOpenConnection(compatibilityLevel);
        using var command = new SqlAzureCommandMock(connection)
        {
            CommandText = """
                SELECT
                    1 AS [Movement.Something.LocationName],
                    2 AS [Movement.Transporter.Id],
                    3 AS [Movement.Something.Destination]
                FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
                """
        };

        if (compatibilityLevel < SqlAzureDbCompatibilityLevels.SqlServer2016)
        {
            var ex2 = Assert.Throws<NotSupportedException>(() => command.ExecuteScalar());
            Assert.Contains("FOR JSON", ex2.Message, StringComparison.OrdinalIgnoreCase);
            return;
        }
        var ex = Assert.Throws<InvalidOperationException>(() => command.ExecuteScalar());
        Assert.Contains("FOR JSON PATH", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("Movement.Something.Destination", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures SQL Azure compatibility mode preserves JSON_QUERY fragments when FOR JSON PATH serializes the final rowset.
    /// PT: Garante que o modo de compatibilidade SQL Azure preserve fragmentos de JSON_QUERY quando FOR JSON PATH serializa o rowset final.
    /// </summary>
    [Theory]
    [MemberDataSqlAzureCompatibilityLevel]
    public void ExecuteScalar_ForJsonPath_WithJsonQuery_ShouldEmbedJsonFragment(int compatibilityLevel)
    {
        using var connection = CreateOpenConnection(compatibilityLevel);
        using (var seed = new SqlAzureCommandMock(connection))
        {
            seed.CommandText = """
                INSERT INTO Users (Id, Name, Email) VALUES (883, 'Ana', '{"profile":{"active":true,"roles":["admin","ops"]}}');
                """;
            seed.ExecuteNonQuery();
        }

        using var command = new SqlAzureCommandMock(connection)
        {
            CommandText = """
                SELECT JSON_QUERY(u.Email, '$.profile') AS [User.Profile]
                FROM Users u
                WHERE u.Id = 883
                FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
                """
        };

        if (compatibilityLevel < SqlAzureDbCompatibilityLevels.SqlServer2016)
        {
            var ex = Assert.Throws<NotSupportedException>(() => command.ExecuteScalar());
            Assert.Contains("JSON_QUERY", ex.Message, StringComparison.OrdinalIgnoreCase);
            return;
        }
        var json = Assert.IsType<string>(command.ExecuteScalar());
        using var document = System.Text.Json.JsonDocument.Parse(json);
        var profile = document.RootElement.GetProperty("User").GetProperty("Profile");

        Assert.Equal(System.Text.Json.JsonValueKind.Object, profile.ValueKind);
        Assert.True(profile.GetProperty("active").GetBoolean());
        Assert.Equal(2, profile.GetProperty("roles").GetArrayLength());
    }

    /// <summary>
    /// EN: Ensures SQL Azure compatibility mode preserves a root JSON object when JSON_QUERY is called without an explicit path.
    /// PT: Garante que o modo de compatibilidade SQL Azure preserve um objeto JSON de raiz quando JSON_QUERY e chamado sem path explicito.
    /// </summary>
    [Theory]
    [MemberDataSqlAzureCompatibilityLevel]
    public void ExecuteScalar_JsonQuery_WithoutPath_ShouldReturnRootJsonFragment(int compatibilityLevel)
    {
        using var connection = CreateOpenConnection(compatibilityLevel);
        using (var seed = new SqlAzureCommandMock(connection))
        {
            seed.CommandText = """
                INSERT INTO Users (Id, Name, Email) VALUES (8831, 'Bia', '{"profile":{"active":true},"roles":["admin","ops"]}');
                """;
            seed.ExecuteNonQuery();
        }

        using var scalarCommand = new SqlAzureCommandMock(connection)
        {
            CommandText = """
                SELECT JSON_QUERY(u.Email)
                FROM Users u
                WHERE u.Id = 8831
                """
        };

        if (compatibilityLevel < SqlAzureDbCompatibilityLevels.SqlServer2016)
        {
            var ex = Assert.Throws<NotSupportedException>(() => scalarCommand.ExecuteScalar());
            Assert.Contains("JSON_QUERY", ex.Message, StringComparison.OrdinalIgnoreCase);
            return;
        }
        var scalarJson = Assert.IsType<string>(scalarCommand.ExecuteScalar());
        using (var scalarDocument = System.Text.Json.JsonDocument.Parse(scalarJson))
        {
            Assert.Equal(System.Text.Json.JsonValueKind.Object, scalarDocument.RootElement.ValueKind);
            Assert.True(scalarDocument.RootElement.GetProperty("profile").GetProperty("active").GetBoolean());
        }

        using var forJsonCommand = new SqlAzureCommandMock(connection)
        {
            CommandText = """
                SELECT JSON_QUERY(u.Email) AS [User.Payload]
                FROM Users u
                WHERE u.Id = 8831
                FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
                """
        };

        var embeddedJson = Assert.IsType<string>(forJsonCommand.ExecuteScalar());
        using var embeddedDocument = System.Text.Json.JsonDocument.Parse(embeddedJson);
        var payload = embeddedDocument.RootElement.GetProperty("User").GetProperty("Payload");

        Assert.Equal(System.Text.Json.JsonValueKind.Object, payload.ValueKind);
        Assert.Equal("ops", payload.GetProperty("roles")[1].GetString());
    }

    /// <summary>
    /// EN: Ensures SQL Azure compatibility mode skips nested FOR JSON AUTO aliases for LEFT JOIN rows without child matches.
    /// PT: Garante que o modo de compatibilidade SQL Azure ignore aliases aninhados de FOR JSON AUTO para linhas de LEFT JOIN sem correspondência filha.
    /// </summary>
    [Theory]
    [MemberDataSqlAzureCompatibilityLevel]
    public void ExecuteScalar_ForJsonAuto_LeftJoinWithoutChild_ShouldSkipNestedAlias(int compatibilityLevel)
    {
        using var connection = CreateOpenConnection(compatibilityLevel);
        using (var seed = new SqlAzureCommandMock(connection))
        {
            seed.CommandText = """
                INSERT INTO Users (Id, Name, Email) VALUES (884, 'Ana', NULL);
                INSERT INTO Users (Id, Name, Email) VALUES (885, 'Bia', NULL);
                INSERT INTO Orders (OrderId, UserId, Amount) VALUES (9905, 884, 10.50);
                """;
            seed.ExecuteNonQuery();
        }

        using var command = new SqlAzureCommandMock(connection)
        {
            CommandText = """
                SELECT u.Id, u.Name, o.OrderId, o.Amount
                FROM Users u
                LEFT JOIN Orders o ON o.UserId = u.Id
                WHERE u.Id IN (884, 885)
                ORDER BY u.Id, o.OrderId
                FOR JSON AUTO, INCLUDE_NULL_VALUES
                """
        };

        if (compatibilityLevel < SqlAzureDbCompatibilityLevels.SqlServer2016)
        {
            var ex = Assert.Throws<NotSupportedException>(() => command.ExecuteScalar());
            Assert.Contains("FOR JSON", ex.Message, StringComparison.OrdinalIgnoreCase);
            return;
        }
        var json = Assert.IsType<string>(command.ExecuteScalar());
        using var document = System.Text.Json.JsonDocument.Parse(json);
        var array = document.RootElement;

        Assert.Equal(2, array.GetArrayLength());
        Assert.True(array[0].TryGetProperty("o", out var firstOrders));
        Assert.Single(firstOrders.EnumerateArray());
        Assert.False(array[1].TryGetProperty("o", out _));
    }

    /// <summary>
    /// EN: Ensures SQL Azure compatibility mode executes schema-qualified OPENJSON through CROSS APPLY on the shared SQL Server runtime path.
    /// PT: Garante que o modo de compatibilidade SQL Azure execute OPENJSON qualificado por schema via CROSS APPLY no caminho compartilhado de runtime do SQL Server.
    /// </summary>
    [Theory]
    [MemberDataSqlAzureCompatibilityLevel]
    public void ExecuteReader_CrossApply_SchemaQualifiedOpenJson_ShouldReturnRows(int compatibilityLevel)
    {
        using var connection = CreateOpenConnection(compatibilityLevel);
        using (var seed = new SqlAzureCommandMock(connection))
        {
            seed.CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (886, 'Ana', '[\"red\",\"blue\"]')";
            seed.ExecuteNonQuery();
        }

        using var command = new SqlAzureCommandMock(connection)
        {
            CommandText = """
                SELECT j.[value] AS Tag
                FROM Users u
                CROSS APPLY dbo.OPENJSON(u.Email) j
                WHERE u.Id = 886
                ORDER BY j.[value]
                """
        };

        if (compatibilityLevel < SqlAzureDbCompatibilityLevels.SqlServer2016)
        {
            var ex = Assert.Throws<NotSupportedException>(() => command.ExecuteReader());
            Assert.Contains("OPENJSON", ex.Message, StringComparison.OrdinalIgnoreCase);
            return;
        }

        using var reader = command.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal("blue", reader.GetString(reader.GetOrdinal("Tag")));
        Assert.True(reader.Read());
        Assert.Equal("red", reader.GetString(reader.GetOrdinal("Tag")));
        Assert.False(reader.Read());
    }

    /// <summary>
    /// EN: Ensures SQL Azure compatibility mode executes schema-qualified OPENJSON WITH explicit schema through CROSS APPLY on the shared SQL Server runtime path.
    /// PT: Garante que o modo de compatibilidade SQL Azure execute OPENJSON qualificado por schema com WITH explicito via CROSS APPLY no caminho compartilhado de runtime do SQL Server.
    /// </summary>
    [Theory]
    [MemberDataSqlAzureCompatibilityLevel]
    public void ExecuteReader_CrossApply_SchemaQualifiedOpenJsonWithSchema_ShouldProjectTypedColumns(int compatibilityLevel)
    {
        using var connection = CreateOpenConnection(compatibilityLevel);
        using (var seed = new SqlAzureCommandMock(connection))
        {
            seed.CommandText = """
                INSERT INTO Users (Id, Name, Email) VALUES (887, 'Ana', '[{"Name":"red","Payload":{"kind":"primary"}}]');
                """;
            seed.ExecuteNonQuery();
        }

        using var command = new SqlAzureCommandMock(connection)
        {
            CommandText = """
                SELECT data.Name AS ColorName, data.PayloadJson AS PayloadJson
                FROM Users u
                CROSS APPLY dbo.OPENJSON(u.Email) WITH (
                    Name NVARCHAR(20) '$.Name',
                    PayloadJson NVARCHAR(MAX) '$.Payload' AS JSON
                ) data
                WHERE u.Id = 887
                """
        };

        if (compatibilityLevel < SqlAzureDbCompatibilityLevels.SqlServer2016)
        {
            var ex = Assert.Throws<NotSupportedException>(() => command.ExecuteReader());
            Assert.Contains("OPENJSON", ex.Message, StringComparison.OrdinalIgnoreCase);
            return;
        }

        using var reader = command.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal("red", reader.GetString(reader.GetOrdinal("ColorName")));
        Assert.Equal("""{"kind":"primary"}""", reader.GetString(reader.GetOrdinal("PayloadJson")));
        Assert.False(reader.Read());
    }

    /// <summary>
    /// EN: Ensures SQL Azure compatibility mode exposes schema-qualified STRING_SPLIT enable_ordinal through the shared SQL Server 2022 runtime semantics.
    /// PT: Garante que o modo de compatibilidade SQL Azure exponha STRING_SPLIT qualificado por schema com enable_ordinal pela semantica compartilhada de runtime do SQL Server 2022.
    /// </summary>
    [Theory]
    [MemberDataSqlAzureCompatibilityLevel]
    public void ExecuteReader_CrossApply_SchemaQualifiedStringSplitWithOrdinal_ShouldReturnOrdinalColumn(int compatibilityLevel)
    {
        using var connection = CreateOpenConnection(compatibilityLevel);
        using (var seed = new SqlAzureCommandMock(connection))
        {
            seed.CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (888, 'Ana', 'red,blue,green')";
            seed.ExecuteNonQuery();
        }

        using var command = new SqlAzureCommandMock(connection)
        {
            CommandText = """
                SELECT part.value AS Token, part.ordinal AS TokenOrdinal
                FROM Users u
                CROSS APPLY dbo.STRING_SPLIT(u.Email, ',', 1) part
                WHERE u.Id = 888
                ORDER BY part.ordinal
                """
        };

        if (compatibilityLevel < SqlAzureDbCompatibilityLevels.SqlServer2022)
        {
            var ex = Assert.Throws<NotSupportedException>(() => command.ExecuteReader());
            Assert.Contains("STRING_SPLIT", ex.Message, StringComparison.OrdinalIgnoreCase);
            return;
        }
        using var reader = command.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal("red", reader.GetString(reader.GetOrdinal("Token")));
        Assert.Equal(1L, reader.GetInt64(reader.GetOrdinal("TokenOrdinal")));
        Assert.True(reader.Read());
        Assert.Equal("blue", reader.GetString(reader.GetOrdinal("Token")));
        Assert.Equal(2L, reader.GetInt64(reader.GetOrdinal("TokenOrdinal")));
        Assert.True(reader.Read());
        Assert.Equal("green", reader.GetString(reader.GetOrdinal("Token")));
        Assert.Equal(3L, reader.GetInt64(reader.GetOrdinal("TokenOrdinal")));
        Assert.False(reader.Read());
    }

    /// <summary>
    /// EN: Ensures SQL Azure compatibility mode accepts schema-qualified STRING_SPLIT enable_ordinal numeric text that coerces exactly to 1.
    /// PT: Garante que o modo de compatibilidade SQL Azure aceite texto numerico em STRING_SPLIT qualificado por schema com enable_ordinal que coerce exatamente para 1.
    /// </summary>
    [Theory]
    [MemberDataSqlAzureCompatibilityLevel]
    public void ExecuteReader_CrossApply_SchemaQualifiedStringSplitWithOrdinalNumericTextFlag_ShouldReturnOrdinalColumn(int compatibilityLevel)
    {
        using var connection = CreateOpenConnection(compatibilityLevel);
        using (var seed = new SqlAzureCommandMock(connection))
        {
            seed.CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (846, 'Ana', 'red,blue')";
            seed.ExecuteNonQuery();
        }

        using var command = new SqlAzureCommandMock(connection)
        {
            CommandText = """
                SELECT part.value AS Token, part.ordinal AS TokenOrdinal
                FROM Users u
                CROSS APPLY dbo.STRING_SPLIT(u.Email, ',', '1.0') part
                WHERE u.Id = 846
                ORDER BY part.ordinal
                """
        };

        if (compatibilityLevel < SqlAzureDbCompatibilityLevels.SqlServer2022)
        {
            var ex = Assert.Throws<NotSupportedException>(() => command.ExecuteReader());
            Assert.Contains("STRING_SPLIT", ex.Message, StringComparison.OrdinalIgnoreCase);
            return;
        }
        using var reader = command.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal("red", reader.GetString(reader.GetOrdinal("Token")));
        Assert.Equal(1L, reader.GetInt64(reader.GetOrdinal("TokenOrdinal")));
        Assert.True(reader.Read());
        Assert.Equal("blue", reader.GetString(reader.GetOrdinal("Token")));
        Assert.Equal(2L, reader.GetInt64(reader.GetOrdinal("TokenOrdinal")));
        Assert.False(reader.Read());
    }

    /// <summary>
    /// EN: Ensures SQL Azure compatibility mode rejects schema-qualified STRING_SPLIT enable_ordinal numeric text outside the 0 or 1 subset.
    /// PT: Garante que o modo de compatibilidade SQL Azure rejeite texto numerico em STRING_SPLIT qualificado por schema com enable_ordinal fora do subset 0 ou 1.
    /// </summary>
    [Theory]
    [MemberDataSqlAzureCompatibilityLevel]
    public void ExecuteReader_CrossApply_SchemaQualifiedStringSplitWithOrdinalInvalidNumericTextFlag_ShouldThrow(int compatibilityLevel)
    {
        using var connection = CreateOpenConnection(compatibilityLevel);
        using (var seed = new SqlAzureCommandMock(connection))
        {
            seed.CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (847, 'Ana', 'red,blue')";
            seed.ExecuteNonQuery();
        }

        using var command = new SqlAzureCommandMock(connection)
        {
            CommandText = """
                SELECT part.value
                FROM Users u
                CROSS APPLY dbo.STRING_SPLIT(u.Email, ',', '2.0') part
                WHERE u.Id = 847
                """
        };

        if (compatibilityLevel < SqlAzureDbCompatibilityLevels.SqlServer2022)
        {
            var ex2 = Assert.Throws<NotSupportedException>(() => command.ExecuteReader());
            Assert.Contains("STRING_SPLIT", ex2.Message, StringComparison.OrdinalIgnoreCase);
            return;
        }
        var ex = Assert.Throws<InvalidOperationException>(() => command.ExecuteReader());
        Assert.Contains("enable_ordinal", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures SQL Azure compatibility mode exposes STRING_SPLIT enable_ordinal through the shared SQL Server 2022 runtime semantics.
    /// PT: Garante que o modo de compatibilidade SQL Azure exponha STRING_SPLIT com enable_ordinal pela semantica compartilhada de runtime do SQL Server 2022.
    /// </summary>
    [Theory]
    [MemberDataSqlAzureCompatibilityLevel]
    public void ExecuteReader_CrossApply_StringSplitWithOrdinal_ShouldReturnOrdinalColumn(int compatibilityLevel)
    {
        using var connection = CreateOpenConnection(compatibilityLevel);
        using (var seed = new SqlAzureCommandMock(connection))
        {
            seed.CommandText = """
                INSERT INTO Users (Id, Name, Email) VALUES (841, 'Ana', 'red,blue,green');
                """;
            seed.ExecuteNonQuery();
        }

        using var command = new SqlAzureCommandMock(connection)
        {
            CommandText = """
                SELECT part.value AS Token, part.ordinal AS TokenOrdinal
                FROM Users u
                CROSS APPLY STRING_SPLIT(u.Email, ',', 1) part
                WHERE u.Id = 841
                ORDER BY part.ordinal
                """
        };

        if (compatibilityLevel < SqlAzureDbCompatibilityLevels.SqlServer2022)
        {
            var ex = Assert.Throws<NotSupportedException>(() => command.ExecuteReader());
            Assert.Contains("STRING_SPLIT", ex.Message, StringComparison.OrdinalIgnoreCase);
            return;
        }
        using var reader = command.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal("red", reader.GetString(reader.GetOrdinal("Token")));
        Assert.Equal(1L, reader.GetInt64(reader.GetOrdinal("TokenOrdinal")));
        Assert.True(reader.Read());
        Assert.Equal("blue", reader.GetString(reader.GetOrdinal("Token")));
        Assert.Equal(2L, reader.GetInt64(reader.GetOrdinal("TokenOrdinal")));
        Assert.True(reader.Read());
        Assert.Equal("green", reader.GetString(reader.GetOrdinal("Token")));
        Assert.Equal(3L, reader.GetInt64(reader.GetOrdinal("TokenOrdinal")));
        Assert.False(reader.Read());
    }

    /// <summary>
    /// EN: Ensures SQL Azure compatibility mode accepts decimal enable_ordinal values that coerce exactly to 0 or 1.
    /// PT: Garante que o modo de compatibilidade SQL Azure aceite valores decimais em enable_ordinal que coercem exatamente para 0 ou 1.
    /// </summary>
    [Theory]
    [MemberDataSqlAzureCompatibilityLevel]
    public void ExecuteReader_CrossApply_StringSplitWithOrdinalDecimalFlag_ShouldReturnOrdinalColumn(int compatibilityLevel)
    {
        using var connection = CreateOpenConnection(compatibilityLevel);
        using (var seed = new SqlAzureCommandMock(connection))
        {
            seed.CommandText = """
                INSERT INTO Users (Id, Name, Email) VALUES (842, 'Ana', 'red,blue');
                """;
            seed.ExecuteNonQuery();
        }

        using var command = new SqlAzureCommandMock(connection)
        {
            CommandText = """
                SELECT part.value AS Token, part.ordinal AS TokenOrdinal
                FROM Users u
                CROSS APPLY STRING_SPLIT(u.Email, ',', CAST(1 AS DECIMAL(10,2))) part
                WHERE u.Id = 842
                ORDER BY part.ordinal
                """
        };

        if (compatibilityLevel < SqlAzureDbCompatibilityLevels.SqlServer2022)
        {
            var ex = Assert.Throws<NotSupportedException>(() => command.ExecuteReader());
            Assert.Contains("STRING_SPLIT", ex.Message, StringComparison.OrdinalIgnoreCase);
            return;
        }
        using var reader = command.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal("red", reader.GetString(reader.GetOrdinal("Token")));
        Assert.Equal(1L, reader.GetInt64(reader.GetOrdinal("TokenOrdinal")));
        Assert.True(reader.Read());
        Assert.Equal("blue", reader.GetString(reader.GetOrdinal("Token")));
        Assert.Equal(2L, reader.GetInt64(reader.GetOrdinal("TokenOrdinal")));
        Assert.False(reader.Read());
    }

    /// <summary>
    /// EN: Ensures SQL Azure compatibility mode accepts numeric text enable_ordinal values that coerce exactly to 0 or 1.
    /// PT: Garante que o modo de compatibilidade SQL Azure aceite valores textuais numericos em enable_ordinal que coercem exatamente para 0 ou 1.
    /// </summary>
    [Theory]
    [MemberDataSqlAzureCompatibilityLevel]
    public void ExecuteReader_CrossApply_StringSplitWithOrdinalNumericTextFlag_ShouldReturnOrdinalColumn(int compatibilityLevel)
    {
        using var connection = CreateOpenConnection(compatibilityLevel);
        using (var seed = new SqlAzureCommandMock(connection))
        {
            seed.CommandText = """
                INSERT INTO Users (Id, Name, Email) VALUES (843, 'Ana', 'red,blue');
                """;
            seed.ExecuteNonQuery();
        }

        using var command = new SqlAzureCommandMock(connection)
        {
            CommandText = """
                SELECT part.value AS Token, part.ordinal AS TokenOrdinal
                FROM Users u
                CROSS APPLY STRING_SPLIT(u.Email, ',', '1.0') part
                WHERE u.Id = 843
                ORDER BY part.ordinal
                """
        };

        if (compatibilityLevel < SqlAzureDbCompatibilityLevels.SqlServer2022)
        {
            var ex = Assert.Throws<NotSupportedException>(() => command.ExecuteReader());
            Assert.Contains("STRING_SPLIT", ex.Message, StringComparison.OrdinalIgnoreCase);
            return;
        }
        using var reader = command.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal("red", reader.GetString(reader.GetOrdinal("Token")));
        Assert.Equal(1L, reader.GetInt64(reader.GetOrdinal("TokenOrdinal")));
        Assert.True(reader.Read());
        Assert.Equal("blue", reader.GetString(reader.GetOrdinal("Token")));
        Assert.Equal(2L, reader.GetInt64(reader.GetOrdinal("TokenOrdinal")));
        Assert.False(reader.Read());
    }

    /// <summary>
    /// EN: Ensures SQL Azure compatibility mode accepts numeric text enable_ordinal values that coerce exactly to 0 and suppresses the ordinal column.
    /// PT: Garante que o modo de compatibilidade SQL Azure aceite valores textuais numericos em enable_ordinal que coercem exatamente para 0 e suprima a coluna ordinal.
    /// </summary>
    [Theory]
    [MemberDataSqlAzureCompatibilityLevel]
    public void ExecuteReader_CrossApply_StringSplitWithOrdinalNumericTextZeroFlag_ShouldSuppressOrdinalColumn(int compatibilityLevel)
    {
        using var connection = CreateOpenConnection(compatibilityLevel);
        using (var seed = new SqlAzureCommandMock(connection))
        {
            seed.CommandText = """
                INSERT INTO Users (Id, Name, Email) VALUES (844, 'Ana', 'red,blue');
                """;
            seed.ExecuteNonQuery();
        }

        using var command = new SqlAzureCommandMock(connection)
        {
            CommandText = """
                SELECT part.value AS Token
                FROM Users u
                CROSS APPLY STRING_SPLIT(u.Email, ',', '0.0') part
                WHERE u.Id = 844
                ORDER BY part.value
                """
        };

        if (compatibilityLevel < SqlAzureDbCompatibilityLevels.SqlServer2022)
        {
            var ex = Assert.Throws<NotSupportedException>(() => command.ExecuteReader());
            Assert.Contains("STRING_SPLIT", ex.Message, StringComparison.OrdinalIgnoreCase);
            return;
        }
        using var reader = command.ExecuteReader();
        Assert.Equal(1, reader.FieldCount);
        Assert.Equal("Token", reader.GetName(0));
        Assert.True(reader.Read());
        Assert.Equal("blue", reader.GetString(reader.GetOrdinal("Token")));
        Assert.True(reader.Read());
        Assert.Equal("red", reader.GetString(reader.GetOrdinal("Token")));
        Assert.False(reader.Read());
    }

    /// <summary>
    /// EN: Ensures SQL Azure compatibility mode rejects invalid numeric text enable_ordinal values outside the 0 or 1 subset.
    /// PT: Garante que o modo de compatibilidade SQL Azure rejeite valores textuais numericos invalidos em enable_ordinal fora do subset 0 ou 1.
    /// </summary>
    [Theory]
    [MemberDataSqlAzureCompatibilityLevel]
    public void ExecuteReader_CrossApply_StringSplitWithOrdinalInvalidNumericTextFlag_ShouldThrow(int compatibilityLevel)
    {
        using var connection = CreateOpenConnection(compatibilityLevel);
        using (var seed = new SqlAzureCommandMock(connection))
        {
            seed.CommandText = """
                INSERT INTO Users (Id, Name, Email) VALUES (845, 'Ana', 'red,blue');
                """;
            seed.ExecuteNonQuery();
        }

        using var command = new SqlAzureCommandMock(connection)
        {
            CommandText = """
                SELECT part.value
                FROM Users u
                CROSS APPLY STRING_SPLIT(u.Email, ',', '2.0') part
                WHERE u.Id = 845
                """
        };

        if (compatibilityLevel < SqlAzureDbCompatibilityLevels.SqlServer2022)
        {
            var ex2 = Assert.Throws<NotSupportedException>(() => command.ExecuteReader());
            Assert.Contains("STRING_SPLIT", ex2.Message, StringComparison.OrdinalIgnoreCase);
            return;
        }
        var ex = Assert.Throws<InvalidOperationException>(() => command.ExecuteReader());
        Assert.Contains("enable_ordinal", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ROWCOUNT function tracks the last select row count for SQL Azure compatibility.
    /// PT: Garante que a função ROWCOUNT acompanhe a contagem da última consulta para compatibilidade SQL Azure.
    /// </summary>
    [Fact]
    public void Select_RowCountFunction_ShouldReturnLastSelectRowCount()
    {
        using var connection = CreateOpenConnection();
        using (var seed = new SqlAzureCommandMock(connection))
        {
            seed.CommandText = """
                INSERT INTO Users (Id, Name, Email) VALUES (131, 'RowCount A', NULL);
                INSERT INTO Users (Id, Name, Email) VALUES (132, 'RowCount B', NULL);
                """;
            seed.ExecuteNonQuery();
        }

        using var command = new SqlAzureCommandMock(connection)
        {
            CommandText = "SELECT TOP 1 Name FROM Users ORDER BY Id; SELECT ROWCOUNT();"
        };

        using var reader = command.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal("RowCount A", reader.GetString(0));
        Assert.True(reader.NextResult());
        Assert.True(reader.Read());
        Assert.Equal(1L, Convert.ToInt64(reader.GetValue(0), CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Ensures SQL Azure rejects FOUND_ROWS because compatibility mode exposes ROWCOUNT and @@ROWCOUNT instead.
    /// PT: Garante que o SQL Azure rejeite FOUND_ROWS porque o modo de compatibilidade expoe ROWCOUNT e @@ROWCOUNT no lugar.
    /// </summary>
    [Fact]
    public void Select_FoundRows_ShouldThrowNotSupportedException()
    {
        using var connection = CreateOpenConnection();
        using (var seed = new SqlAzureCommandMock(connection))
        {
            seed.CommandText = """
                INSERT INTO Users (Id, Name, Email) VALUES (141, 'Found A', NULL);
                INSERT INTO Users (Id, Name, Email) VALUES (142, 'Found B', NULL);
                """;
            seed.ExecuteNonQuery();
        }

        using var command = new SqlAzureCommandMock(connection)
        {
            CommandText = "SELECT TOP 1 Name FROM Users ORDER BY Id; SELECT FOUND_ROWS();"
        };

        var ex = Assert.Throws<NotSupportedException>(() => command.ExecuteReader());
        Assert.Contains("FOUND_ROWS", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures @@ROWCOUNT is reset to zero after BEGIN TRANSACTION in SQL Azure compatibility mode.
    /// PT: Garante que @@ROWCOUNT seja resetado para zero após BEGIN TRANSACTION no modo de compatibilidade SQL Azure.
    /// </summary>
    [Fact]
    public void Batch_BeginTransactionThenRowCount_ShouldReturnZero()
    {
        using var connection = CreateOpenConnection();
        using var command = new SqlAzureCommandMock(connection)
        {
            CommandText = "BEGIN TRANSACTION; SELECT @@ROWCOUNT;"
        };

        using var reader = command.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal(0L, Convert.ToInt64(reader.GetValue(0), CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Ensures @@ROWCOUNT remains zero after SAVEPOINT/RELEASE SAVEPOINT batch statements.
    /// PT: Garante que @@ROWCOUNT permaneça zero após instruções de lote SAVEPOINT/RELEASE SAVEPOINT.
    /// </summary>
    [Fact]
    public void Batch_SavepointAndReleaseThenRowCount_ShouldReturnZero()
    {
        using var connection = CreateOpenConnection();
        using var command = new SqlAzureCommandMock(connection)
        {
            CommandText = "BEGIN TRANSACTION; SAVEPOINT sp1; RELEASE SAVEPOINT sp1; SELECT @@ROWCOUNT;"
        };

        using var reader = command.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal(0L, Convert.ToInt64(reader.GetValue(0), CultureInfo.InvariantCulture));
    }
}
