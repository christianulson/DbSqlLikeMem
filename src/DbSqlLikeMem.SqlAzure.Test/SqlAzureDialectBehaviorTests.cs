namespace DbSqlLikeMem.SqlAzure.Test;

/// <summary>
/// EN: Covers SQL Azure dialect behaviors inherited from SQL Server semantics.
/// PT: Cobre comportamentos de dialeto do SQL Azure herdados da semântica do SQL Server.
/// </summary>
public sealed class SqlAzureDialectBehaviorTests
{
    private static SqlAzureConnectionMock CreateOpenConnection()
    {
        var db = new SqlAzureDbMock();
        db.AddTable("Users", [
            new("Id", DbType.Int32, false),
            new("Name", DbType.String, false),
            new("Email", DbType.String, true)
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
    [Fact]
    public void ExecuteReader_CrossApply_OpenJson_ShouldExpandRows()
    {
        using var connection = CreateOpenConnection();
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
    [Fact]
    public void ExecuteReader_CrossApply_OpenJsonWithSchema_ShouldProjectTypedColumns()
    {
        using var connection = CreateOpenConnection();
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
    [Fact]
    public void ExecuteReader_CrossApply_OpenJsonWithQuotedKeyAndIndexPath_ShouldProjectValue()
    {
        using var connection = CreateOpenConnection();
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
    /// EN: Ensures SQL Azure compatibility mode serializes FOR JSON AUTO with INCLUDE_NULL_VALUES and WITHOUT_ARRAY_WRAPPER on the shared SQL Server runtime path.
    /// PT: Garante que o modo de compatibilidade SQL Azure serialize FOR JSON AUTO com INCLUDE_NULL_VALUES e WITHOUT_ARRAY_WRAPPER no caminho compartilhado de runtime do SQL Server.
    /// </summary>
    [Fact]
    public void ExecuteScalar_ForJsonAutoWithOptions_ShouldSerializeSingleObject()
    {
        using var connection = CreateOpenConnection();
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
    /// EN: Ensures SQL Azure compatibility mode exposes STRING_SPLIT enable_ordinal through the shared SQL Server 2022 runtime semantics.
    /// PT: Garante que o modo de compatibilidade SQL Azure exponha STRING_SPLIT com enable_ordinal pela semantica compartilhada de runtime do SQL Server 2022.
    /// </summary>
    [Fact]
    public void ExecuteReader_CrossApply_StringSplitWithOrdinal_ShouldReturnOrdinalColumn()
    {
        using var connection = CreateOpenConnection();
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
