namespace DbSqlLikeMem.MariaDb.Test;

/// <summary>
/// EN: Covers MariaDB runtime behavior built on top of the shared MySQL provider family.
/// PT: Cobre o comportamento de runtime do MariaDB construido sobre a familia compartilhada do provider MySQL.
/// </summary>
public sealed class MariaDbMockTests : XUnitTestBase
{
    /// <summary>
    /// EN: Initializes the MariaDB runtime test fixture.
    /// PT: Inicializa a fixture de testes de runtime do MariaDB.
    /// </summary>
    public MariaDbMockTests(ITestOutputHelper helper)
        : base(helper)
    {
    }

    /// <summary>
    /// EN: Ensures INSERT ... RETURNING returns the inserted projection once the MariaDB version gate is enabled.
    /// PT: Garante que INSERT ... RETURNING retorne a projecao inserida quando o gate de versao do MariaDB estiver habilitado.
    /// </summary>
    [Fact]
    [Trait("Category", "MariaDbMock")]
    public void ExecuteReader_InsertReturning_ShouldReturnInsertedProjection()
    {
        using var connection = CreateOpenConnection(MariaDbDbVersions.Version10_5);
        using var command = new MySqlCommandMock(connection)
        {
            CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (901, 'Returning Insert', 'insert@maria.test') RETURNING Id, Name AS user_name"
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(901, reader.GetInt32(reader.GetOrdinal("Id")));
        Assert.Equal("Returning Insert", reader.GetString(reader.GetOrdinal("user_name")));
        Assert.False(reader.Read());
    }

    /// <summary>
    /// EN: Ensures INSERT ... SET with ON DUPLICATE KEY UPDATE returns the updated row projection in MariaDB.
    /// PT: Garante que INSERT ... SET com ON DUPLICATE KEY UPDATE retorne a projecao da linha atualizada no MariaDB.
    /// </summary>
    [Fact]
    [Trait("Category", "MariaDbMock")]
    public void ExecuteReader_InsertSetOnDuplicateKeyUpdateReturning_ShouldReturnUpdatedProjection()
    {
        using var connection = CreateOpenConnection(MariaDbDbVersions.Version10_5);
        using (var setup = new MySqlCommandMock(connection)
        {
            CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (1401, 'Original', 'original@maria.test')"
        })
        {
            setup.ExecuteNonQuery();
        }

        using var command = new MySqlCommandMock(connection)
        {
            CommandText = """
                INSERT INTO Users SET Id = 1401, Name = 'Updated', Email = 'updated@maria.test'
                ON DUPLICATE KEY UPDATE
                    Name = VALUES(Name),
                    Email = VALUES(Email)
                RETURNING Id, Name, Email
                """
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(1401, reader.GetInt32(reader.GetOrdinal("Id")));
        Assert.Equal("Updated", reader.GetString(reader.GetOrdinal("Name")));
        Assert.Equal("updated@maria.test", reader.GetString(reader.GetOrdinal("Email")));
        Assert.False(reader.Read());

        var row = Assert.Single(connection.GetTable("Users"));
        Assert.Equal("Updated", Convert.ToString(row[1], CultureInfo.InvariantCulture));
        Assert.Equal("updated@maria.test", Convert.ToString(row[2], CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Ensures INSERT ... SET with LOW_PRIORITY, PARTITION, and ON DUPLICATE KEY UPDATE returns the updated row projection in MariaDB.
    /// PT: Garante que INSERT ... SET com LOW_PRIORITY, PARTITION e ON DUPLICATE KEY UPDATE retorne a projecao da linha atualizada no MariaDB.
    /// </summary>
    [Fact]
    [Trait("Category", "MariaDbMock")]
    public void ExecuteReader_InsertSetWithModifiersAndOnDuplicateKeyUpdateReturning_ShouldReturnUpdatedProjection()
    {
        using var connection = CreateOpenConnection(MariaDbDbVersions.Version10_5);
        using (var setup = new MySqlCommandMock(connection)
        {
            CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (1601, 'Original', 'original@maria.test')"
        })
        {
            setup.ExecuteNonQuery();
        }

        using var command = new MySqlCommandMock(connection)
        {
            CommandText = """
                INSERT LOW_PRIORITY INTO Users PARTITION (p0)
                SET Id = 1601, Name = 'Replacement', Email = 'repl@maria.test'
                ON DUPLICATE KEY UPDATE
                    Name = VALUES(Name),
                    Email = VALUES(Email)
                RETURNING Id, Name, Email
                """
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(1601, reader.GetInt32(reader.GetOrdinal("Id")));
        Assert.Equal("Replacement", reader.GetString(reader.GetOrdinal("Name")));
        Assert.Equal("repl@maria.test", reader.GetString(reader.GetOrdinal("Email")));
        Assert.False(reader.Read());
        Assert.Equal("Replacement", Convert.ToString(connection.GetTable("Users")[0][1], CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Ensures INSERT ... ON DUPLICATE KEY UPDATE returns the updated row projection when MariaDB resolves a duplicate key.
    /// PT: Garante que INSERT ... ON DUPLICATE KEY UPDATE retorne a projecao da linha atualizada quando o MariaDB resolver uma chave duplicada.
    /// </summary>
    [Fact]
    [Trait("Category", "MariaDbMock")]
    public void ExecuteReader_InsertOnDuplicateKeyUpdateReturning_ShouldReturnUpdatedProjection()
    {
        using var connection = CreateOpenConnection(MariaDbDbVersions.Version10_5);
        using (var setup = new MySqlCommandMock(connection)
        {
            CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (910, 'Original', 'original@maria.test')"
        })
        {
            setup.ExecuteNonQuery();
        }

        using var command = new MySqlCommandMock(connection)
        {
            CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (910, 'Updated', 'updated@maria.test') ON DUPLICATE KEY UPDATE Name = VALUES(Name), Email = VALUES(Email) RETURNING Id, Name, Email"
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(910, reader.GetInt32(reader.GetOrdinal("Id")));
        Assert.Equal("Updated", reader.GetString(reader.GetOrdinal("Name")));
        Assert.Equal("updated@maria.test", reader.GetString(reader.GetOrdinal("Email")));
        Assert.False(reader.Read());

        var row = Assert.Single(connection.GetTable("Users"));
        Assert.Equal("Updated", Convert.ToString(row[1], CultureInfo.InvariantCulture));
        Assert.Equal("updated@maria.test", Convert.ToString(row[2], CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Ensures multi-row INSERT ... ON DUPLICATE KEY UPDATE returns one projection per affected row and keeps batch updates consistent.
    /// PT: Garante que INSERT ... ON DUPLICATE KEY UPDATE com multiplas linhas retorne uma projecao por linha afetada e mantenha as atualizacoes em lote consistentes.
    /// </summary>
    [Fact]
    [Trait("Category", "MariaDbMock")]
    public void ExecuteReader_InsertOnDuplicateKeyUpdateMultiRowReturning_ShouldReturnAllProjections()
    {
        using var connection = CreateOpenConnection(MariaDbDbVersions.Version10_5);
        using (var setup = new MySqlCommandMock(connection)
        {
            CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (930, 'Original', 'original@maria.test')"
        })
        {
            setup.ExecuteNonQuery();
        }

        using var command = new MySqlCommandMock(connection)
        {
            CommandText = """
                INSERT INTO Users (Id, Name, Email)
                VALUES
                    (930, 'Updated', 'updated@maria.test'),
                    (931, 'Inserted', 'inserted@maria.test')
                ON DUPLICATE KEY UPDATE
                    Name = VALUES(Name),
                    Email = VALUES(Email)
                RETURNING Id, Name
                """
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(930, reader.GetInt32(reader.GetOrdinal("Id")));
        Assert.Equal("Updated", reader.GetString(reader.GetOrdinal("Name")));

        Assert.True(reader.Read());
        Assert.Equal(931, reader.GetInt32(reader.GetOrdinal("Id")));
        Assert.Equal("Inserted", reader.GetString(reader.GetOrdinal("Name")));
        Assert.False(reader.Read());

        var rows = connection.GetTable("Users");
        Assert.Equal(2, rows.Count);
        Assert.Contains(rows, row => Convert.ToInt32(row[0], CultureInfo.InvariantCulture) == 930);
        Assert.Contains(rows, row => Convert.ToInt32(row[0], CultureInfo.InvariantCulture) == 931);
    }

    /// <summary>
    /// EN: Ensures INSERT ... SELECT returns the inserted projection when MariaDB enables RETURNING.
    /// PT: Garante que INSERT ... SELECT retorne a projecao inserida quando o MariaDB habilita RETURNING.
    /// </summary>
    [Fact]
    [Trait("Category", "MariaDbMock")]
    public void ExecuteReader_InsertSelectReturning_ShouldReturnInsertedProjection()
    {
        var (_, rawConnection) = DbMockConnectionFactory.CreateMariaDbWithTables(
            db =>
            {
                var source = db.AddTable("SourceUsers", [
                    new("Id", DbType.Int32, false),
                    new("Name", DbType.String, false),
                    new("Email", DbType.String, true)
                ]);
                source.AddPrimaryKeyIndexes("Id");

                var target = db.AddTable("ArchiveUsers", [
                    new("Id", DbType.Int32, false),
                    new("Name", DbType.String, false),
                    new("Email", DbType.String, true)
                ]);
                target.AddPrimaryKeyIndexes("Id");

                source.Add(new Dictionary<int, object?> { [0] = 1001, [1] = "Ana", [2] = "ana@maria.test" });
                source.Add(new Dictionary<int, object?> { [0] = 1002, [1] = "Bia", [2] = "bia@maria.test" });
            });

        using var connection = Assert.IsType<MariaDbConnectionMock>(rawConnection);
        using var command = new MySqlCommandMock(connection)
        {
            CommandText = """
                INSERT INTO ArchiveUsers (Id, Name, Email)
                SELECT Id, Name, Email
                FROM SourceUsers
                WHERE Id >= 1001
                ORDER BY Id
                RETURNING Id, Name, Email
                """
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(1001, reader.GetInt32(reader.GetOrdinal("Id")));
        Assert.Equal("Ana", reader.GetString(reader.GetOrdinal("Name")));
        Assert.Equal("ana@maria.test", reader.GetString(reader.GetOrdinal("Email")));

        Assert.True(reader.Read());
        Assert.Equal(1002, reader.GetInt32(reader.GetOrdinal("Id")));
        Assert.Equal("Bia", reader.GetString(reader.GetOrdinal("Name")));
        Assert.Equal("bia@maria.test", reader.GetString(reader.GetOrdinal("Email")));
        Assert.False(reader.Read());

        var archiveRows = connection.Db.GetTable("ArchiveUsers");
        Assert.Equal(2, archiveRows.Count);
        Assert.Contains(archiveRows, row => Convert.ToInt32(row[0], CultureInfo.InvariantCulture) == 1001);
        Assert.Contains(archiveRows, row => Convert.ToInt32(row[0], CultureInfo.InvariantCulture) == 1002);
    }

    /// <summary>
    /// EN: Ensures INSERT ... SELECT with IGNORE skips conflicting rows and returns only inserted projections in MariaDB.
    /// PT: Garante que INSERT ... SELECT com IGNORE ignore linhas conflitantes e retorne apenas as projecoes inseridas no MariaDB.
    /// </summary>
    [Fact]
    [Trait("Category", "MariaDbMock")]
    public void ExecuteReader_InsertSelectIgnore_Returning_ShouldSkipConflictsAndReturnInsertedProjection()
    {
        var (_, rawConnection) = DbMockConnectionFactory.CreateMariaDbWithTables(
            db =>
            {
                var source = db.AddTable("SourceUsers", [
                    new("Id", DbType.Int32, false),
                    new("Name", DbType.String, false),
                    new("Email", DbType.String, true)
                ]);
                source.AddPrimaryKeyIndexes("Id");

                var target = db.AddTable("ArchiveUsers", [
                    new("Id", DbType.Int32, false),
                    new("Name", DbType.String, false),
                    new("Email", DbType.String, true)
                ]);
                target.AddPrimaryKeyIndexes("Id");

                source.Add(new Dictionary<int, object?> { [0] = 1201, [1] = "Ana", [2] = "ana@maria.test" });
                source.Add(new Dictionary<int, object?> { [0] = 1202, [1] = "Bia", [2] = "bia@maria.test" });
                target.Add(new Dictionary<int, object?> { [0] = 1201, [1] = "Legacy", [2] = "legacy@maria.test" });
            });

        using var connection = Assert.IsType<MariaDbConnectionMock>(rawConnection);
        using var command = new MySqlCommandMock(connection)
        {
            CommandText = """
                INSERT IGNORE INTO ArchiveUsers (Id, Name, Email)
                SELECT Id, Name, Email
                FROM SourceUsers
                WHERE Id >= 1201
                ORDER BY Id
                RETURNING Id, Name, Email
                """
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(1202, reader.GetInt32(reader.GetOrdinal("Id")));
        Assert.Equal("Bia", reader.GetString(reader.GetOrdinal("Name")));
        Assert.Equal("bia@maria.test", reader.GetString(reader.GetOrdinal("Email")));
        Assert.False(reader.Read());

        var archiveRows = connection.Db.GetTable("ArchiveUsers");
        Assert.Equal(2, archiveRows.Count);
        Assert.Contains(archiveRows, row => Convert.ToInt32(row[0], CultureInfo.InvariantCulture) == 1201);
        Assert.Contains(archiveRows, row => Convert.ToInt32(row[0], CultureInfo.InvariantCulture) == 1202);
    }

    /// <summary>
    /// EN: Ensures INSERT ... SELECT with ON DUPLICATE KEY UPDATE returns one projection per affected row in MariaDB.
    /// PT: Garante que INSERT ... SELECT com ON DUPLICATE KEY UPDATE retorne uma projecao por linha afetada no MariaDB.
    /// </summary>
    [Fact]
    [Trait("Category", "MariaDbMock")]
    public void ExecuteReader_InsertSelectOnDuplicateKeyUpdateReturning_ShouldReturnAffectedProjections()
    {
        var (_, rawConnection) = DbMockConnectionFactory.CreateMariaDbWithTables(
            db =>
            {
                var source = db.AddTable("SourceUsers", [
                    new("Id", DbType.Int32, false),
                    new("Name", DbType.String, false),
                    new("Email", DbType.String, true)
                ]);
                source.AddPrimaryKeyIndexes("Id");

                var target = db.AddTable("ArchiveUsers", [
                    new("Id", DbType.Int32, false),
                    new("Name", DbType.String, false),
                    new("Email", DbType.String, true)
                ]);
                target.AddPrimaryKeyIndexes("Id");

                source.Add(new Dictionary<int, object?> { [0] = 1101, [1] = "Ana", [2] = "ana@maria.test" });
                source.Add(new Dictionary<int, object?> { [0] = 1102, [1] = "Bia", [2] = "bia@maria.test" });
                target.Add(new Dictionary<int, object?> { [0] = 1101, [1] = "Legacy", [2] = "legacy@maria.test" });
            });

        using var connection = Assert.IsType<MariaDbConnectionMock>(rawConnection);
        using var command = new MySqlCommandMock(connection)
        {
            CommandText = """
                INSERT INTO ArchiveUsers (Id, Name, Email)
                SELECT Id, Name, Email
                FROM SourceUsers
                WHERE Id >= 1101
                ORDER BY Id
                ON DUPLICATE KEY UPDATE
                    Name = VALUES(Name),
                    Email = VALUES(Email)
                RETURNING Id, Name, Email
                """
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(1101, reader.GetInt32(reader.GetOrdinal("Id")));
        Assert.Equal("Ana", reader.GetString(reader.GetOrdinal("Name")));
        Assert.Equal("ana@maria.test", reader.GetString(reader.GetOrdinal("Email")));

        Assert.True(reader.Read());
        Assert.Equal(1102, reader.GetInt32(reader.GetOrdinal("Id")));
        Assert.Equal("Bia", reader.GetString(reader.GetOrdinal("Name")));
        Assert.Equal("bia@maria.test", reader.GetString(reader.GetOrdinal("Email")));
        Assert.False(reader.Read());

        var archiveRows = connection.Db.GetTable("ArchiveUsers");
        Assert.Equal(2, archiveRows.Count);
        Assert.Contains(archiveRows, row => Convert.ToInt32(row[0], CultureInfo.InvariantCulture) == 1101 && Convert.ToString(row[1], CultureInfo.InvariantCulture) == "Ana");
        Assert.Contains(archiveRows, row => Convert.ToInt32(row[0], CultureInfo.InvariantCulture) == 1102 && Convert.ToString(row[1], CultureInfo.InvariantCulture) == "Bia");
    }

    /// <summary>
    /// EN: Ensures REPLACE ... SELECT returns one projection per affected row in MariaDB.
    /// PT: Garante que REPLACE ... SELECT retorne uma projecao por linha afetada no MariaDB.
    /// </summary>
    [Fact]
    [Trait("Category", "MariaDbMock")]
    public void ExecuteReader_ReplaceSelectReturning_ShouldReturnAffectedProjections()
    {
        var (_, rawConnection) = DbMockConnectionFactory.CreateMariaDbWithTables(
            db =>
            {
                var source = db.AddTable("SourceUsers", [
                    new("Id", DbType.Int32, false),
                    new("Name", DbType.String, false),
                    new("Email", DbType.String, true)
                ]);
                source.AddPrimaryKeyIndexes("Id");

                var target = db.AddTable("ArchiveUsers", [
                    new("Id", DbType.Int32, false),
                    new("Name", DbType.String, false),
                    new("Email", DbType.String, true)
                ]);
                target.AddPrimaryKeyIndexes("Id");

                source.Add(new Dictionary<int, object?> { [0] = 1201, [1] = "Ana", [2] = "ana@maria.test" });
                source.Add(new Dictionary<int, object?> { [0] = 1202, [1] = "Bia", [2] = "bia@maria.test" });
                target.Add(new Dictionary<int, object?> { [0] = 1201, [1] = "Legacy", [2] = "legacy@maria.test" });
            });

        using var connection = Assert.IsType<MariaDbConnectionMock>(rawConnection);
        using var command = new MySqlCommandMock(connection)
        {
            CommandText = """
                REPLACE INTO ArchiveUsers (Id, Name, Email)
                SELECT Id, Name, Email
                FROM SourceUsers
                WHERE Id >= 1201
                ORDER BY Id
                RETURNING Id, Name, Email
                """
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(1201, reader.GetInt32(reader.GetOrdinal("Id")));
        Assert.Equal("Ana", reader.GetString(reader.GetOrdinal("Name")));
        Assert.Equal("ana@maria.test", reader.GetString(reader.GetOrdinal("Email")));

        Assert.True(reader.Read());
        Assert.Equal(1202, reader.GetInt32(reader.GetOrdinal("Id")));
        Assert.Equal("Bia", reader.GetString(reader.GetOrdinal("Name")));
        Assert.Equal("bia@maria.test", reader.GetString(reader.GetOrdinal("Email")));
        Assert.False(reader.Read());

        var archiveRows = connection.Db.GetTable("ArchiveUsers");
        Assert.Equal(2, archiveRows.Count);
        Assert.Contains(archiveRows, row => Convert.ToInt32(row[0], CultureInfo.InvariantCulture) == 1201 && Convert.ToString(row[1], CultureInfo.InvariantCulture) == "Ana");
        Assert.Contains(archiveRows, row => Convert.ToInt32(row[0], CultureInfo.InvariantCulture) == 1202 && Convert.ToString(row[1], CultureInfo.InvariantCulture) == "Bia");
    }

    /// <summary>
    /// EN: Ensures DELETE ... RETURNING returns the deleted row snapshot once the MariaDB version gate is enabled.
    /// PT: Garante que DELETE ... RETURNING retorne o snapshot da linha excluida quando o gate de versao do MariaDB estiver habilitado.
    /// </summary>
    [Fact]
    [Trait("Category", "MariaDbMock")]
    public void ExecuteReader_DeleteReturning_ShouldReturnDeletedRowSnapshot()
    {
        using var connection = CreateOpenConnection(MariaDbDbVersions.Version10_5);
        using (var setup = new MySqlCommandMock(connection)
        {
            CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (902, 'To Delete', 'delete@maria.test')"
        })
        {
            setup.ExecuteNonQuery();
        }

        using var command = new MySqlCommandMock(connection)
        {
            CommandText = "DELETE FROM Users WHERE Id = 902 RETURNING Id, Name"
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(902, reader.GetInt32(reader.GetOrdinal("Id")));
        Assert.Equal("To Delete", reader.GetString(reader.GetOrdinal("Name")));
        Assert.False(reader.Read());
        Assert.DoesNotContain(connection.GetTable("Users"), row => Convert.ToInt32(row[0], CultureInfo.InvariantCulture) == 902);
    }

    /// <summary>
    /// EN: Ensures REPLACE ... RETURNING replaces an existing MariaDB row and returns the inserted projection.
    /// PT: Garante que REPLACE ... RETURNING substitua uma linha existente do MariaDB e retorne a projecao inserida.
    /// </summary>
    [Fact]
    [Trait("Category", "MariaDbMock")]
    public void ExecuteReader_ReplaceReturning_ShouldReplaceExistingRow()
    {
        using var connection = CreateOpenConnection(MariaDbDbVersions.Version10_5);
        using (var setup = new MySqlCommandMock(connection)
        {
            CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (904, 'Original', 'orig@maria.test')"
        })
        {
            setup.ExecuteNonQuery();
        }

        using var command = new MySqlCommandMock(connection)
        {
            CommandText = "REPLACE INTO Users (Id, Name, Email) VALUES (904, 'Replacement', 'repl@maria.test') RETURNING Id, Name"
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(904, reader.GetInt32(reader.GetOrdinal("Id")));
        Assert.Equal("Replacement", reader.GetString(reader.GetOrdinal("Name")));
        Assert.False(reader.Read());
        Assert.Single(connection.GetTable("Users"));
        Assert.Equal("Replacement", Convert.ToString(connection.GetTable("Users")[0][1], CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Ensures REPLACE ... SET with RETURNING replaces an existing MariaDB row and returns the inserted projection.
    /// PT: Garante que REPLACE ... SET com RETURNING substitua uma linha existente do MariaDB e retorne a projecao inserida.
    /// </summary>
    [Fact]
    [Trait("Category", "MariaDbMock")]
    public void ExecuteReader_ReplaceSetReturning_ShouldReplaceExistingRow()
    {
        using var connection = CreateOpenConnection(MariaDbDbVersions.Version10_5);
        using (var setup = new MySqlCommandMock(connection)
        {
            CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (1501, 'Original', 'orig@maria.test')"
        })
        {
            setup.ExecuteNonQuery();
        }

        using var command = new MySqlCommandMock(connection)
        {
            CommandText = """
                REPLACE INTO Users SET Id = 1501, Name = 'Replacement', Email = 'repl@maria.test'
                RETURNING Id, Name, Email
                """
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(1501, reader.GetInt32(reader.GetOrdinal("Id")));
        Assert.Equal("Replacement", reader.GetString(reader.GetOrdinal("Name")));
        Assert.Equal("repl@maria.test", reader.GetString(reader.GetOrdinal("Email")));
        Assert.False(reader.Read());
        Assert.Equal("Replacement", Convert.ToString(connection.GetTable("Users")[0][1], CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Ensures multi-row REPLACE returns one projection per affected row while keeping the final table state consistent.
    /// PT: Garante que REPLACE com multiplas linhas retorne uma projecao por linha afetada e mantenha o estado final da tabela consistente.
    /// </summary>
    [Fact]
    [Trait("Category", "MariaDbMock")]
    public void ExecuteReader_ReplaceMultiRowReturning_ShouldReturnAllProjections()
    {
        using var connection = CreateOpenConnection(MariaDbDbVersions.Version10_5);
        using (var setup = new MySqlCommandMock(connection)
        {
            CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (920, 'Keep', 'keep@maria.test')"
        })
        {
            setup.ExecuteNonQuery();
        }

        using var command = new MySqlCommandMock(connection)
        {
            CommandText = """
                REPLACE INTO Users (Id, Name, Email)
                VALUES
                    (920, 'Keep Updated', 'keep.updated@maria.test'),
                    (921, 'Fresh Row', 'fresh@maria.test')
                RETURNING Id, Name
                """
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(920, reader.GetInt32(reader.GetOrdinal("Id")));
        Assert.Equal("Keep Updated", reader.GetString(reader.GetOrdinal("Name")));

        Assert.True(reader.Read());
        Assert.Equal(921, reader.GetInt32(reader.GetOrdinal("Id")));
        Assert.Equal("Fresh Row", reader.GetString(reader.GetOrdinal("Name")));
        Assert.False(reader.Read());

        var rows = connection.GetTable("Users");
        Assert.Equal(2, rows.Count);
        Assert.Contains(rows, row => Convert.ToInt32(row[0], CultureInfo.InvariantCulture) == 920);
        Assert.Contains(rows, row => Convert.ToInt32(row[0], CultureInfo.InvariantCulture) == 921);
    }

    /// <summary>
    /// EN: Ensures MariaDB executes INSERT with VALUE, LOW_PRIORITY, and PARTITION syntax through the shared runtime path.
    /// PT: Garante que o MariaDB execute INSERT com sintaxe VALUE, LOW_PRIORITY e PARTITION pelo caminho compartilhado de runtime.
    /// </summary>
    [Fact]
    [Trait("Category", "MariaDbMock")]
    public void ExecuteReader_InsertValuePartition_ShouldReturnInsertedProjection()
    {
        using var connection = CreateOpenConnection(MariaDbDbVersions.Version10_5);
        using var command = new MySqlCommandMock(connection)
        {
            CommandText = "INSERT LOW_PRIORITY INTO Users PARTITION (p0) VALUE (905, 'Partition Insert', 'partition@maria.test') RETURNING Id, Name"
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(905, reader.GetInt32(reader.GetOrdinal("Id")));
        Assert.Equal("Partition Insert", reader.GetString(reader.GetOrdinal("Name")));
        Assert.False(reader.Read());
        Assert.Single(connection.GetTable("Users"));
    }

    /// <summary>
    /// EN: Ensures MariaDB executes INSERT and REPLACE statements with the DELAYED modifier through the shared runtime path.
    /// PT: Garante que o MariaDB execute statements INSERT e REPLACE com o modificador DELAYED pelo caminho compartilhado de runtime.
    /// </summary>
    [Fact]
    [Trait("Category", "MariaDbMock")]
    public void ExecuteReader_DelayedInsertAndReplace_ShouldReturnProjectedRows()
    {
        using var connection = CreateOpenConnection(MariaDbDbVersions.Version10_5);

        using (var insert = new MySqlCommandMock(connection)
        {
            CommandText = "INSERT DELAYED INTO Users VALUE (907, 'Delayed Insert', 'delayed@maria.test') RETURNING Id, Name"
        })
        using (var reader = insert.ExecuteReader())
        {
            Assert.True(reader.Read());
            Assert.Equal(907, reader.GetInt32(reader.GetOrdinal("Id")));
            Assert.Equal("Delayed Insert", reader.GetString(reader.GetOrdinal("Name")));
            Assert.False(reader.Read());
        }

        using (var setup = new MySqlCommandMock(connection)
        {
            CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (908, 'Original', 'orig-delay@maria.test')"
        })
        {
            setup.ExecuteNonQuery();
        }

        using var replace = new MySqlCommandMock(connection)
        {
            CommandText = "REPLACE DELAYED INTO Users VALUE (908, 'Delayed Replace', 'replace@maria.test') RETURNING Id, Name"
        };

        using var replaceReader = replace.ExecuteReader();

        Assert.True(replaceReader.Read());
        Assert.Equal(908, replaceReader.GetInt32(replaceReader.GetOrdinal("Id")));
        Assert.Equal("Delayed Replace", replaceReader.GetString(replaceReader.GetOrdinal("Name")));
        Assert.False(replaceReader.Read());
        Assert.Equal("Delayed Replace", Convert.ToString(connection.GetTable("Users")[1][1], CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Ensures MariaDB INSERT IGNORE skips duplicate rows and returns only the inserted projections.
    /// PT: Garante que INSERT IGNORE do MariaDB ignore linhas duplicadas e retorne apenas as projecoes inseridas.
    /// </summary>
    [Fact]
    [Trait("Category", "MariaDbMock")]
    public void ExecuteReader_InsertIgnore_ShouldSkipDuplicateRowsAndReturnInsertedProjection()
    {
        using var connection = CreateOpenConnection(MariaDbDbVersions.Version10_5);
        using (var setup = new MySqlCommandMock(connection)
        {
            CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (910, 'Seed', 'seed@maria.test')"
        })
        {
            setup.ExecuteNonQuery();
        }

        using var command = new MySqlCommandMock(connection)
        {
            CommandText = """
                INSERT IGNORE INTO Users (Id, Name, Email)
                VALUES (910, 'Dup', 'dup@maria.test'),
                       (911, 'Fresh', 'fresh@maria.test')
                RETURNING Id, Name
                """
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(911, reader.GetInt32(reader.GetOrdinal("Id")));
        Assert.Equal("Fresh", reader.GetString(reader.GetOrdinal("Name")));
        Assert.False(reader.Read());

        var rows = connection.GetTable("Users");
        Assert.Equal(2, rows.Count);
        Assert.Contains(rows, row => Convert.ToInt32(row[0], CultureInfo.InvariantCulture) == 910);
        Assert.Contains(rows, row => Convert.ToInt32(row[0], CultureInfo.InvariantCulture) == 911);
    }

    /// <summary>
    /// EN: Ensures MariaDB executes INSERT ... SET through the shared runtime path and returns the inserted projection.
    /// PT: Garante que o MariaDB execute INSERT ... SET pelo caminho compartilhado de runtime e retorne a projecao inserida.
    /// </summary>
    [Fact]
    [Trait("Category", "MariaDbMock")]
    public void ExecuteReader_InsertSet_ShouldReturnInsertedProjection()
    {
        using var connection = CreateOpenConnection(MariaDbDbVersions.Version10_5);
        using var command = new MySqlCommandMock(connection)
        {
            CommandText = "INSERT INTO Users SET Id = 908, Name = 'Set Insert', Email = 'set@maria.test' RETURNING Id, Name"
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(908, reader.GetInt32(reader.GetOrdinal("Id")));
        Assert.Equal("Set Insert", reader.GetString(reader.GetOrdinal("Name")));
        Assert.False(reader.Read());
        Assert.Single(connection.GetTable("Users"));
    }

    /// <summary>
    /// EN: Ensures MariaDB executes REPLACE with VALUE, LOW_PRIORITY, and PARTITION syntax through the shared runtime path.
    /// PT: Garante que o MariaDB execute REPLACE com sintaxe VALUE, LOW_PRIORITY e PARTITION pelo caminho compartilhado de runtime.
    /// </summary>
    [Fact]
    [Trait("Category", "MariaDbMock")]
    public void ExecuteReader_ReplaceValuePartition_ShouldReplaceExistingRow()
    {
        using var connection = CreateOpenConnection(MariaDbDbVersions.Version10_5);
        using (var setup = new MySqlCommandMock(connection)
        {
            CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (906, 'Original', 'orig2@maria.test')"
        })
        {
            setup.ExecuteNonQuery();
        }

        using var command = new MySqlCommandMock(connection)
        {
            CommandText = "REPLACE LOW_PRIORITY INTO Users PARTITION (p0) VALUE (906, 'Replacement', 'repl2@maria.test') RETURNING Id, Name"
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(906, reader.GetInt32(reader.GetOrdinal("Id")));
        Assert.Equal("Replacement", reader.GetString(reader.GetOrdinal("Name")));
        Assert.False(reader.Read());
        Assert.Single(connection.GetTable("Users"));
        Assert.Equal("Replacement", Convert.ToString(connection.GetTable("Users")[0][1], CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Ensures MariaDB rejects aggregate functions inside RETURNING at execution time.
    /// PT: Garante que o MariaDB rejeite funcoes de agregacao dentro de RETURNING no tempo de execucao.
    /// </summary>
    [Fact]
    [Trait("Category", "MariaDbMock")]
    public void ExecuteReader_InsertReturning_Aggregate_ShouldThrow()
    {
        using var connection = CreateOpenConnection(MariaDbDbVersions.Version10_5);
        using var command = new MySqlCommandMock(connection)
        {
            CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (907, 'Agg', 'agg@maria.test') RETURNING COUNT(*)"
        };

        var ex = Assert.Throws<InvalidOperationException>(() => command.ExecuteReader());

        Assert.Contains("aggregate", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures MariaDB still rejects RETURNING before the version gate is enabled.
    /// PT: Garante que o MariaDB ainda rejeite RETURNING antes de o gate de versao estar habilitado.
    /// </summary>
    [Fact]
    [Trait("Category", "MariaDbMock")]
    public void ExecuteReader_InsertReturning_BeforeGate_ShouldThrow()
    {
        using var connection = CreateOpenConnection(MariaDbDbVersions.Version10_3);
        using var command = new MySqlCommandMock(connection)
        {
            CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (903, 'Blocked', NULL) RETURNING Id"
        };

        var ex = Assert.Throws<NotSupportedException>(() => command.ExecuteReader());

        Assert.Contains(SqlConst.RETURNING, ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures MariaDB executes sequence DDL plus NEXT/PREVIOUS VALUE FOR through the shared runtime path.
    /// PT: Garante que o MariaDB execute DDL de sequence e NEXT/PREVIOUS VALUE FOR pelo caminho compartilhado de runtime.
    /// </summary>
    [Fact]
    [Trait("Category", "MariaDbMock")]
    public void ExecuteScalar_SequenceFamilies_ShouldWork()
    {
        using var connection = CreateOpenConnection(MariaDbDbVersions.Version10_3);
        using var command = new MySqlCommandMock(connection)
        {
            CommandText = "CREATE SEQUENCE seq_users START WITH 10 INCREMENT BY 2"
        };
        Assert.Equal(0, command.ExecuteNonQuery());

        command.CommandText = "SELECT NEXT VALUE FOR seq_users";
        Assert.Equal(10L, Convert.ToInt64(command.ExecuteScalar(), CultureInfo.InvariantCulture));

        command.CommandText = "SELECT PREVIOUS VALUE FOR seq_users";
        Assert.Equal(10L, Convert.ToInt64(command.ExecuteScalar(), CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Ensures MariaDB can consume sequence expressions inside INSERT statements through the shared runtime path.
    /// PT: Garante que o MariaDB consiga consumir expressoes de sequence dentro de INSERTs pelo caminho compartilhado de runtime.
    /// </summary>
    [Fact]
    [Trait("Category", "MariaDbMock")]
    public void ExecuteNonQuery_InsertWithSequenceExpressions_ShouldWork()
    {
        using var connection = CreateOpenConnection(MariaDbDbVersions.Version10_3);
        using var command = new MySqlCommandMock(connection)
        {
            CommandText = """
                CREATE SEQUENCE seq_users START WITH 20 INCREMENT BY 5;
                INSERT INTO Users (Id, Name, Email) VALUES (NEXT VALUE FOR seq_users, 'Ana', NULL);
                INSERT INTO Users (Id, Name, Email) VALUES (NEXT VALUE FOR seq_users, 'Bia', NULL);
                """
        };

        Assert.Equal(2, command.ExecuteNonQuery());

        var users = connection.GetTable("Users");
        Assert.Equal(2, users.Count);
        Assert.Equal(20, Convert.ToInt32(users[0][0], CultureInfo.InvariantCulture));
        Assert.Equal(25, Convert.ToInt32(users[1][0], CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Ensures MariaDB evaluates SOUNDS LIKE through the shared runtime path.
    /// PT: Garante que o MariaDB avalie SOUNDS LIKE pelo caminho compartilhado de runtime.
    /// </summary>
    [Fact]
    [Trait("Category", "MariaDbMock")]
    public void ExecuteScalar_SoundsLike_ShouldUseSoundexComparison()
    {
        using var connection = CreateOpenConnection(MariaDbDbVersions.Version10_5);
        using var command = new MySqlCommandMock(connection)
        {
            CommandText = "SELECT CASE WHEN 'Robert' SOUNDS LIKE 'Rupert' THEN 1 ELSE 0 END"
        };

        Assert.Equal(1, Convert.ToInt32(command.ExecuteScalar(), CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Ensures MariaDB exposes _ROWID as the single-column primary key in the shared runtime.
    /// PT: Garante que o MariaDB exponha _ROWID como a chave primaria de coluna unica no runtime compartilhado.
    /// </summary>
    [Fact]
    [Trait("Category", "MariaDbMock")]
    public void ExecuteScalar_RowId_ShouldResolveToPrimaryKey()
    {
        using var connection = CreateOpenConnection(MariaDbDbVersions.Version10_5);
        using (var setup = new MySqlCommandMock(connection)
        {
            CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (909, 'RowId', 'rowid@maria.test')"
        })
        {
            setup.ExecuteNonQuery();
        }

        using var command = new MySqlCommandMock(connection)
        {
            CommandText = "SELECT _ROWID FROM Users WHERE Id = 909"
        };

        Assert.Equal(909, Convert.ToInt32(command.ExecuteScalar(), CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Ensures MariaDB can materialize JSON_TABLE rows with ordinality and typed PATH columns in the shared runtime.
    /// PT: Garante que o MariaDB consiga materializar linhas de JSON_TABLE com ordinality e colunas PATH tipadas no runtime compartilhado.
    /// </summary>
    [Fact]
    [Trait("Category", "MariaDbMock")]
    public void ExecuteReader_JsonTable_ShouldProjectRows()
    {
        using var connection = CreateOpenConnection(MariaDbDbVersions.Version10_6);
        using var command = new MySqlCommandMock(connection)
        {
            CommandText = """
                SELECT jt.ord, jt.Id, jt.Name
                FROM JSON_TABLE(
                    '[{"id":1,"name":"Ana"},{"id":2,"name":"Bia"}]',
                    '$[*]' COLUMNS(
                        ord FOR ORDINALITY,
                        Id INT PATH '$.id',
                        Name VARCHAR(50) PATH '$.name'
                    )
                ) jt
                ORDER BY jt.ord
                """
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(1L, reader.GetInt64(reader.GetOrdinal("ord")));
        Assert.Equal(1, reader.GetInt32(reader.GetOrdinal("Id")));
        Assert.Equal("Ana", reader.GetString(reader.GetOrdinal("Name")));

        Assert.True(reader.Read());
        Assert.Equal(2L, reader.GetInt64(reader.GetOrdinal("ord")));
        Assert.Equal(2, reader.GetInt32(reader.GetOrdinal("Id")));
        Assert.Equal("Bia", reader.GetString(reader.GetOrdinal("Name")));
        Assert.False(reader.Read());
    }

    /// <summary>
    /// EN: Ensures MariaDB can correlate JSON_TABLE with an outer row source and expand one JSON row per matching parent row.
    /// PT: Garante que o MariaDB consiga correlacionar JSON_TABLE com uma fonte de linha externa e expandir uma linha JSON por linha pai correspondente.
    /// </summary>
    [Fact]
    [Trait("Category", "MariaDbMock")]
    public void ExecuteReader_JsonTable_WithOuterRowSource_ShouldExpandCorrelatedRows()
    {
        var (_, rawConnection) = DbMockConnectionFactory.CreateMariaDbWithTables(
            db =>
            {
                var orders = db.AddTable("Orders", [
                    new("Id", DbType.Int32, false),
                    new("Tags", DbType.String, true)
                ]);
                orders.AddPrimaryKeyIndexes("Id");

                orders.Add(new Dictionary<int, object?> { [0] = 101, [1] = """["vip","new"]""" });
                orders.Add(new Dictionary<int, object?> { [0] = 102, [1] = "[]" });
                orders.Add(new Dictionary<int, object?> { [0] = 103, [1] = """["beta"]""" });
            });

        using var connection = Assert.IsType<MariaDbConnectionMock>(rawConnection);
        using var command = new MySqlCommandMock(connection)
        {
            CommandText = """
                SELECT o.Id AS OrderId, jt.Tag
                FROM Orders o,
                     JSON_TABLE(
                    o.Tags,
                    '$[*]' COLUMNS(
                        Tag VARCHAR(20) PATH '$'
                    )
                ) jt
                ORDER BY o.Id, jt.Tag
                """
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(101, reader.GetInt32(reader.GetOrdinal("OrderId")));
        Assert.Equal("vip", reader.GetString(reader.GetOrdinal("Tag")));

        Assert.True(reader.Read());
        Assert.Equal(101, reader.GetInt32(reader.GetOrdinal("OrderId")));
        Assert.Equal("new", reader.GetString(reader.GetOrdinal("Tag")));

        Assert.True(reader.Read());
        Assert.Equal(103, reader.GetInt32(reader.GetOrdinal("OrderId")));
        Assert.Equal("beta", reader.GetString(reader.GetOrdinal("Tag")));
        Assert.False(reader.Read());
    }

    /// <summary>
    /// EN: Ensures MariaDB skips correlated JSON_TABLE expansion when the outer JSON document is NULL.
    /// PT: Garante que o MariaDB omita a expansao correlacionada de JSON_TABLE quando o documento JSON externo for NULL.
    /// </summary>
    [Fact]
    [Trait("Category", "MariaDbMock")]
    public void ExecuteReader_JsonTable_WithNullOuterDocument_ShouldSkipParentRows()
    {
        var (_, rawConnection) = DbMockConnectionFactory.CreateMariaDbWithTables(
            db =>
            {
                var orders = db.AddTable("Orders", [
                    new("Id", DbType.Int32, false),
                    new("Tags", DbType.String, true)
                ]);
                orders.AddPrimaryKeyIndexes("Id");

                orders.Add(new Dictionary<int, object?> { [0] = 201, [1] = null });
                orders.Add(new Dictionary<int, object?> { [0] = 202, [1] = """["solo"]""" });
            });

        using var connection = Assert.IsType<MariaDbConnectionMock>(rawConnection);
        using var command = new MySqlCommandMock(connection)
        {
            CommandText = """
                SELECT o.Id AS OrderId, jt.Tag
                FROM Orders o,
                     JSON_TABLE(
                    o.Tags,
                    '$[*]' COLUMNS(
                        Tag VARCHAR(20) PATH '$'
                    )
                ) jt
                ORDER BY o.Id, jt.Tag
                """
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(202, reader.GetInt32(reader.GetOrdinal("OrderId")));
        Assert.Equal("solo", reader.GetString(reader.GetOrdinal("Tag")));
        Assert.False(reader.Read());
    }

    /// <summary>
    /// EN: Ensures correlated MariaDB JSON_TABLE still expands nested branches when the outer document is populated.
    /// PT: Garante que o JSON_TABLE correlacionado do MariaDB ainda expanda ramos nested quando o documento externo estiver preenchido.
    /// </summary>
    [Fact]
    [Trait("Category", "MariaDbMock")]
    public void ExecuteReader_JsonTable_WithOuterRowSourceAndNestedPath_ShouldExpandNestedRows()
    {
        var (_, rawConnection) = DbMockConnectionFactory.CreateMariaDbWithTables(
            db =>
            {
                var orders = db.AddTable("Orders", [
                    new("Id", DbType.Int32, false),
                    new("Tags", DbType.String, true)
                ]);
                orders.AddPrimaryKeyIndexes("Id");

                orders.Add(new Dictionary<int, object?> { [0] = 301, [1] = """[{"id":1,"tags":["vip","new"]},{"id":2,"tags":["beta"]}]""" });
                orders.Add(new Dictionary<int, object?> { [0] = 302, [1] = """[{"id":3,"tags":[]}]""" });
            });

        using var connection = Assert.IsType<MariaDbConnectionMock>(rawConnection);
        using var command = new MySqlCommandMock(connection)
        {
            CommandText = """
                SELECT o.Id AS OrderId, jt.id AS ItemId, jt.tag_ord AS TagOrd, jt.tag AS Tag
                FROM Orders o,
                     JSON_TABLE(
                    o.Tags,
                    '$[*]' COLUMNS(
                        id INT PATH '$.id',
                        NESTED PATH '$.tags[*]' COLUMNS(
                            tag_ord FOR ORDINALITY,
                            tag VARCHAR(20) PATH '$'
                        )
                    )
                ) jt
                ORDER BY o.Id, jt.id, jt.tag_ord
                """
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(301, reader.GetInt32(reader.GetOrdinal("OrderId")));
        Assert.Equal(1, reader.GetInt32(reader.GetOrdinal("ItemId")));
        Assert.Equal(1L, reader.GetInt64(reader.GetOrdinal("TagOrd")));
        Assert.Equal("vip", reader.GetString(reader.GetOrdinal("Tag")));

        Assert.True(reader.Read());
        Assert.Equal(301, reader.GetInt32(reader.GetOrdinal("OrderId")));
        Assert.Equal(1, reader.GetInt32(reader.GetOrdinal("ItemId")));
        Assert.Equal(2L, reader.GetInt64(reader.GetOrdinal("TagOrd")));
        Assert.Equal("new", reader.GetString(reader.GetOrdinal("Tag")));

        Assert.True(reader.Read());
        Assert.Equal(301, reader.GetInt32(reader.GetOrdinal("OrderId")));
        Assert.Equal(2, reader.GetInt32(reader.GetOrdinal("ItemId")));
        Assert.Equal(1L, reader.GetInt64(reader.GetOrdinal("TagOrd")));
        Assert.Equal("beta", reader.GetString(reader.GetOrdinal("Tag")));
        Assert.False(reader.Read());
    }

    /// <summary>
    /// EN: Ensures correlated MariaDB JSON_TABLE nested branches can apply ON EMPTY fallbacks while expanding outer rows.
    /// PT: Garante que ramos nested de JSON_TABLE correlacionado consigam aplicar fallbacks ON EMPTY enquanto expandem linhas externas.
    /// </summary>
    [Fact]
    [Trait("Category", "MariaDbMock")]
    public void ExecuteReader_JsonTable_WithOuterRowSourceAndNestedDefaultOnEmpty_ShouldUseFallback()
    {
        var (_, rawConnection) = DbMockConnectionFactory.CreateMariaDbWithTables(
            db =>
            {
                var orders = db.AddTable("Orders", [
                    new("Id", DbType.Int32, false),
                    new("Tags", DbType.String, true)
                ]);
                orders.AddPrimaryKeyIndexes("Id");

                orders.Add(new Dictionary<int, object?> { [0] = 501, [1] = """[{"id":1,"tags":[{"name":"vip"},{}]},{"id":2,"tags":[{"name":"beta"}]}]""" });
            });

        using var connection = Assert.IsType<MariaDbConnectionMock>(rawConnection);
        using var command = new MySqlCommandMock(connection)
        {
            CommandText = """
                SELECT o.Id AS OrderId, jt.id AS ItemId, jt.tag_ord AS TagOrd, jt.tag_name AS TagName
                FROM Orders o,
                     JSON_TABLE(
                    o.Tags,
                    '$[*]' COLUMNS(
                        id INT PATH '$.id',
                        NESTED PATH '$.tags[*]' COLUMNS(
                            tag_ord FOR ORDINALITY,
                            tag_name VARCHAR(20) PATH '$.name' DEFAULT 'fallback' ON EMPTY
                        )
                    )
                ) jt
                ORDER BY o.Id, jt.id, jt.tag_ord
                """
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(501, reader.GetInt32(reader.GetOrdinal("OrderId")));
        Assert.Equal(1, reader.GetInt32(reader.GetOrdinal("ItemId")));
        Assert.Equal(1L, reader.GetInt64(reader.GetOrdinal("TagOrd")));
        Assert.Equal("vip", reader.GetString(reader.GetOrdinal("TagName")));

        Assert.True(reader.Read());
        Assert.Equal(501, reader.GetInt32(reader.GetOrdinal("OrderId")));
        Assert.Equal(1, reader.GetInt32(reader.GetOrdinal("ItemId")));
        Assert.Equal(2L, reader.GetInt64(reader.GetOrdinal("TagOrd")));
        Assert.Equal("fallback", reader.GetString(reader.GetOrdinal("TagName")));

        Assert.True(reader.Read());
        Assert.Equal(501, reader.GetInt32(reader.GetOrdinal("OrderId")));
        Assert.Equal(2, reader.GetInt32(reader.GetOrdinal("ItemId")));
        Assert.Equal(1L, reader.GetInt64(reader.GetOrdinal("TagOrd")));
        Assert.Equal("beta", reader.GetString(reader.GetOrdinal("TagName")));
        Assert.False(reader.Read());
    }

    /// <summary>
    /// EN: Ensures correlated MariaDB JSON_TABLE applies ON EMPTY defaults on the root path while expanding nested rows.
    /// PT: Garante que JSON_TABLE correlacionado do MariaDB aplique defaults de ON EMPTY no caminho raiz enquanto expande linhas nested.
    /// </summary>
    [Fact]
    [Trait("Category", "MariaDbMock")]
    public void ExecuteReader_JsonTable_WithOuterRowSourceAndRootDefaultOnEmptyAndNestedPath_ShouldUseFallback()
    {
        var (_, rawConnection) = DbMockConnectionFactory.CreateMariaDbWithTables(
            db =>
            {
                var orders = db.AddTable("Orders", [
                    new("Id", DbType.Int32, false),
                    new("Tags", DbType.String, true)
                ]);
                orders.AddPrimaryKeyIndexes("Id");

                orders.Add(new Dictionary<int, object?> { [0] = 801, [1] = """[{"tags":[{"name":"vip"},{"name":"new"}]}]""" });
                orders.Add(new Dictionary<int, object?> { [0] = 802, [1] = """[{"id":2,"tags":[{"name":"beta"}]}]""" });
            });

        using var connection = Assert.IsType<MariaDbConnectionMock>(rawConnection);
        using var command = new MySqlCommandMock(connection)
        {
            CommandText = """
                SELECT o.Id AS OrderId, jt.item_id AS ItemId, jt.tag_ord AS TagOrd, jt.tag_name AS TagName
                FROM Orders o,
                     JSON_TABLE(
                    o.Tags,
                    '$[*]' COLUMNS(
                        item_id INT PATH '$.id' DEFAULT '0' ON EMPTY,
                        NESTED PATH '$.tags[*]' COLUMNS(
                            tag_ord FOR ORDINALITY,
                            tag_name VARCHAR(20) PATH '$.name'
                        )
                    )
                ) jt
                ORDER BY o.Id, jt.item_id, jt.tag_ord
                """
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(801, reader.GetInt32(reader.GetOrdinal("OrderId")));
        Assert.Equal(0, reader.GetInt32(reader.GetOrdinal("ItemId")));
        Assert.Equal(1L, reader.GetInt64(reader.GetOrdinal("TagOrd")));
        Assert.Equal("vip", reader.GetString(reader.GetOrdinal("TagName")));

        Assert.True(reader.Read());
        Assert.Equal(801, reader.GetInt32(reader.GetOrdinal("OrderId")));
        Assert.Equal(0, reader.GetInt32(reader.GetOrdinal("ItemId")));
        Assert.Equal(2L, reader.GetInt64(reader.GetOrdinal("TagOrd")));
        Assert.Equal("new", reader.GetString(reader.GetOrdinal("TagName")));

        Assert.True(reader.Read());
        Assert.Equal(802, reader.GetInt32(reader.GetOrdinal("OrderId")));
        Assert.Equal(2, reader.GetInt32(reader.GetOrdinal("ItemId")));
        Assert.Equal(1L, reader.GetInt64(reader.GetOrdinal("TagOrd")));
        Assert.Equal("beta", reader.GetString(reader.GetOrdinal("TagName")));
        Assert.False(reader.Read());
    }

    /// <summary>
    /// EN: Ensures correlated MariaDB JSON_TABLE can combine root ordinality and EXISTS PATH while expanding outer rows.
    /// PT: Garante que JSON_TABLE correlacionado do MariaDB consiga combinar ordinality raiz e EXISTS PATH enquanto expande linhas externas.
    /// </summary>
    [Fact]
    [Trait("Category", "MariaDbMock")]
    public void ExecuteReader_JsonTable_WithOuterRowSourceAndRootOrdinalityAndExistsPath_ShouldReturnFlags()
    {
        var (_, rawConnection) = DbMockConnectionFactory.CreateMariaDbWithTables(
            db =>
            {
                var orders = db.AddTable("Orders", [
                    new("Id", DbType.Int32, false),
                    new("Tags", DbType.String, true)
                ]);
                orders.AddPrimaryKeyIndexes("Id");

                orders.Add(new Dictionary<int, object?> { [0] = 951, [1] = """[{"id":1,"tag":"vip"},{"id":2}]""" });
            });

        using var connection = Assert.IsType<MariaDbConnectionMock>(rawConnection);
        using var command = new MySqlCommandMock(connection)
        {
            CommandText = """
                SELECT o.Id AS OrderId, jt.row_ord AS RowOrd, jt.item_id AS ItemId, jt.has_tag AS HasTag
                FROM Orders o,
                     JSON_TABLE(
                    o.Tags,
                    '$[*]' COLUMNS(
                        row_ord FOR ORDINALITY,
                        item_id INT PATH '$.id',
                        has_tag INT EXISTS PATH '$.tag'
                    )
                ) jt
                ORDER BY o.Id, jt.row_ord
                """
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(951, reader.GetInt32(reader.GetOrdinal("OrderId")));
        Assert.Equal(1L, reader.GetInt64(reader.GetOrdinal("RowOrd")));
        Assert.Equal(1, reader.GetInt32(reader.GetOrdinal("ItemId")));
        Assert.Equal(1, reader.GetInt32(reader.GetOrdinal("HasTag")));

        Assert.True(reader.Read());
        Assert.Equal(951, reader.GetInt32(reader.GetOrdinal("OrderId")));
        Assert.Equal(2L, reader.GetInt64(reader.GetOrdinal("RowOrd")));
        Assert.Equal(2, reader.GetInt32(reader.GetOrdinal("ItemId")));
        Assert.Equal(0, reader.GetInt32(reader.GetOrdinal("HasTag")));
        Assert.False(reader.Read());
    }

    /// <summary>
    /// EN: Ensures correlated MariaDB JSON_TABLE can combine a strict root path with multiple nested fallback branches.
    /// PT: Garante que JSON_TABLE correlacionado do MariaDB consiga combinar um caminho raiz strict com multiplos ramos nested de fallback.
    /// </summary>
    [Fact]
    [Trait("Category", "MariaDbMock")]
    public void ExecuteReader_JsonTable_WithOuterRowSourceAndStrictRowPathAndNestedFallbackBranches_ShouldProjectRows()
    {
        var (_, rawConnection) = DbMockConnectionFactory.CreateMariaDbWithTables(
            db =>
            {
                var orders = db.AddTable("Orders", [
                    new("Id", DbType.Int32, false),
                    new("Tags", DbType.String, true)
                ]);
                orders.AddPrimaryKeyIndexes("Id");

                orders.Add(new Dictionary<int, object?> { [0] = 991, [1] = """[{"id":1,"tags":[{"name":"vip"},{}],"metrics":[{"value":{"x":1}},{"value":42}]},{"id":2,"tags":[{"name":"new"}],"metrics":[{"value":7}]}]""" });
            });

        using var connection = Assert.IsType<MariaDbConnectionMock>(rawConnection);
        using var command = new MySqlCommandMock(connection)
        {
            CommandText = """
                SELECT o.Id AS OrderId, jt.item_id AS ItemId, jt.tag_ord AS TagOrd, jt.tag_name AS TagName, jt.metric_ord AS MetricOrd, jt.tag_value AS TagValue
                FROM Orders o,
                     JSON_TABLE(
                    o.Tags,
                    'strict $[*]' COLUMNS(
                        item_id INT PATH '$.id',
                        NESTED PATH '$.tags[*]' COLUMNS(
                            tag_ord FOR ORDINALITY,
                            tag_name VARCHAR(20) PATH '$.name' DEFAULT 'fallback' ON EMPTY
                        ),
                        NESTED PATH '$.metrics[*]' COLUMNS(
                            metric_ord FOR ORDINALITY,
                            tag_value INT PATH '$.value' DEFAULT '99' ON ERROR
                        )
                    )
                ) jt
                ORDER BY o.Id, jt.item_id, jt.tag_ord, jt.metric_ord
                """
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(991, reader.GetInt32(reader.GetOrdinal("OrderId")));
        Assert.Equal(1, reader.GetInt32(reader.GetOrdinal("ItemId")));
        Assert.Equal(1L, reader.GetInt64(reader.GetOrdinal("TagOrd")));
        Assert.Equal("vip", reader.GetString(reader.GetOrdinal("TagName")));
        Assert.True(reader.IsDBNull(reader.GetOrdinal("MetricOrd")));
        Assert.True(reader.IsDBNull(reader.GetOrdinal("TagValue")));

        Assert.True(reader.Read());
        Assert.Equal(991, reader.GetInt32(reader.GetOrdinal("OrderId")));
        Assert.Equal(1, reader.GetInt32(reader.GetOrdinal("ItemId")));
        Assert.Equal(2L, reader.GetInt64(reader.GetOrdinal("TagOrd")));
        Assert.Equal("fallback", reader.GetString(reader.GetOrdinal("TagName")));
        Assert.True(reader.IsDBNull(reader.GetOrdinal("MetricOrd")));
        Assert.True(reader.IsDBNull(reader.GetOrdinal("TagValue")));

        Assert.True(reader.Read());
        Assert.Equal(991, reader.GetInt32(reader.GetOrdinal("OrderId")));
        Assert.Equal(1, reader.GetInt32(reader.GetOrdinal("ItemId")));
        Assert.True(reader.IsDBNull(reader.GetOrdinal("TagOrd")));
        Assert.True(reader.IsDBNull(reader.GetOrdinal("TagName")));
        Assert.Equal(1L, reader.GetInt64(reader.GetOrdinal("MetricOrd")));
        Assert.Equal(99, reader.GetInt32(reader.GetOrdinal("TagValue")));

        Assert.True(reader.Read());
        Assert.Equal(991, reader.GetInt32(reader.GetOrdinal("OrderId")));
        Assert.Equal(1, reader.GetInt32(reader.GetOrdinal("ItemId")));
        Assert.True(reader.IsDBNull(reader.GetOrdinal("TagOrd")));
        Assert.True(reader.IsDBNull(reader.GetOrdinal("TagName")));
        Assert.Equal(2L, reader.GetInt64(reader.GetOrdinal("MetricOrd")));
        Assert.Equal(42, reader.GetInt32(reader.GetOrdinal("TagValue")));

        Assert.True(reader.Read());
        Assert.Equal(991, reader.GetInt32(reader.GetOrdinal("OrderId")));
        Assert.Equal(2, reader.GetInt32(reader.GetOrdinal("ItemId")));
        Assert.Equal(1L, reader.GetInt64(reader.GetOrdinal("TagOrd")));
        Assert.Equal("new", reader.GetString(reader.GetOrdinal("TagName")));
        Assert.True(reader.IsDBNull(reader.GetOrdinal("MetricOrd")));
        Assert.True(reader.IsDBNull(reader.GetOrdinal("TagValue")));

        Assert.True(reader.Read());
        Assert.Equal(991, reader.GetInt32(reader.GetOrdinal("OrderId")));
        Assert.Equal(2, reader.GetInt32(reader.GetOrdinal("ItemId")));
        Assert.True(reader.IsDBNull(reader.GetOrdinal("TagOrd")));
        Assert.True(reader.IsDBNull(reader.GetOrdinal("TagName")));
        Assert.Equal(1L, reader.GetInt64(reader.GetOrdinal("MetricOrd")));
        Assert.Equal(7, reader.GetInt32(reader.GetOrdinal("TagValue")));
        Assert.False(reader.Read());
    }

    /// <summary>
    /// EN: Ensures correlated MariaDB JSON_TABLE skips outer NULL documents even when nested branches are present.
    /// PT: Garante que JSON_TABLE correlacionado do MariaDB omita documentos externos NULL mesmo quando ramos nested existirem.
    /// </summary>
    [Fact]
    [Trait("Category", "MariaDbMock")]
    public void ExecuteReader_JsonTable_WithNullOuterDocumentAndNestedPath_ShouldSkipParentRows()
    {
        var (_, rawConnection) = DbMockConnectionFactory.CreateMariaDbWithTables(
            db =>
            {
                var orders = db.AddTable("Orders", [
                    new("Id", DbType.Int32, false),
                    new("Tags", DbType.String, true)
                ]);
                orders.AddPrimaryKeyIndexes("Id");

                orders.Add(new Dictionary<int, object?> { [0] = 981, [1] = null });
                orders.Add(new Dictionary<int, object?> { [0] = 982, [1] = """[{"id":2,"tags":[{"name":"beta"}]}]""" });
            });

        using var connection = Assert.IsType<MariaDbConnectionMock>(rawConnection);
        using var command = new MySqlCommandMock(connection)
        {
            CommandText = """
                SELECT o.Id AS OrderId, jt.item_id AS ItemId, jt.tag_ord AS TagOrd, jt.tag_name AS TagName
                FROM Orders o,
                     JSON_TABLE(
                    o.Tags,
                    '$[*]' COLUMNS(
                        item_id INT PATH '$.id',
                        NESTED PATH '$.tags[*]' COLUMNS(
                            tag_ord FOR ORDINALITY,
                            tag_name VARCHAR(20) PATH '$.name'
                        )
                    )
                ) jt
                ORDER BY o.Id, jt.item_id, jt.tag_ord
                """
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(982, reader.GetInt32(reader.GetOrdinal("OrderId")));
        Assert.Equal(2, reader.GetInt32(reader.GetOrdinal("ItemId")));
        Assert.Equal(1L, reader.GetInt64(reader.GetOrdinal("TagOrd")));
        Assert.Equal("beta", reader.GetString(reader.GetOrdinal("TagName")));
        Assert.False(reader.Read());
    }

    /// <summary>
    /// EN: Ensures correlated MariaDB JSON_TABLE nested branches raise an error when ERROR ON EMPTY is configured for a missing value.
    /// PT: Garante que ramos nested de JSON_TABLE correlacionado lancem erro quando ERROR ON EMPTY estiver configurado para um valor ausente.
    /// </summary>
    [Fact]
    [Trait("Category", "MariaDbMock")]
    public void ExecuteReader_JsonTable_WithOuterRowSourceAndNestedErrorOnEmpty_ShouldThrow()
    {
        var (_, rawConnection) = DbMockConnectionFactory.CreateMariaDbWithTables(
            db =>
            {
                var orders = db.AddTable("Orders", [
                    new("Id", DbType.Int32, false),
                    new("Tags", DbType.String, true)
                ]);
                orders.AddPrimaryKeyIndexes("Id");

                orders.Add(new Dictionary<int, object?> { [0] = 601, [1] = """[{"id":1,"tags":[{}]}]""" });
            });

        using var connection = Assert.IsType<MariaDbConnectionMock>(rawConnection);
        using var command = new MySqlCommandMock(connection)
        {
            CommandText = """
                SELECT o.Id AS OrderId, jt.item_id AS ItemId, jt.tag_name AS TagName
                FROM Orders o,
                     JSON_TABLE(
                    o.Tags,
                    '$[*]' COLUMNS(
                        item_id INT PATH '$.id',
                        NESTED PATH '$.tags[*]' COLUMNS(
                            tag_name VARCHAR(20) PATH '$.name' ERROR ON EMPTY
                        )
                    )
                ) jt
                ORDER BY o.Id, jt.item_id
                """
        };

        var ex = Assert.Throws<InvalidOperationException>(() => command.ExecuteReader());

        Assert.Contains("ERROR ON EMPTY", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures correlated MariaDB JSON_TABLE nested branches can apply ON ERROR fallbacks when a nested value is not scalar.
    /// PT: Garante que ramos nested de JSON_TABLE correlacionado consigam aplicar fallbacks ON ERROR quando um valor nested nao for escalar.
    /// </summary>
    [Fact]
    [Trait("Category", "MariaDbMock")]
    public void ExecuteReader_JsonTable_WithOuterRowSourceAndNestedDefaultOnError_ShouldUseFallback()
    {
        var (_, rawConnection) = DbMockConnectionFactory.CreateMariaDbWithTables(
            db =>
            {
                var orders = db.AddTable("Orders", [
                    new("Id", DbType.Int32, false),
                    new("Tags", DbType.String, true)
                ]);
                orders.AddPrimaryKeyIndexes("Id");

                orders.Add(new Dictionary<int, object?> { [0] = 701, [1] = """[{"id":1,"tags":[{"value":{"name":"vip"}},{"value":42}]}]""" });
            });

        using var connection = Assert.IsType<MariaDbConnectionMock>(rawConnection);
        using var command = new MySqlCommandMock(connection)
        {
            CommandText = """
                SELECT o.Id AS OrderId, jt.item_id AS ItemId, jt.tag_ord AS TagOrd, jt.tag_value AS TagValue
                FROM Orders o,
                     JSON_TABLE(
                    o.Tags,
                    '$[*]' COLUMNS(
                        item_id INT PATH '$.id',
                        NESTED PATH '$.tags[*]' COLUMNS(
                            tag_ord FOR ORDINALITY,
                            tag_value INT PATH '$.value' DEFAULT '99' ON ERROR
                        )
                    )
                ) jt
                ORDER BY o.Id, jt.item_id, jt.tag_ord
                """
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(701, reader.GetInt32(reader.GetOrdinal("OrderId")));
        Assert.Equal(1, reader.GetInt32(reader.GetOrdinal("ItemId")));
        Assert.Equal(1L, reader.GetInt64(reader.GetOrdinal("TagOrd")));
        Assert.Equal(99, reader.GetInt32(reader.GetOrdinal("TagValue")));

        Assert.True(reader.Read());
        Assert.Equal(701, reader.GetInt32(reader.GetOrdinal("OrderId")));
        Assert.Equal(1, reader.GetInt32(reader.GetOrdinal("ItemId")));
        Assert.Equal(2L, reader.GetInt64(reader.GetOrdinal("TagOrd")));
        Assert.Equal(42, reader.GetInt32(reader.GetOrdinal("TagValue")));
        Assert.False(reader.Read());
    }

    /// <summary>
    /// EN: Ensures correlated MariaDB JSON_TABLE nested strict paths fail when the nested array is missing.
    /// PT: Garante que caminhos nested strict de JSON_TABLE correlacionado falhem quando o array nested estiver ausente.
    /// </summary>
    [Fact]
    [Trait("Category", "MariaDbMock")]
    public void ExecuteReader_JsonTable_WithOuterRowSourceAndNestedStrictPath_ShouldThrow()
    {
        var (_, rawConnection) = DbMockConnectionFactory.CreateMariaDbWithTables(
            db =>
            {
                var orders = db.AddTable("Orders", [
                    new("Id", DbType.Int32, false),
                    new("Tags", DbType.String, true)
                ]);
                orders.AddPrimaryKeyIndexes("Id");

                orders.Add(new Dictionary<int, object?> { [0] = 801, [1] = """[{"id":1,"tags":[{"name":"vip"}]},{"id":2}]""" });
            });

        using var connection = Assert.IsType<MariaDbConnectionMock>(rawConnection);
        using var command = new MySqlCommandMock(connection)
        {
            CommandText = """
                SELECT o.Id AS OrderId, jt.item_id AS ItemId, jt.tag_name AS TagName
                FROM Orders o,
                     JSON_TABLE(
                    o.Tags,
                    '$[*]' COLUMNS(
                        item_id INT PATH '$.id',
                        NESTED PATH 'strict $.tags[*]' COLUMNS(
                            tag_name VARCHAR(20) PATH '$'
                        )
                    )
                ) jt
                ORDER BY o.Id, jt.item_id
                """
        };

        var ex = Assert.Throws<InvalidOperationException>(() => command.ExecuteReader());

        Assert.Contains("strict nested path", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures correlated MariaDB JSON_TABLE root strict paths fail when the JSON array path does not exist.
    /// PT: Garante que caminhos strict na raiz de JSON_TABLE correlacionado falhem quando o caminho do array JSON nao existir.
    /// </summary>
    [Fact]
    [Trait("Category", "MariaDbMock")]
    public void ExecuteReader_JsonTable_WithOuterRowSourceAndStrictRowPath_ShouldThrow()
    {
        var (_, rawConnection) = DbMockConnectionFactory.CreateMariaDbWithTables(
            db =>
            {
                var orders = db.AddTable("Orders", [
                    new("Id", DbType.Int32, false),
                    new("Tags", DbType.String, true)
                ]);
                orders.AddPrimaryKeyIndexes("Id");

                orders.Add(new Dictionary<int, object?> { [0] = 1001, [1] = """[{"items":[{"id":1}]}]""" });
                orders.Add(new Dictionary<int, object?> { [0] = 1002, [1] = """[{"id":2}]""" });
            });

        using var connection = Assert.IsType<MariaDbConnectionMock>(rawConnection);
        using var command = new MySqlCommandMock(connection)
        {
            CommandText = """
                SELECT o.Id AS OrderId, jt.item_id AS ItemId
                FROM Orders o,
                     JSON_TABLE(
                    o.Tags,
                    'strict $.items[*]' COLUMNS(
                        item_id INT PATH '$.id'
                    )
                ) jt
                ORDER BY o.Id, jt.item_id
                """
        };

        var ex = Assert.Throws<InvalidOperationException>(() => command.ExecuteReader());

        Assert.Contains("strict path", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures correlated MariaDB JSON_TABLE keeps sibling nested branches independent while expanding outer rows.
    /// PT: Garante que JSON_TABLE correlacionado do MariaDB mantenha ramos nested irmaos independentes enquanto expande linhas externas.
    /// </summary>
    [Fact]
    [Trait("Category", "MariaDbMock")]
    public void ExecuteReader_JsonTable_WithOuterRowSourceAndSiblingNestedPaths_ShouldProjectIndependentRows()
    {
        var (_, rawConnection) = DbMockConnectionFactory.CreateMariaDbWithTables(
            db =>
            {
                var orders = db.AddTable("Orders", [
                    new("Id", DbType.Int32, false),
                    new("Tags", DbType.String, true)
                ]);
                orders.AddPrimaryKeyIndexes("Id");

                orders.Add(new Dictionary<int, object?> { [0] = 901, [1] = """{"sizes":[{"size":"small"},{"size":"medium"}],"colors":[{"color":"red"},{"color":"blue"}]}""" });
            });

        using var connection = Assert.IsType<MariaDbConnectionMock>(rawConnection);
        using var command = new MySqlCommandMock(connection)
        {
            CommandText = """
                SELECT o.Id AS OrderId, jt.Size, jt.Color
                FROM Orders o,
                     JSON_TABLE(
                    o.Tags,
                    '$' COLUMNS(
                        NESTED PATH '$.sizes[*]' COLUMNS(
                            Size VARCHAR(20) PATH '$.size'
                        ),
                        NESTED PATH '$.colors[*]' COLUMNS(
                            Color VARCHAR(20) PATH '$.color'
                        )
                    )
                ) jt
                ORDER BY o.Id, jt.Size, jt.Color
                """
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(901, reader.GetInt32(reader.GetOrdinal("OrderId")));
        Assert.Equal("small", reader.GetString(reader.GetOrdinal("Size")));
        Assert.True(reader.IsDBNull(reader.GetOrdinal("Color")));

        Assert.True(reader.Read());
        Assert.Equal(901, reader.GetInt32(reader.GetOrdinal("OrderId")));
        Assert.Equal("medium", reader.GetString(reader.GetOrdinal("Size")));
        Assert.True(reader.IsDBNull(reader.GetOrdinal("Color")));

        Assert.True(reader.Read());
        Assert.Equal(901, reader.GetInt32(reader.GetOrdinal("OrderId")));
        Assert.True(reader.IsDBNull(reader.GetOrdinal("Size")));
        Assert.Equal("red", reader.GetString(reader.GetOrdinal("Color")));

        Assert.True(reader.Read());
        Assert.Equal(901, reader.GetInt32(reader.GetOrdinal("OrderId")));
        Assert.True(reader.IsDBNull(reader.GetOrdinal("Size")));
        Assert.Equal("blue", reader.GetString(reader.GetOrdinal("Color")));
        Assert.False(reader.Read());
    }

    /// <summary>
    /// EN: Ensures correlated MariaDB JSON_TABLE keeps sibling nested branches independent while preserving ordinality on each branch.
    /// PT: Garante que JSON_TABLE correlacionado do MariaDB mantenha ramos nested irmaos independentes preservando ordinality em cada ramo.
    /// </summary>
    [Fact]
    [Trait("Category", "MariaDbMock")]
    public void ExecuteReader_JsonTable_WithOuterRowSourceAndSiblingNestedPathsAndOrdinality_ShouldProjectIndependentRows()
    {
        var (_, rawConnection) = DbMockConnectionFactory.CreateMariaDbWithTables(
            db =>
            {
                var orders = db.AddTable("Orders", [
                    new("Id", DbType.Int32, false),
                    new("Tags", DbType.String, true)
                ]);
                orders.AddPrimaryKeyIndexes("Id");

                orders.Add(new Dictionary<int, object?> { [0] = 902, [1] = """{"sizes":[{"size":"small"},{"size":"medium"}],"colors":[{"color":"red"},{"color":"blue"}]}""" });
            });

        using var connection = Assert.IsType<MariaDbConnectionMock>(rawConnection);
        using var command = new MySqlCommandMock(connection)
        {
            CommandText = """
                SELECT o.Id AS OrderId, jt.SizeOrd, jt.Size, jt.ColorOrd, jt.Color
                FROM Orders o,
                     JSON_TABLE(
                    o.Tags,
                    '$' COLUMNS(
                        NESTED PATH '$.sizes[*]' COLUMNS(
                            SizeOrd FOR ORDINALITY,
                            Size VARCHAR(20) PATH '$.size'
                        ),
                        NESTED PATH '$.colors[*]' COLUMNS(
                            ColorOrd FOR ORDINALITY,
                            Color VARCHAR(20) PATH '$.color'
                        )
                    )
                ) jt
                ORDER BY o.Id, jt.SizeOrd, jt.ColorOrd
                """
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(902, reader.GetInt32(reader.GetOrdinal("OrderId")));
        Assert.Equal(1L, reader.GetInt64(reader.GetOrdinal("SizeOrd")));
        Assert.Equal("small", reader.GetString(reader.GetOrdinal("Size")));
        Assert.True(reader.IsDBNull(reader.GetOrdinal("ColorOrd")));
        Assert.True(reader.IsDBNull(reader.GetOrdinal("Color")));

        Assert.True(reader.Read());
        Assert.Equal(902, reader.GetInt32(reader.GetOrdinal("OrderId")));
        Assert.Equal(2L, reader.GetInt64(reader.GetOrdinal("SizeOrd")));
        Assert.Equal("medium", reader.GetString(reader.GetOrdinal("Size")));
        Assert.True(reader.IsDBNull(reader.GetOrdinal("ColorOrd")));
        Assert.True(reader.IsDBNull(reader.GetOrdinal("Color")));

        Assert.True(reader.Read());
        Assert.Equal(902, reader.GetInt32(reader.GetOrdinal("OrderId")));
        Assert.True(reader.IsDBNull(reader.GetOrdinal("SizeOrd")));
        Assert.True(reader.IsDBNull(reader.GetOrdinal("Size")));
        Assert.Equal(1L, reader.GetInt64(reader.GetOrdinal("ColorOrd")));
        Assert.Equal("red", reader.GetString(reader.GetOrdinal("Color")));

        Assert.True(reader.Read());
        Assert.Equal(902, reader.GetInt32(reader.GetOrdinal("OrderId")));
        Assert.True(reader.IsDBNull(reader.GetOrdinal("SizeOrd")));
        Assert.True(reader.IsDBNull(reader.GetOrdinal("Size")));
        Assert.Equal(2L, reader.GetInt64(reader.GetOrdinal("ColorOrd")));
        Assert.Equal("blue", reader.GetString(reader.GetOrdinal("Color")));
        Assert.False(reader.Read());
    }

    /// <summary>
    /// EN: Ensures correlated MariaDB JSON_TABLE keeps sibling nested branches independent while mixing EXISTS PATH and ordinality on each branch.
    /// PT: Garante que JSON_TABLE correlacionado do MariaDB mantenha ramos nested irmaos independentes misturando EXISTS PATH e ordinality em cada ramo.
    /// </summary>
    [Fact]
    [Trait("Category", "MariaDbMock")]
    public void ExecuteReader_JsonTable_WithOuterRowSourceAndSiblingNestedExistsAndOrdinality_ShouldProjectIndependentRows()
    {
        var (_, rawConnection) = DbMockConnectionFactory.CreateMariaDbWithTables(
            db =>
            {
                var orders = db.AddTable("Orders", [
                    new("Id", DbType.Int32, false),
                    new("Tags", DbType.String, true)
                ]);
                orders.AddPrimaryKeyIndexes("Id");

                orders.Add(new Dictionary<int, object?> { [0] = 903, [1] = """{"sizes":[{"size":"small"},{"size":"medium"}],"colors":[{"color":"red"},{"shade":"blue"}]}""" });
            });

        using var connection = Assert.IsType<MariaDbConnectionMock>(rawConnection);
        using var command = new MySqlCommandMock(connection)
        {
            CommandText = """
                SELECT o.Id AS OrderId, jt.SizeOrd, jt.Size, jt.ColorOrd, jt.HasColor
                FROM Orders o,
                     JSON_TABLE(
                    o.Tags,
                    '$' COLUMNS(
                        NESTED PATH '$.sizes[*]' COLUMNS(
                            SizeOrd FOR ORDINALITY,
                            Size VARCHAR(20) PATH '$.size'
                        ),
                        NESTED PATH '$.colors[*]' COLUMNS(
                            ColorOrd FOR ORDINALITY,
                            HasColor INT EXISTS PATH '$.color'
                        )
                    )
                ) jt
                ORDER BY o.Id, jt.SizeOrd, jt.ColorOrd
                """
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(903, reader.GetInt32(reader.GetOrdinal("OrderId")));
        Assert.Equal(1L, reader.GetInt64(reader.GetOrdinal("SizeOrd")));
        Assert.Equal("small", reader.GetString(reader.GetOrdinal("Size")));
        Assert.True(reader.IsDBNull(reader.GetOrdinal("ColorOrd")));
        Assert.True(reader.IsDBNull(reader.GetOrdinal("HasColor")));

        Assert.True(reader.Read());
        Assert.Equal(903, reader.GetInt32(reader.GetOrdinal("OrderId")));
        Assert.Equal(2L, reader.GetInt64(reader.GetOrdinal("SizeOrd")));
        Assert.Equal("medium", reader.GetString(reader.GetOrdinal("Size")));
        Assert.True(reader.IsDBNull(reader.GetOrdinal("ColorOrd")));
        Assert.True(reader.IsDBNull(reader.GetOrdinal("HasColor")));

        Assert.True(reader.Read());
        Assert.Equal(903, reader.GetInt32(reader.GetOrdinal("OrderId")));
        Assert.True(reader.IsDBNull(reader.GetOrdinal("SizeOrd")));
        Assert.True(reader.IsDBNull(reader.GetOrdinal("Size")));
        Assert.Equal(1L, reader.GetInt64(reader.GetOrdinal("ColorOrd")));
        Assert.Equal(1, reader.GetInt32(reader.GetOrdinal("HasColor")));

        Assert.True(reader.Read());
        Assert.Equal(903, reader.GetInt32(reader.GetOrdinal("OrderId")));
        Assert.True(reader.IsDBNull(reader.GetOrdinal("SizeOrd")));
        Assert.True(reader.IsDBNull(reader.GetOrdinal("Size")));
        Assert.Equal(2L, reader.GetInt64(reader.GetOrdinal("ColorOrd")));
        Assert.Equal(0, reader.GetInt32(reader.GetOrdinal("HasColor")));
        Assert.False(reader.Read());
    }

    /// <summary>
    /// EN: Ensures correlated MariaDB JSON_TABLE nested EXISTS PATH columns return 1/0 flags for each nested row.
    /// PT: Garante que colunas nested EXISTS PATH de JSON_TABLE correlacionado no MariaDB retornem flags 1/0 para cada linha nested.
    /// </summary>
    [Fact]
    [Trait("Category", "MariaDbMock")]
    public void ExecuteReader_JsonTable_WithOuterRowSourceAndNestedExistsPath_ShouldReturnFlags()
    {
        var (_, rawConnection) = DbMockConnectionFactory.CreateMariaDbWithTables(
            db =>
            {
                var orders = db.AddTable("Orders", [
                    new("Id", DbType.Int32, false),
                    new("Tags", DbType.String, true)
                ]);
                orders.AddPrimaryKeyIndexes("Id");

                orders.Add(new Dictionary<int, object?> { [0] = 401, [1] = """[{"id":1,"tags":[{"name":"vip"},{"name":"new"}]},{"id":2,"tags":[{}]}]""" });
            });

        using var connection = Assert.IsType<MariaDbConnectionMock>(rawConnection);
        using var command = new MySqlCommandMock(connection)
        {
            CommandText = """
                SELECT o.Id AS OrderId, jt.item_id AS ItemId, jt.has_tag AS HasTag
                FROM Orders o,
                     JSON_TABLE(
                    o.Tags,
                    '$[*]' COLUMNS(
                        item_id INT PATH '$.id',
                        NESTED PATH '$.tags[*]' COLUMNS(
                            has_tag INT EXISTS PATH '$.name'
                        )
                    )
                ) jt
                ORDER BY o.Id, jt.item_id, jt.has_tag
                """
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(401, reader.GetInt32(reader.GetOrdinal("OrderId")));
        Assert.Equal(1, reader.GetInt32(reader.GetOrdinal("ItemId")));
        Assert.Equal(1, reader.GetInt32(reader.GetOrdinal("HasTag")));

        Assert.True(reader.Read());
        Assert.Equal(401, reader.GetInt32(reader.GetOrdinal("OrderId")));
        Assert.Equal(1, reader.GetInt32(reader.GetOrdinal("ItemId")));
        Assert.Equal(1, reader.GetInt32(reader.GetOrdinal("HasTag")));

        Assert.True(reader.Read());
        Assert.Equal(401, reader.GetInt32(reader.GetOrdinal("OrderId")));
        Assert.Equal(2, reader.GetInt32(reader.GetOrdinal("ItemId")));
        Assert.Equal(0, reader.GetInt32(reader.GetOrdinal("HasTag")));
        Assert.False(reader.Read());
    }

    /// <summary>
    /// EN: Ensures strict JSON_TABLE row paths fail when the root array path does not exist in MariaDB.
    /// PT: Garante que caminhos strict em JSON_TABLE falhem quando o caminho raiz do array nao existir no MariaDB.
    /// </summary>
    [Fact]
    [Trait("Category", "MariaDbMock")]
    public void ExecuteReader_JsonTable_WithStrictRowPathMissing_ShouldThrow()
    {
        using var connection = CreateOpenConnection(MariaDbDbVersions.Version10_6);
        using var command = new MySqlCommandMock(connection)
        {
            CommandText = """
                SELECT jt.Id
                FROM JSON_TABLE(
                    '[{"id":1}]',
                    'strict $.items[*]' COLUMNS(
                        Id INT PATH '$.id'
                    )
                ) jt
                """
        };

        var ex = Assert.Throws<InvalidOperationException>(() => command.ExecuteReader());

        Assert.Contains("strict path", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures MariaDB JSON_TABLE supports EXISTS PATH columns with 1/0 semantics in the shared runtime.
    /// PT: Garante que JSON_TABLE do MariaDB suporte colunas EXISTS PATH com semantica 1/0 no runtime compartilhado.
    /// </summary>
    [Fact]
    [Trait("Category", "MariaDbMock")]
    public void ExecuteReader_JsonTable_WithExistsPath_ShouldReturnFlags()
    {
        using var connection = CreateOpenConnection(MariaDbDbVersions.Version10_6);
        using var command = new MySqlCommandMock(connection)
        {
            CommandText = """
                SELECT jt.Id, jt.HasEmail
                FROM JSON_TABLE(
                    '[{"id":1,"email":"ana@test.dev"},{"id":2}]',
                    '$[*]' COLUMNS(
                        Id INT PATH '$.id',
                        HasEmail INT EXISTS PATH '$.email'
                    )
                ) jt
                ORDER BY jt.Id
                """
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(1, reader.GetInt32(reader.GetOrdinal("Id")));
        Assert.Equal(1, reader.GetInt32(reader.GetOrdinal("HasEmail")));

        Assert.True(reader.Read());
        Assert.Equal(2, reader.GetInt32(reader.GetOrdinal("Id")));
        Assert.Equal(0, reader.GetInt32(reader.GetOrdinal("HasEmail")));
        Assert.False(reader.Read());
    }

    /// <summary>
    /// EN: Ensures MariaDB JSON_TABLE applies ON EMPTY defaults for missing PATH values in the shared runtime.
    /// PT: Garante que JSON_TABLE do MariaDB aplique defaults de ON EMPTY para valores PATH ausentes no runtime compartilhado.
    /// </summary>
    [Fact]
    [Trait("Category", "MariaDbMock")]
    public void ExecuteReader_JsonTable_WithDefaultOnEmpty_ShouldUseFallback()
    {
        using var connection = CreateOpenConnection(MariaDbDbVersions.Version10_6);
        using var command = new MySqlCommandMock(connection)
        {
            CommandText = """
                SELECT jt.Id, jt.Title
                FROM JSON_TABLE(
                    '[{"id":1,"title":"Ana"},{"id":2}]',
                    '$[*]' COLUMNS(
                        Id INT PATH '$.id',
                        Title VARCHAR(30) PATH '$.title' DEFAULT 'fallback' ON EMPTY
                    )
                ) jt
                ORDER BY jt.Id
                """
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(1, reader.GetInt32(reader.GetOrdinal("Id")));
        Assert.Equal("Ana", reader.GetString(reader.GetOrdinal("Title")));

        Assert.True(reader.Read());
        Assert.Equal(2, reader.GetInt32(reader.GetOrdinal("Id")));
        Assert.Equal("fallback", reader.GetString(reader.GetOrdinal("Title")));
        Assert.False(reader.Read());
    }

    /// <summary>
    /// EN: Ensures MariaDB JSON_TABLE applies ON ERROR defaults when a PATH resolves to a non-scalar JSON value.
    /// PT: Garante que JSON_TABLE do MariaDB aplique defaults de ON ERROR quando um PATH resolve para um valor JSON nao escalar.
    /// </summary>
    [Fact]
    [Trait("Category", "MariaDbMock")]
    public void ExecuteReader_JsonTable_WithDefaultOnError_ShouldUseFallback()
    {
        using var connection = CreateOpenConnection(MariaDbDbVersions.Version10_6);
        using var command = new MySqlCommandMock(connection)
        {
            CommandText = """
                SELECT jt.Id, jt.TagValue
                FROM JSON_TABLE(
                    '[{"id":1,"tag":{"name":"vip"}},{"id":2,"tag":42}]',
                    '$[*]' COLUMNS(
                        Id INT PATH '$.id',
                        TagValue INT PATH '$.tag' DEFAULT '99' ON ERROR
                    )
                ) jt
                ORDER BY jt.Id
                """
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(1, reader.GetInt32(reader.GetOrdinal("Id")));
        Assert.Equal(99, reader.GetInt32(reader.GetOrdinal("TagValue")));

        Assert.True(reader.Read());
        Assert.Equal(2, reader.GetInt32(reader.GetOrdinal("Id")));
        Assert.Equal(42, reader.GetInt32(reader.GetOrdinal("TagValue")));
        Assert.False(reader.Read());
    }

    /// <summary>
    /// EN: Ensures MariaDB JSON_TABLE raises an error when ON EMPTY is configured to fail on a missing PATH.
    /// PT: Garante que JSON_TABLE do MariaDB lance erro quando ON EMPTY estiver configurado para falhar em PATH ausente.
    /// </summary>
    [Fact]
    [Trait("Category", "MariaDbMock")]
    public void ExecuteReader_JsonTable_WithErrorOnEmpty_ShouldThrow()
    {
        using var connection = CreateOpenConnection(MariaDbDbVersions.Version10_6);
        using var command = new MySqlCommandMock(connection)
        {
            CommandText = """
                SELECT jt.Id, jt.Title
                FROM JSON_TABLE(
                    '[{"id":1,"title":"Ana"},{"id":2}]',
                    '$[*]' COLUMNS(
                        Id INT PATH '$.id',
                        Title VARCHAR(30) PATH '$.title' ERROR ON EMPTY
                    )
                ) jt
                ORDER BY jt.Id
                """
        };

        var ex = Assert.Throws<InvalidOperationException>(() => command.ExecuteReader());

        Assert.Contains("Title", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures MariaDB JSON_TABLE can expand NESTED PATH rows while preserving parent columns in the shared runtime.
    /// PT: Garante que JSON_TABLE do MariaDB consiga expandir linhas de NESTED PATH preservando as colunas pai no runtime compartilhado.
    /// </summary>
    [Fact]
    [Trait("Category", "MariaDbMock")]
    public void ExecuteReader_JsonTable_WithNestedPath_ShouldProjectNestedRows()
    {
        using var connection = CreateOpenConnection(MariaDbDbVersions.Version10_6);
        using var command = new MySqlCommandMock(connection)
        {
            CommandText = """
                SELECT jt.Id, jt.TagOrd, jt.TagName
                FROM JSON_TABLE(
                    '[{"id":1,"tags":[{"name":"vip"},{"name":"new"}]},{"id":2,"tags":[{"name":"beta"}]}]',
                    '$[*]' COLUMNS(
                        Id INT PATH '$.id',
                        NESTED PATH '$.tags[*]' COLUMNS(
                            TagOrd FOR ORDINALITY,
                            TagName VARCHAR(30) PATH '$.name'
                        )
                    )
                ) jt
                ORDER BY jt.Id, jt.TagOrd
                """
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(1, reader.GetInt32(reader.GetOrdinal("Id")));
        Assert.Equal(1L, reader.GetInt64(reader.GetOrdinal("TagOrd")));
        Assert.Equal("vip", reader.GetString(reader.GetOrdinal("TagName")));

        Assert.True(reader.Read());
        Assert.Equal(1, reader.GetInt32(reader.GetOrdinal("Id")));
        Assert.Equal(2L, reader.GetInt64(reader.GetOrdinal("TagOrd")));
        Assert.Equal("new", reader.GetString(reader.GetOrdinal("TagName")));

        Assert.True(reader.Read());
        Assert.Equal(2, reader.GetInt32(reader.GetOrdinal("Id")));
        Assert.Equal(1L, reader.GetInt64(reader.GetOrdinal("TagOrd")));
        Assert.Equal("beta", reader.GetString(reader.GetOrdinal("TagName")));
        Assert.False(reader.Read());
    }

    /// <summary>
    /// EN: Ensures MariaDB JSON_TABLE returns null-complemented rows when a nested path has no matches.
    /// PT: Garante que JSON_TABLE do MariaDB retorne linhas com complemento nulo quando um nested path nao encontra correspondencias.
    /// </summary>
    [Fact]
    [Trait("Category", "MariaDbMock")]
    public void ExecuteReader_JsonTable_WithMissingNestedPath_ShouldReturnNullComplementedRows()
    {
        using var connection = CreateOpenConnection(MariaDbDbVersions.Version10_6);
        using var command = new MySqlCommandMock(connection)
        {
            CommandText = """
                SELECT jt.Id, jt.TagName
                FROM JSON_TABLE(
                    '[{"id":1,"tags":[{"name":"vip"}]},{"id":2}]',
                    '$[*]' COLUMNS(
                        Id INT PATH '$.id',
                        NESTED PATH '$.tags[*]' COLUMNS(
                            TagName VARCHAR(30) PATH '$.name'
                        )
                    )
                ) jt
                ORDER BY jt.Id
                """
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(1, reader.GetInt32(reader.GetOrdinal("Id")));
        Assert.Equal("vip", reader.GetString(reader.GetOrdinal("TagName")));

        Assert.True(reader.Read());
        Assert.Equal(2, reader.GetInt32(reader.GetOrdinal("Id")));
        Assert.True(reader.IsDBNull(reader.GetOrdinal("TagName")));
        Assert.False(reader.Read());
    }

    /// <summary>
    /// EN: Ensures strict nested JSON_TABLE paths fail when the nested array is missing in MariaDB.
    /// PT: Garante que caminhos nested strict em JSON_TABLE falhem quando o array aninhado estiver ausente no MariaDB.
    /// </summary>
    [Fact]
    [Trait("Category", "MariaDbMock")]
    public void ExecuteReader_JsonTable_WithStrictNestedPathMissing_ShouldThrow()
    {
        using var connection = CreateOpenConnection(MariaDbDbVersions.Version10_6);
        using var command = new MySqlCommandMock(connection)
        {
            CommandText = """
                SELECT jt.Id, jt.TagName
                FROM JSON_TABLE(
                    '[{"id":1}]',
                    '$[*]' COLUMNS(
                        Id INT PATH '$.id',
                        NESTED PATH 'strict $.tags[*]' COLUMNS(
                            TagName VARCHAR(30) PATH '$.name'
                        )
                    )
                ) jt
                """
        };

        var ex = Assert.Throws<InvalidOperationException>(() => command.ExecuteReader());

        Assert.Contains("strict nested path", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures nested JSON_TABLE columns can apply ON EMPTY fallbacks in MariaDB.
    /// PT: Garante que colunas nested de JSON_TABLE consigam aplicar fallbacks ON EMPTY no MariaDB.
    /// </summary>
    [Fact]
    [Trait("Category", "MariaDbMock")]
    public void ExecuteReader_JsonTable_WithNestedDefaultOnEmpty_ShouldUseFallback()
    {
        using var connection = CreateOpenConnection(MariaDbDbVersions.Version10_6);
        using var command = new MySqlCommandMock(connection)
        {
            CommandText = """
                SELECT jt.Id, jt.TagOrd, jt.TagName
                FROM JSON_TABLE(
                    '[{"id":1,"tags":[{"name":"vip"},{}]}]',
                    '$[*]' COLUMNS(
                        Id INT PATH '$.id',
                        NESTED PATH '$.tags[*]' COLUMNS(
                            TagOrd FOR ORDINALITY,
                            TagName VARCHAR(30) PATH '$.name' DEFAULT 'fallback' ON EMPTY
                        )
                    )
                ) jt
                ORDER BY jt.Id, jt.TagOrd
                """
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(1, reader.GetInt32(reader.GetOrdinal("Id")));
        Assert.Equal(1L, reader.GetInt64(reader.GetOrdinal("TagOrd")));
        Assert.Equal("vip", reader.GetString(reader.GetOrdinal("TagName")));

        Assert.True(reader.Read());
        Assert.Equal(1, reader.GetInt32(reader.GetOrdinal("Id")));
        Assert.Equal(2L, reader.GetInt64(reader.GetOrdinal("TagOrd")));
        Assert.Equal("fallback", reader.GetString(reader.GetOrdinal("TagName")));
        Assert.False(reader.Read());
    }

    /// <summary>
    /// EN: Ensures nested JSON_TABLE columns can apply ON ERROR fallbacks in MariaDB when a nested value is not scalar.
    /// PT: Garante que colunas nested de JSON_TABLE consigam aplicar fallbacks ON ERROR no MariaDB quando um valor aninhado nao for escalar.
    /// </summary>
    [Fact]
    [Trait("Category", "MariaDbMock")]
    public void ExecuteReader_JsonTable_WithNestedDefaultOnError_ShouldUseFallback()
    {
        using var connection = CreateOpenConnection(MariaDbDbVersions.Version10_6);
        using var command = new MySqlCommandMock(connection)
        {
            CommandText = """
                SELECT jt.Id, jt.TagOrd, jt.TagValue
                FROM JSON_TABLE(
                    '[{"id":1,"tags":[{"value":{"name":"vip"}},{"value":42}]}]',
                    '$[*]' COLUMNS(
                        Id INT PATH '$.id',
                        NESTED PATH '$.tags[*]' COLUMNS(
                            TagOrd FOR ORDINALITY,
                            TagValue INT PATH '$.value' DEFAULT '99' ON ERROR
                        )
                    )
                ) jt
                ORDER BY jt.Id, jt.TagOrd
                """
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(1, reader.GetInt32(reader.GetOrdinal("Id")));
        Assert.Equal(1L, reader.GetInt64(reader.GetOrdinal("TagOrd")));
        Assert.Equal(99, reader.GetInt32(reader.GetOrdinal("TagValue")));

        Assert.True(reader.Read());
        Assert.Equal(1, reader.GetInt32(reader.GetOrdinal("Id")));
        Assert.Equal(2L, reader.GetInt64(reader.GetOrdinal("TagOrd")));
        Assert.Equal(42, reader.GetInt32(reader.GetOrdinal("TagValue")));
        Assert.False(reader.Read());
    }

    /// <summary>
    /// EN: Ensures nested JSON_TABLE columns raise an error when ERROR ON ERROR is configured for a non-scalar value.
    /// PT: Garante que colunas nested de JSON_TABLE lancem erro quando ERROR ON ERROR estiver configurado para um valor nao escalar.
    /// </summary>
    [Fact]
    [Trait("Category", "MariaDbMock")]
    public void ExecuteReader_JsonTable_WithNestedErrorOnError_ShouldThrow()
    {
        using var connection = CreateOpenConnection(MariaDbDbVersions.Version10_6);
        using var command = new MySqlCommandMock(connection)
        {
            CommandText = """
                SELECT jt.Id, jt.TagValue
                FROM JSON_TABLE(
                    '[{"id":1,"tags":[{"value":{"name":"vip"}}]}]',
                    '$[*]' COLUMNS(
                        Id INT PATH '$.id',
                        NESTED PATH '$.tags[*]' COLUMNS(
                            TagValue INT PATH '$.value' ERROR ON ERROR
                        )
                    )
                ) jt
                """
        };

        var ex = Assert.Throws<InvalidOperationException>(() => command.ExecuteReader());

        Assert.Contains("TagValue", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures nested JSON_TABLE columns raise an error when ERROR ON EMPTY is configured for a missing child value.
    /// PT: Garante que colunas nested de JSON_TABLE lancem erro quando ERROR ON EMPTY estiver configurado para valor filho ausente.
    /// </summary>
    [Fact]
    [Trait("Category", "MariaDbMock")]
    public void ExecuteReader_JsonTable_WithNestedErrorOnEmpty_ShouldThrow()
    {
        using var connection = CreateOpenConnection(MariaDbDbVersions.Version10_6);
        using var command = new MySqlCommandMock(connection)
        {
            CommandText = """
                SELECT jt.Id, jt.TagName
                FROM JSON_TABLE(
                    '[{"id":1,"tags":[{}]}]',
                    '$[*]' COLUMNS(
                        Id INT PATH '$.id',
                        NESTED PATH '$.tags[*]' COLUMNS(
                            TagName VARCHAR(30) PATH '$.name' ERROR ON EMPTY
                        )
                    )
                ) jt
                """
        };

        var ex = Assert.Throws<InvalidOperationException>(() => command.ExecuteReader());

        Assert.Contains("TagName", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures MariaDB JSON_TABLE keeps sibling nested paths independent and emits nulls for the missing sibling side.
    /// PT: Garante que JSON_TABLE do MariaDB mantenha nested paths irmaos independentes e emita nulos no lado irmao ausente.
    /// </summary>
    [Fact]
    [Trait("Category", "MariaDbMock")]
    public void ExecuteReader_JsonTable_WithSiblingNestedPaths_ShouldProjectIndependentRows()
    {
        using var connection = CreateOpenConnection(MariaDbDbVersions.Version10_6);
        using var command = new MySqlCommandMock(connection)
        {
            CommandText = """
                SELECT jt.Size, jt.Color
                FROM JSON_TABLE(
                    '{
                        "sizes":[{"size":"small"},{"size":"medium"}],
                        "colors":[{"color":"red"},{"color":"blue"}]
                    }',
                    '$' COLUMNS(
                        NESTED PATH '$.sizes[*]' COLUMNS(
                            Size VARCHAR(20) PATH '$.size'
                        ),
                        NESTED PATH '$.colors[*]' COLUMNS(
                            Color VARCHAR(20) PATH '$.color'
                        )
                    )
                ) jt
                """
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal("small", reader.GetString(reader.GetOrdinal("Size")));
        Assert.True(reader.IsDBNull(reader.GetOrdinal("Color")));

        Assert.True(reader.Read());
        Assert.Equal("medium", reader.GetString(reader.GetOrdinal("Size")));
        Assert.True(reader.IsDBNull(reader.GetOrdinal("Color")));

        Assert.True(reader.Read());
        Assert.True(reader.IsDBNull(reader.GetOrdinal("Size")));
        Assert.Equal("red", reader.GetString(reader.GetOrdinal("Color")));

        Assert.True(reader.Read());
        Assert.True(reader.IsDBNull(reader.GetOrdinal("Size")));
        Assert.Equal("blue", reader.GetString(reader.GetOrdinal("Color")));
        Assert.False(reader.Read());
    }

    /// <summary>
    /// EN: Ensures MariaDB JSON_TABLE evaluates EXISTS PATH columns inside nested branches with 1/0 semantics.
    /// PT: Garante que JSON_TABLE do MariaDB avalie colunas EXISTS PATH dentro de ramos nested com semantica 1/0.
    /// </summary>
    [Fact]
    [Trait("Category", "MariaDbMock")]
    public void ExecuteReader_JsonTable_WithNestedExistsPath_ShouldReturnFlags()
    {
        using var connection = CreateOpenConnection(MariaDbDbVersions.Version10_6);
        using var command = new MySqlCommandMock(connection)
        {
            CommandText = """
                SELECT jt.Id, jt.HasTag
                FROM JSON_TABLE(
                    '[{"id":1,"tags":[{"name":"vip"}]},{"id":2,"tags":[{}]}]',
                    '$[*]' COLUMNS(
                        Id INT PATH '$.id',
                        NESTED PATH '$.tags[*]' COLUMNS(
                            HasTag INT EXISTS PATH '$.name'
                        )
                    )
                ) jt
                ORDER BY jt.Id
                """
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(1, reader.GetInt32(reader.GetOrdinal("Id")));
        Assert.Equal(1, reader.GetInt32(reader.GetOrdinal("HasTag")));

        Assert.True(reader.Read());
        Assert.Equal(2, reader.GetInt32(reader.GetOrdinal("Id")));
        Assert.Equal(0, reader.GetInt32(reader.GetOrdinal("HasTag")));
        Assert.False(reader.Read());
    }

    private static MariaDbConnectionMock CreateOpenConnection(int version)
    {
        var db = new MariaDbDbMock(version);
        var users = db.AddTable("Users", [
            new("Id", DbType.Int32, false),
            new("Name", DbType.String, false),
            new("Email", DbType.String, true)
        ]);
        users.AddPrimaryKeyIndexes("Id");

        var connection = new MariaDbConnectionMock(db);
        connection.Open();
        return connection;
    }
}
