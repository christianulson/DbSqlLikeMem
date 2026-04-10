namespace DbSqlLikeMem.VisualStudioExtension.Core.Test;

/// <summary>
/// EN: Verifies metadata extraction and query generation for supported database providers.
/// PT: Verifica a extracao de metadados e a geracao de consultas para provedores de banco suportados.
/// </summary>
public sealed class SqlDatabaseMetadataProviderTests
{
    /// <summary>
    /// EN: Verifies table metadata is serialized with columns, keys, indexes, foreign keys, and triggers.
    /// PT: Verifica se os metadados de tabela sao serializados com colunas, chaves, indices, chaves estrangeiras e triggers.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlDatabaseMetadataProvider")]
    public async Task GetObjectAsync_ReturnsCompleteStructureMetadata()
    {
        var executor = new FakeSqlQueryExecutor();
        executor.WhenContains("FROM INFORMATION_SCHEMA.TABLES", [
            Row(("SchemaName", "dbo"), ("ObjectName", "Orders"), ("ObjectType", "Table"))
        ]);
        executor.WhenContains("FROM INFORMATION_SCHEMA.COLUMNS", [
            Row(("ColumnName", "Id"), ("DataType", "int"), ("Ordinal", 1), ("IsNullable", "NO"), ("Extra", "auto_increment"), ("DefaultValue", null), ("CharMaxLen", null), ("NumScale", null), ("ColumnType", "int"), ("Generated", "")),
            Row(("ColumnName", "CustomerId"), ("DataType", "int"), ("Ordinal", 2), ("IsNullable", "NO"), ("Extra", ""), ("DefaultValue", null), ("CharMaxLen", null), ("NumScale", null), ("ColumnType", "int"), ("Generated", ""))
        ]);
        executor.WhenContains("INDEX_NAME='PRIMARY'", [
            Row(("ColumnName", "Id"))
        ]);
        executor.WhenContains("FROM INFORMATION_SCHEMA.STATISTICS", [
            Row(("IndexName", Const.PRIMARY), ("NonUnique", 0), ("ColumnName", "Id"), ("Seq", 1)),
            Row(("IndexName", "IX_Orders_CustomerId"), ("NonUnique", 1), ("ColumnName", "CustomerId"), ("Seq", 1))
        ]);
        executor.WhenContains("FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE", [
            Row(("ColumnName", "CustomerId"), ("RefTable", "Customers"), ("RefColumn", "Id"))
        ]);
        executor.WhenContains("FROM INFORMATION_SCHEMA.TRIGGERS", [
            Row(("TriggerName", "trg_orders_audit"))
        ]);

        var provider = new SqlDatabaseMetadataProvider(executor);
        var conn = new ConnectionDefinition("1", "MySql", "ERP", "conn");
        var reference = new DatabaseObjectReference("dbo", "Orders", DatabaseObjectType.Table);

        var result = await provider.GetObjectAsync(conn, reference, TestContext.Current.CancellationToken);

        Assert.NotNull(result);
        Assert.Contains("Id|int|1|0|1", result!.Properties!["Columns"]);
        Assert.Equal("Id", result.Properties["PrimaryKey"]);
        Assert.DoesNotContain(Const.PRIMARY, result.Properties["Indexes"], StringComparison.OrdinalIgnoreCase);
        Assert.Contains("IX_Orders_CustomerId|0|CustomerId", result.Properties["Indexes"]);
        Assert.Equal("CustomerId|Customers|Id", result.Properties["ForeignKeys"]);
        Assert.Equal("trg_orders_audit", result.Properties["Triggers"]);
    }

    /// <summary>
    /// EN: Ensures routine metadata and parameter metadata are serialized for function objects.
    /// PT: Garante que os metadados da rotina e dos parametros sejam serializados para objetos do tipo function.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlDatabaseMetadataProvider")]
    public async Task GetObjectAsync_ReturnsRoutineMetadataForFunction()
    {
        var executor = new FakeSqlQueryExecutor();
        executor.WhenContains("FROM INFORMATION_SCHEMA.TABLES", [
            Row(("SchemaName", "dbo"), ("ObjectName", "fn_Total"), ("ObjectType", "Function"))
        ]);
        executor.WhenContains("ROUTINE_DEFINITION", [
            Row(("SchemaName", "dbo"), ("ObjectName", "fn_Total"), ("RoutineType", "Function"), ("ReturnTypeSql", "int"), ("BodySql", "CustomerId"))
        ]);
        executor.WhenContains("INFORMATION_SCHEMA.PARAMETERS", [
            Row(("SchemaName", "dbo"), ("ObjectName", "fn_Total"), ("ParameterName", "CustomerId"), ("ParameterMode", "IN"), ("DataType", "int"), ("Ordinal", 1), ("DefaultValue", ""), ("IsNullable", "NO"), ("CharMaxLen", null), ("NumPrecision", 10), ("NumScale", 0), ("IsVariadic", "0"), ("IsOrderByClause", "0"), ("IsFrameClause", "0"))
        ]);

        var provider = new SqlDatabaseMetadataProvider(executor);
        var conn = new ConnectionDefinition("1", "MySql", "ERP", "conn");
        var reference = new DatabaseObjectReference("dbo", "fn_Total", DatabaseObjectType.Function);

        var result = await provider.GetObjectAsync(conn, reference, TestContext.Current.CancellationToken);

        Assert.NotNull(result);
        Assert.Equal("int", result!.Properties!["ReturnTypeSql"]);
        Assert.Equal("CustomerId", result.Properties["BodySql"]);
        Assert.Contains("CustomerId|int|1|0|0|0|", result.Properties["Parameters"]);
    }

    /// <summary>
    /// EN: Ensures function body text is reduced to the executable scalar expression before serialization.
    /// PT: Garante que o texto do corpo da function seja reduzido a expressao escalar executavel antes da serializacao.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlDatabaseMetadataProvider")]
    public async Task GetObjectAsync_NormalizesFunctionBodyFromCreateFunctionSource()
    {
        var executor = new FakeSqlQueryExecutor();
        executor.WhenContains("FROM INFORMATION_SCHEMA.TABLES", [
            Row(("SchemaName", "dbo"), ("ObjectName", "fn_Total"), ("ObjectType", "Function"))
        ]);
        executor.WhenContains("ROUTINE_DEFINITION", [
            Row(("SchemaName", "dbo"), ("ObjectName", "fn_Total"), ("RoutineType", "Function"), ("ReturnTypeSql", "int"), ("BodySql", "CREATE FUNCTION fn_Total(CustomerId int) RETURNS int AS BEGIN RETURN CustomerId + 1; END"))
        ]);
        executor.WhenContains("INFORMATION_SCHEMA.PARAMETERS", [
            Row(("SchemaName", "dbo"), ("ObjectName", "fn_Total"), ("ParameterName", "CustomerId"), ("ParameterMode", "IN"), ("DataType", "int"), ("Ordinal", 1), ("DefaultValue", ""), ("IsNullable", "NO"), ("CharMaxLen", null), ("NumPrecision", 10), ("NumScale", 0), ("IsVariadic", "0"), ("IsOrderByClause", "0"), ("IsFrameClause", "0"))
        ]);

        var provider = new SqlDatabaseMetadataProvider(executor);
        var conn = new ConnectionDefinition("1", "MySql", "ERP", "conn");
        var reference = new DatabaseObjectReference("dbo", "fn_Total", DatabaseObjectType.Function);

        var result = await provider.GetObjectAsync(conn, reference, TestContext.Current.CancellationToken);

        Assert.NotNull(result);
        Assert.Equal("CustomerId + 1", result!.Properties!["BodySql"]);
    }


    /// <summary>
    /// Verifies MySQL list query uses database name parsed from connection string.
    /// Verifica se a consulta de listagem MySQL usa o nome do banco extraído da connection string.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlDatabaseMetadataProvider")]
    public async Task ListObjectsAsync_ForMySql_UsesDatabaseNameFromConnectionString()
    {
        var executor = new FakeSqlQueryExecutor();
        executor.WhenContains("FROM INFORMATION_SCHEMA.TABLES", []);

        var provider = new SqlDatabaseMetadataProvider(executor);
        var conn = new ConnectionDefinition(
            "1",
            "MySql",
            "ApelidoDaConexao",
            "Server=localhost;Port=3306;Database=addresses;Uid=root;Pwd=secret;");

        _ = await provider.ListObjectsAsync(conn, TestContext.Current.CancellationToken);

        Assert.True(executor.TryGetLastParametersFor("FROM INFORMATION_SCHEMA.TABLES", out var parameters));
        Assert.Equal("addresses", parameters!["databaseName"]?.ToString());
    }

    /// <summary>
    /// EN: Verifies MariaDB list query uses the database name parsed from the connection string.
    /// PT: Verifica se a consulta de listagem do MariaDB usa o nome do banco extraído da connection string.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlDatabaseMetadataProvider")]
    public async Task ListObjectsAsync_ForMariaDb_UsesDatabaseNameFromConnectionString()
    {
        var executor = new FakeSqlQueryExecutor();
        executor.WhenContains("FROM INFORMATION_SCHEMA.TABLES", []);

        var provider = new SqlDatabaseMetadataProvider(executor);
        var conn = new ConnectionDefinition(
            "1",
            "MariaDb",
            "AliasMaria",
            "Server=localhost;Port=3306;Database=addresses_mariadb;Uid=root;Pwd=secret;");

        _ = await provider.ListObjectsAsync(conn, TestContext.Current.CancellationToken);

        Assert.True(executor.TryGetLastParametersFor("FROM INFORMATION_SCHEMA.TABLES", out var parameters));
        Assert.Equal("addresses_mariadb", parameters!["databaseName"]?.ToString());
    }

    /// <summary>
    /// EN: Verifies provider support stays available for the configured database types.
    /// PT: Verifica se o suporte do provedor permanece disponivel para os tipos de banco configurados.
    /// </summary>
    [Theory]
    [Trait("Category", "SqlDatabaseMetadataProvider")]
    [InlineData("MySql")]
    [InlineData("SqlServer")]
    [InlineData("SqlAzure")]
    [InlineData("AzureSql")]
    [InlineData("azure-sql")]
    [InlineData("sql_azure")]
    [InlineData("PostgreSql")]
    [InlineData("Oracle")]
    [InlineData("Sqlite")]
    [InlineData("Db2")]
    [InlineData("MariaDb")]
    [InlineData("Firebird")]
    public void QueryFactory_SupportsConfiguredDatabases(string databaseType)
    {
        Assert.False(string.IsNullOrWhiteSpace(SqlMetadataQueryFactory.BuildListObjectsQuery(databaseType)));
        Assert.False(string.IsNullOrWhiteSpace(SqlMetadataQueryFactory.BuildObjectColumnsQuery(databaseType)));
        Assert.False(string.IsNullOrWhiteSpace(SqlMetadataQueryFactory.BuildPrimaryKeyQuery(databaseType)));
        Assert.False(string.IsNullOrWhiteSpace(SqlMetadataQueryFactory.BuildIndexesQuery(databaseType)));
        Assert.False(string.IsNullOrWhiteSpace(SqlMetadataQueryFactory.BuildForeignKeysQuery(databaseType)));
        Assert.False(string.IsNullOrWhiteSpace(SqlMetadataQueryFactory.BuildTriggersQuery(databaseType)));
        Assert.False(string.IsNullOrWhiteSpace(SqlMetadataQueryFactory.BuildSequenceMetadataQuery(databaseType)));
        Assert.False(string.IsNullOrWhiteSpace(SqlMetadataQueryFactory.BuildRoutineMetadataQuery(databaseType)));
        Assert.False(string.IsNullOrWhiteSpace(SqlMetadataQueryFactory.BuildRoutineParametersQuery(databaseType)));
    }

    /// <summary>
    /// EN: Verifies MariaDB exposes a sequence metadata query with sequence columns.
    /// PT: Verifica se o MariaDB expõe uma consulta de metadata de sequence com as colunas de sequence.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlDatabaseMetadataProvider")]
    public void QueryFactory_BuildsMariaDbSequenceMetadataQuery()
    {
        var query = SqlMetadataQueryFactory.BuildSequenceMetadataQuery("MariaDb");

        Assert.Contains("INFORMATION_SCHEMA.SEQUENCES", query, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("START_VALUE", query, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("INCREMENT", query, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Verifies Firebird list queries include tables, views, procedures and functions.
    /// PT: Verifica se as consultas de listagem do Firebird incluem tabelas, views, procedimentos e funcoes.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlDatabaseMetadataProvider")]
    public void QueryFactory_BuildsFirebirdListObjectsQuery()
    {
        var query = SqlMetadataQueryFactory.BuildListObjectsQuery("Firebird");

        Assert.Contains("RDB$RELATIONS", query, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("RDB$PROCEDURES", query, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("RDB$FUNCTIONS", query, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Verifies MariaDB list queries include sequences while MySQL keeps the shared subset.
    /// PT: Verifica se as consultas de listagem do MariaDB incluem sequences enquanto o MySQL mantem o subconjunto compartilhado.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlDatabaseMetadataProvider")]
    public void QueryFactory_BuildsMariaDbListObjectsQueryWithSequences()
    {
        var mariaDbQuery = SqlMetadataQueryFactory.BuildListObjectsQuery("MariaDb");
        var mySqlQuery = SqlMetadataQueryFactory.BuildListObjectsQuery("MySql");

        Assert.Contains("INFORMATION_SCHEMA.SEQUENCES", mariaDbQuery, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("INFORMATION_SCHEMA.SEQUENCES", mySqlQuery, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Verifies SQL Azure list query uses database name parsed from connection string.
    /// Verifica se a consulta de listagem SQL Azure usa o nome do banco extraído da connection string.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlDatabaseMetadataProvider")]
    public async Task ListObjectsAsync_ForSqlAzure_UsesDatabaseNameFromConnectionString()
    {
        var executor = new FakeSqlQueryExecutor();
        executor.WhenContains("FROM sys.objects", []);

        var provider = new SqlDatabaseMetadataProvider(executor);
        var conn = new ConnectionDefinition(
            "1",
            "SqlAzure",
            "AliasAzure",
            "Server=tcp:localhost,1433;Initial Catalog=erp_azure;User ID=sa;Password=secret;");

        _ = await provider.ListObjectsAsync(conn, TestContext.Current.CancellationToken);

        Assert.True(executor.TryGetLastParametersFor("FROM sys.objects", out var parameters));
        Assert.Equal("erp_azure", parameters!["databaseName"]?.ToString());
    }

    /// <summary>
    /// EN: Verifies MariaDB sequence metadata queries expose the expected columns.
    /// PT: Verifica se as consultas de metadata de sequence do MariaDB expõem as colunas esperadas.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlDatabaseMetadataProvider")]
    public async Task GetObjectAsync_ForSequence_ReturnsSequenceMetadataOnly()
    {
        var executor = new FakeSqlQueryExecutor();
        executor.WhenContains("FROM sys.objects", [
            Row(("SchemaName", "dbo"), ("ObjectName", "seq_orders"), ("ObjectType", "Sequence"))
        ]);
        executor.WhenContains("FROM sys.sequences", [
            Row(("StartValue", 100L), ("IncrementBy", 5L), ("CurrentValue", 115L))
        ]);

        var provider = new SqlDatabaseMetadataProvider(executor);
        var conn = new ConnectionDefinition("1", "SqlServer", "ERP", "Server=.;Initial Catalog=erp;");
        var reference = new DatabaseObjectReference("dbo", "seq_orders", DatabaseObjectType.Sequence);

        var result = await provider.GetObjectAsync(conn, reference, TestContext.Current.CancellationToken);

        Assert.NotNull(result);
        Assert.Equal("100", result!.Properties!["StartValue"]);
        Assert.Equal("5", result.Properties["IncrementBy"]);
        Assert.Equal("115", result.Properties["CurrentValue"]);
        Assert.False(result.Properties.ContainsKey("Columns"));
    }

    /// <summary>
    /// EN: Verifies MariaDB sequence metadata is read and serialized with the sequence detail query.
    /// PT: Verifica se a metadata de sequence do MariaDB e lida e serializada pela query de detalhe da sequence.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlDatabaseMetadataProvider")]
    public async Task GetObjectAsync_ForMariaDbSequence_ReturnsSequenceMetadataOnly()
    {
        var executor = new FakeSqlQueryExecutor();
        executor.WhenContains("FROM INFORMATION_SCHEMA.TABLES", [
            Row(("SchemaName", "dbo"), ("ObjectName", "seq_orders"), ("ObjectType", "Sequence"))
        ]);
        executor.WhenContains("CAST(START_VALUE AS CHAR)", [
            Row(("SchemaName", "dbo"), ("ObjectName", "seq_orders"), ("StartValue", 100L), ("IncrementBy", 5L), ("CurrentValue", 115L))
        ]);

        var provider = new SqlDatabaseMetadataProvider(executor);
        var conn = new ConnectionDefinition("1", "MariaDb", "ERP", "Server=.;Database=erp;");
        var reference = new DatabaseObjectReference("dbo", "seq_orders", DatabaseObjectType.Sequence);

        var result = await provider.GetObjectAsync(conn, reference, TestContext.Current.CancellationToken);

        Assert.NotNull(result);
        Assert.Equal("100", result!.Properties!["StartValue"]);
        Assert.Equal("5", result.Properties["IncrementBy"]);
        Assert.Equal("115", result.Properties["CurrentValue"]);
        Assert.False(result.Properties.ContainsKey("Columns"));
    }

    private static IReadOnlyDictionary<string, object?> Row(params (string Key, object? Value)[] items)
        => items.ToDictionary(x => x.Key, x => x.Value);

    private sealed class FakeSqlQueryExecutor : ISqlQueryExecutor
    {
        private readonly List<(string Contains, IReadOnlyCollection<IReadOnlyDictionary<string, object?>> Rows)> _responses = [];
        private readonly List<(string Sql, IReadOnlyDictionary<string, object?> Parameters)> _calls = [];

    /// <summary>
    /// EN: Verifies Firebird list queries include the object families used by the generator.
    /// PT: Verifica se as consultas de listagem do Firebird incluem as familias de objetos usadas pela geracao.
    /// </summary>
        public void WhenContains(string containsSql, IReadOnlyCollection<IReadOnlyDictionary<string, object?>> rows)
            => _responses.Add((containsSql, rows));

        public bool TryGetLastParametersFor(string containsSql, out IReadOnlyDictionary<string, object?>? parameters)
        {
            for (var i = _calls.Count - 1; i >= 0; i--)
            {
                if (_calls[i].Sql.Contains(containsSql, StringComparison.OrdinalIgnoreCase))
                {
                    parameters = _calls[i].Parameters;
                    return true;
                }
            }

            parameters = null;
            return false;
        }

    /// <summary>
    /// EN: Verifies MariaDB list queries include sequence objects.
    /// PT: Verifica se as consultas de listagem do MariaDB incluem objetos do tipo sequence.
    /// </summary>
        public Task<IReadOnlyCollection<IReadOnlyDictionary<string, object?>>> QueryAsync(
            ConnectionDefinition connection,
            string sql,
            IReadOnlyDictionary<string, object?> parameters,
            CancellationToken cancellationToken = default)
        {
            _calls.Add((sql, parameters));
            var hit = _responses.FirstOrDefault(x => sql.Contains(x.Contains, StringComparison.OrdinalIgnoreCase));
            return Task.FromResult(hit.Rows ?? []);
        }
    }
}
