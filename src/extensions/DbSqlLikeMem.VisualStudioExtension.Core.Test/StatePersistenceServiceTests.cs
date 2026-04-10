using DbSqlLikeMem.VisualStudioExtension.Core.Persistence;

namespace DbSqlLikeMem.VisualStudioExtension.Core.Test;

/// <summary>
/// EN: Verifies persistence behavior for the Visual Studio extension state model.
/// PT: Verifica o comportamento de persistência do modelo de estado da extensão do Visual Studio.
/// </summary>
public sealed class StatePersistenceServiceTests
{
    /// <summary>
    /// EN: Ensures mapping namespaces survive save/load roundtrips in the persisted extension state.
    /// PT: Garante que namespaces de mapeamento sobrevivam ao ciclo de salvar/carregar no estado persistido da extensão.
    /// </summary>
    [Fact]
    [Trait("Category", "StatePersistenceService")]
    public void SaveAndLoad_ShouldRoundTripMappingNamespaceAndTemplatePatterns()
    {
        var tempPath = Path.Combine(Path.GetTempPath(), $"dbsql-state-{Guid.NewGuid():N}.json");

        try
        {
            var service = new StatePersistenceService();
            var state = new ExtensionState(
                [new ConnectionDefinition("1", "SqlServer", "ERP", "Server=.;Database=ERP;")],
                [
                    new ConnectionMappingConfiguration(
                        "1",
                        new Dictionary<DatabaseObjectType, ObjectTypeMapping>
                        {
                            [DatabaseObjectType.Table] = new(DatabaseObjectType.Table, "Generated", "{NamePascal}Model.cs", "Company.Project.Generated"),
                        })
                ],
                new TemplateConfiguration(
                    "templates/model.template.txt",
                    "templates/repository.template.txt",
                    "src/Models",
                    "src/Repositories",
                    "{Schema}_{NamePascal}Model.cs",
                    "{Schema}_{NamePascal}Repository.cs"));

            service.Save(state, tempPath);
            var loaded = service.Load(tempPath);

            var configuration = Assert.Single(loaded!.Mappings);
            Assert.Equal("Company.Project.Generated", configuration.Mappings[DatabaseObjectType.Table].Namespace);
            Assert.Equal("Company.Project.Generated", configuration.Mappings[DatabaseObjectType.Function].Namespace);
            Assert.Equal("{Schema}_{NamePascal}Model.cs", loaded.TemplateConfiguration.ModelFileNamePattern);
            Assert.Equal("{Schema}_{NamePascal}Repository.cs", loaded.TemplateConfiguration.RepositoryFileNamePattern);
        }
        finally
        {
            if (File.Exists(tempPath))
            {
                File.Delete(tempPath);
            }
        }
    }

    /// <summary>
    /// EN: Ensures legacy database type aliases are normalized when persisted state is loaded.
    /// PT: Garante que aliases legados de tipo de banco sejam normalizados quando o estado persistido e carregado.
    /// </summary>
    [Theory]
    [Trait("Category", "StatePersistenceService")]
    [InlineData("azure-sql", "AzureSql")]
    [InlineData("mssql", "SqlServer")]
    [InlineData("postgres", "PostgreSql")]
    [InlineData("pgsql", "PostgreSql")]
    [InlineData("sqlite3", "Sqlite")]
    [InlineData("db2/luw", "Db2")]
    [InlineData("db2luw", "Db2")]
    [InlineData("firebirdsql", "Firebird")]
    public void Load_ShouldNormalizeLegacyDatabaseTypeAliases(string persistedDatabaseType, string expectedDatabaseType)
    {
        var tempPath = Path.Combine(Path.GetTempPath(), $"dbsql-state-alias-{Guid.NewGuid():N}.json");

        try
        {
            File.WriteAllText(tempPath, $$"""
            {
              "connections": [
                {
                  "id": "1",
                  "databaseType": "{{persistedDatabaseType}}",
                  "databaseName": "ERP",
                  "connectionString": "Server=.;Database=ERP;"
                }
              ],
              "mappings": []
            }
            """);

            var service = new StatePersistenceService();
            var loaded = service.Load(tempPath);

            var connection = Assert.Single(loaded!.Connections);
            Assert.Equal(expectedDatabaseType, connection.DatabaseType);
        }
        finally
        {
            if (File.Exists(tempPath))
            {
                File.Delete(tempPath);
            }
        }
    }

    /// <summary>
    /// EN: Ensures legacy mapping payloads get the missing Function entry backfilled when state is loaded.
    /// PT: Garante que payloads legados de mapeamento recebam novamente a entrada Function ausente quando o estado e carregado.
    /// </summary>
    [Fact]
    [Trait("Category", "StatePersistenceService")]
    public void Load_ShouldBackfillMissingFunctionMappingFromLegacyState()
    {
        var tempPath = Path.Combine(Path.GetTempPath(), $"dbsql-state-legacy-mapping-{Guid.NewGuid():N}.json");

        try
        {
            File.WriteAllText(tempPath, """
            {
              "connections": [
                {
                  "id": "1",
                  "databaseType": "SqlServer",
                  "databaseName": "ERP",
                  "connectionString": "Server=.;Database=ERP;"
                }
              ],
              "mappings": [
                {
                  "connectionId": "1",
                  "mappings": {
                    "Table": {
                      "objectType": "Table",
                      "outputDirectory": "Generated/Tables",
                      "fileNamePattern": "{NamePascal}TableTests.cs",
                      "namespace": "Company.Project.Generated"
                    }
                  }
                }
              ]
            }
            """);

            var service = new StatePersistenceService();
            var loaded = service.Load(tempPath);

            var mapping = Assert.Single(loaded!.Mappings);
            Assert.True(mapping.Mappings.ContainsKey(DatabaseObjectType.Function));

            var functionMapping = mapping.Mappings[DatabaseObjectType.Function];
            Assert.Equal(DatabaseObjectType.Function, functionMapping.ObjectType);
            Assert.Equal("Generated/Tables", functionMapping.OutputDirectory);
            Assert.Equal("{NamePascal}TableTests.cs", functionMapping.FileNamePattern);
            Assert.Equal("Company.Project.Generated", functionMapping.Namespace);
        }
        finally
        {
            if (File.Exists(tempPath))
            {
                File.Delete(tempPath);
            }
        }
    }
}
