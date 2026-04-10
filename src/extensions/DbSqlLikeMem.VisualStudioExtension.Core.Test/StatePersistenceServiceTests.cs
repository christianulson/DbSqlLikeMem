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

            var mapping = Assert.Single(Assert.Single(loaded!.Mappings).Mappings);
            Assert.Equal("Company.Project.Generated", mapping.Value.Namespace);
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
    [Fact]
    [Trait("Category", "StatePersistenceService")]
    public void Load_ShouldNormalizeLegacyDatabaseTypeAliases()
    {
        var tempPath = Path.Combine(Path.GetTempPath(), $"dbsql-state-alias-{Guid.NewGuid():N}.json");

        try
        {
            File.WriteAllText(tempPath, """
            {
              "connections": [
                {
                  "id": "1",
                  "databaseType": "azure-sql",
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
            Assert.Equal("AzureSql", connection.DatabaseType);
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
