namespace DbSqlLikeMem.VisualStudioExtension.Core.Test;

/// <summary>
/// EN: Verifies object-type mapping updates for the Visual Studio extension generation flow.
/// PT: Verifica atualizacoes de mapeamento por tipo de objeto para o fluxo de geracao da extensao do Visual Studio.
/// </summary>
public sealed class ConnectionMappingServiceTests
{
    /// <summary>
    /// EN: Ensures missing mappings fall back to the shared default output and file pattern.
    /// PT: Garante que mapeamentos ausentes voltem ao diretorio e padrao de arquivo compartilhados por padrao.
    /// </summary>
    [Fact]
    [Trait("Category", "ConnectionMappingService")]
    public void GetMappingOrDefault_WhenMappingIsMissing_ReturnsSharedDefaults()
    {
        var service = new ConnectionMappingService();

        var mapping = service.GetMappingOrDefault(configuration: null, DatabaseObjectType.View);

        Assert.Equal(DatabaseObjectType.View, mapping.ObjectType);
        Assert.Equal("Generated", mapping.OutputDirectory);
        Assert.Equal("{NamePascal}{Type}Factory.cs", mapping.FileNamePattern);
        Assert.Null(mapping.Namespace);
    }

    /// <summary>
    /// EN: Ensures updates affect only the selected object type and preserve remaining mappings for the same connection.
    /// PT: Garante que atualizacoes afetem apenas o tipo de objeto selecionado e preservem os mapeamentos restantes da mesma conexao.
    /// </summary>
    [Fact]
    [Trait("Category", "ConnectionMappingService")]
    public void UpsertMapping_WhenConfigurationExists_UpdatesOnlySelectedObjectType()
    {
        var service = new ConnectionMappingService();
        var configuration = new ConnectionMappingConfiguration(
            "conn-1",
            new Dictionary<DatabaseObjectType, ObjectTypeMapping>
            {
                [DatabaseObjectType.Table] = new(DatabaseObjectType.Table, "Generated/Tables", "{NamePascal}TableTests.cs", "Company.Project.Tables"),
                [DatabaseObjectType.View] = new(DatabaseObjectType.View, "Generated/Views", "{NamePascal}ViewTests.cs", "Company.Project.Views"),
                [DatabaseObjectType.Procedure] = new(DatabaseObjectType.Procedure, "Generated/Procedures", "{NamePascal}ProcedureTests.cs", "Company.Project.Procedures"),
                [DatabaseObjectType.Sequence] = new(DatabaseObjectType.Sequence, "Generated/Sequences", "{NamePascal}SequenceTests.cs", "Company.Project.Sequences"),
            });

        var updated = service.UpsertMapping(
            "conn-1",
            configuration,
            DatabaseObjectType.View,
            "{Schema}_{NamePascal}ProjectionTests.cs",
            "tests/Integration/Views",
            "Company.Project.Integration.Views");

        Assert.Equal("conn-1", updated.ConnectionId);
        Assert.Equal("{Schema}_{NamePascal}ProjectionTests.cs", updated.Mappings[DatabaseObjectType.View].FileNamePattern);
        Assert.Equal("tests/Integration/Views", updated.Mappings[DatabaseObjectType.View].OutputDirectory);
        Assert.Equal("Company.Project.Integration.Views", updated.Mappings[DatabaseObjectType.View].Namespace);

        Assert.Equal("{NamePascal}TableTests.cs", updated.Mappings[DatabaseObjectType.Table].FileNamePattern);
        Assert.Equal("Generated/Tables", updated.Mappings[DatabaseObjectType.Table].OutputDirectory);
        Assert.Equal("Company.Project.Tables", updated.Mappings[DatabaseObjectType.Table].Namespace);
    }

    /// <summary>
    /// EN: Ensures creating a mapping for a new connection still keeps defaults for the object types not explicitly configured.
    /// PT: Garante que criar um mapeamento para uma nova conexao ainda mantenha valores padrao para os tipos nao configurados explicitamente.
    /// </summary>
    [Fact]
    [Trait("Category", "ConnectionMappingService")]
    public void UpsertMapping_WhenConfigurationIsMissing_CreatesDefaultsAndOverridesSelectedObjectType()
    {
        var service = new ConnectionMappingService();

        var updated = service.UpsertMapping(
            "conn-2",
            configuration: null,
            DatabaseObjectType.Table,
            "{NamePascal}IntegrationTests.cs",
            "tests/Integration/Tables",
            "Company.Project.Integration");

        Assert.Equal("conn-2", updated.ConnectionId);
        Assert.Equal("{NamePascal}IntegrationTests.cs", updated.Mappings[DatabaseObjectType.Table].FileNamePattern);
        Assert.Equal("tests/Integration/Tables", updated.Mappings[DatabaseObjectType.Table].OutputDirectory);
        Assert.Equal("Company.Project.Integration", updated.Mappings[DatabaseObjectType.Table].Namespace);

        Assert.Equal("{NamePascal}{Type}Factory.cs", updated.Mappings[DatabaseObjectType.View].FileNamePattern);
        Assert.Equal("Generated", updated.Mappings[DatabaseObjectType.View].OutputDirectory);
    }

    /// <summary>
    /// EN: Ensures baseline-driven mapping defaults reuse the versioned profile catalog while preserving the informed namespace.
    /// PT: Garante que defaults de mapeamento guiados por baseline reutilizem o catalogo versionado de perfis preservando o namespace informado.
    /// </summary>
    [Fact]
    [Trait("Category", "ConnectionMappingService")]
    public void CreateRecommendedMapping_ShouldReuseProfileDefaultsAndPreserveNamespace()
    {
        var service = new ConnectionMappingService();

        var mapping = service.CreateRecommendedMapping("worker", DatabaseObjectType.Sequence, "Company.Project.Batch.Sequences");

        Assert.Equal(DatabaseObjectType.Sequence, mapping.ObjectType);
        Assert.Equal("tests/Consistency/Sequences", mapping.OutputDirectory);
        Assert.Equal("{NamePascal}SequenceConsistencyTests.cs", mapping.FileNamePattern);
        Assert.Equal("Company.Project.Batch.Sequences", mapping.Namespace);
    }
}
