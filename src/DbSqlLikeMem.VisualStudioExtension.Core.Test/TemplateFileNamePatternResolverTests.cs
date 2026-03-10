namespace DbSqlLikeMem.VisualStudioExtension.Core.Test;

/// <summary>
/// EN: Verifies file name resolution for template-based generation.
/// PT: Verifica a resolucao de nomes de arquivo para a geracao baseada em templates.
/// </summary>
public sealed class TemplateFileNamePatternResolverTests
{
    /// <summary>
    /// EN: Ensures the default file name patterns remain stable for model and repository generation.
    /// PT: Garante que os padroes padrao de nome de arquivo permaneçam estaveis para geracao de model e repository.
    /// </summary>
    [Fact]
    [Trait("Category", "TemplateFileNamePatternResolver")]
    public void Resolve_WithDefaultPattern_ShouldUseNamePascalAndKindSuffix()
    {
        var connection = new ConnectionDefinition("1", "SqlServer", "ERP", "conn");
        var dbObject = new DatabaseObjectReference("dbo", "sales_order", DatabaseObjectType.Table);

        var modelFile = TemplateFileNamePatternResolver.Resolve(null, "Model", connection, dbObject);
        var repositoryFile = TemplateFileNamePatternResolver.Resolve(string.Empty, "Repository", connection, dbObject);

        Assert.Equal("SalesOrderModel.cs", modelFile);
        Assert.Equal("SalesOrderRepository.cs", repositoryFile);
    }

    /// <summary>
    /// EN: Ensures configured placeholders are expanded for template-based file names.
    /// PT: Garante que placeholders configurados sejam expandidos nos nomes de arquivo baseados em template.
    /// </summary>
    [Fact]
    [Trait("Category", "TemplateFileNamePatternResolver")]
    public void Resolve_WithCustomPattern_ShouldExpandSupportedPlaceholders()
    {
        var connection = new ConnectionDefinition("1", "PostgreSql", "Billing", "conn");
        var dbObject = new DatabaseObjectReference("sales", "monthly-report", DatabaseObjectType.View);

        var fileName = TemplateFileNamePatternResolver.Resolve(
            "{DatabaseType}_{DatabaseName}_{Schema}_{NamePascal}_{Type}_{Namespace}.g.cs",
            "Model",
            connection,
            dbObject,
            "Company.Project.Generated");

        Assert.Equal("PostgreSql_Billing_sales_MonthlyReport_View_Company.Project.Generated.g.cs", fileName);
    }
}
