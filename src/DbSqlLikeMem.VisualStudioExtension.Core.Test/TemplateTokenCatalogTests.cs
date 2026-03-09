namespace DbSqlLikeMem.VisualStudioExtension.Core.Test;

/// <summary>
/// EN: Verifies the supported token catalog used by template-based generation.
/// PT: Verifica o catalogo de tokens suportados usado pela geracao baseada em templates.
/// </summary>
public sealed class TemplateTokenCatalogTests
{
    /// <summary>
    /// EN: Ensures the catalog exposes all documented tokens used by generation templates.
    /// PT: Garante que o catalogo exponha todos os tokens documentados usados pelos templates de geracao.
    /// </summary>
    [Fact]
    [Trait("Category", "TemplateTokenCatalog")]
    public void GetSupportedTokens_ShouldExposeDocumentedGenerationTokens()
    {
        var tokens = TemplateTokenCatalog.GetSupportedTokens();

        Assert.Contains("{{ClassName}}", tokens);
        Assert.Contains("{{ObjectName}}", tokens);
        Assert.Contains("{{Schema}}", tokens);
        Assert.Contains("{{ObjectType}}", tokens);
        Assert.Contains("{{DatabaseType}}", tokens);
        Assert.Contains("{{DatabaseName}}", tokens);
        Assert.Contains("{{Namespace}}", tokens);
    }

    /// <summary>
    /// EN: Ensures unsupported placeholders are detected without flagging documented tokens.
    /// PT: Garante que placeholders nao suportados sejam detectados sem marcar tokens documentados.
    /// </summary>
    [Fact]
    [Trait("Category", "TemplateTokenCatalog")]
    public void FindUnsupportedTokens_ShouldReturnOnlyUnknownPlaceholders()
    {
        const string template = """
            {{ClassName}}
            {{Schema}}
            {{UnknownToken}}
            {{AnotherUnknown}}
            """;

        var tokens = TemplateTokenCatalog.FindUnsupportedTokens(template);

        Assert.Equal(["{{AnotherUnknown}}", "{{UnknownToken}}"], tokens.OrderBy(x => x).ToArray());
    }

    /// <summary>
    /// EN: Ensures the shipped baseline templates only use supported placeholders.
    /// PT: Garante que os templates de baseline entregues usem apenas placeholders suportados.
    /// </summary>
    [Fact]
    [Trait("Category", "TemplateTokenCatalog")]
    public void CurrentBaselineTemplates_ShouldUseOnlySupportedTokens()
    {
        var repositoryRoot = TemplateBaselineCatalog.FindRepositoryRoot(AppContext.BaseDirectory)
            ?? throw new InvalidOperationException("Repository root containing templates/dbsqllikemem could not be resolved from the test base directory.");

        foreach (var profile in TemplateBaselineCatalog.GetProfiles())
        {
            var modelTemplate = File.ReadAllText(Path.Combine(repositoryRoot, profile.ModelTemplateRelativePath.Replace('/', Path.DirectorySeparatorChar)));
            var repositoryTemplate = File.ReadAllText(Path.Combine(repositoryRoot, profile.RepositoryTemplateRelativePath.Replace('/', Path.DirectorySeparatorChar)));

            Assert.Empty(TemplateTokenCatalog.FindUnsupportedTokens(modelTemplate));
            Assert.Empty(TemplateTokenCatalog.FindUnsupportedTokens(repositoryTemplate));
        }
    }
}
