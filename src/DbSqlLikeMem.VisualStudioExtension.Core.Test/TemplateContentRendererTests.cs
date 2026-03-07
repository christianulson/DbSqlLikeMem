namespace DbSqlLikeMem.VisualStudioExtension.Core.Test;

/// <summary>
/// EN: Verifies template token rendering used by the Visual Studio extension generation flow.
/// PT: Verifica a renderização de tokens de template usada pelo fluxo de geração da extensão do Visual Studio.
/// </summary>
public sealed class TemplateContentRendererTests
{
    /// <summary>
    /// EN: Ensures all documented generation tokens, including namespace, are replaced in rendered content.
    /// PT: Garante que todos os tokens documentados de geração, incluindo namespace, sejam substituídos no conteúdo renderizado.
    /// </summary>
    [Fact]
    [Trait("Category", "TemplateContentRenderer")]
    public void Render_ReplacesDocumentedTokensIncludingNamespace()
    {
        const string template = """
            namespace {{Namespace}};

            // {{DatabaseType}} / {{DatabaseName}}
            // {{Schema}}.{{ObjectName}} ({{ObjectType}})
            public class {{ClassName}}
            {
            }
            """;

        var connection = new ConnectionDefinition("1", "SqlServer", "Billing", "conn");
        var dbObject = new DatabaseObjectReference("dbo", "vw_active_customers", DatabaseObjectType.View);

        var content = TemplateContentRenderer.Render(
            template,
            "VwActiveCustomersView",
            dbObject,
            connection,
            "Sample.Generated");

        Assert.Contains("namespace Sample.Generated;", content);
        Assert.Contains("// SqlServer / Billing", content);
        Assert.Contains("// dbo.vw_active_customers (View)", content);
        Assert.Contains("public class VwActiveCustomersView", content);
    }

    /// <summary>
    /// EN: Ensures the namespace token falls back to an empty string when no namespace is configured.
    /// PT: Garante que o token de namespace use string vazia quando nenhum namespace estiver configurado.
    /// </summary>
    [Fact]
    [Trait("Category", "TemplateContentRenderer")]
    public void Render_WithoutNamespaceConfiguration_ShouldUseEmptyString()
    {
        const string template = "namespace {{Namespace}};";
        var connection = new ConnectionDefinition("1", "PostgreSql", "ERP", "conn");
        var dbObject = new DatabaseObjectReference("public", "orders", DatabaseObjectType.Table);

        var content = TemplateContentRenderer.Render(template, "OrdersTable", dbObject, connection);

        Assert.Equal("namespace ;", content);
    }
}
