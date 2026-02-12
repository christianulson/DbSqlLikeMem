namespace DbSqlLikeMem.VisualStudioExtension.Core.Models;

public sealed record TemplateConfiguration(
    string ModelTemplatePath,
    string RepositoryTemplatePath,
    string ModelOutputDirectory,
    string RepositoryOutputDirectory)
{
    public static TemplateConfiguration Default { get; } = new(
        string.Empty,
        string.Empty,
        "Generated/Models",
        "Generated/Repositories");
}
