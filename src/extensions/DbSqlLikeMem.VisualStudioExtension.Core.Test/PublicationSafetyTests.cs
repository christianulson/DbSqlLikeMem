namespace DbSqlLikeMem.VisualStudioExtension.Core.Test;

/// <summary>
/// EN: Verifies generation rejects conflicting destinations before changing local files.
/// PT-br: Verifica se a geracao rejeita destinos conflitantes antes de alterar arquivos locais.
/// </summary>
public sealed class PublicationSafetyTests
{
    /// <summary>
    /// EN: Rejects duplicate output files from objects in different schemas without overwriting existing content.
    /// PT-br: Rejeita arquivos de saida duplicados de objetos em schemas diferentes sem sobrescrever o conteudo existente.
    /// </summary>
    [Fact]
    public async Task GenerateAsync_DuplicateTargets_PreservesExistingFile()
    {
        var output = Path.Combine(Path.GetTempPath(), $"dbsql-safety-{Guid.NewGuid():N}");
        Directory.CreateDirectory(output);
        var existing = Path.Combine(output, "Orders.cs");
        File.WriteAllText(existing, "// keep");
        try
        {
            var request = new GenerationRequest(
                new ConnectionDefinition("1", "SqlServer", "ERP", "conn"),
                [
                    new DatabaseObjectReference("sales", "orders", DatabaseObjectType.Table, "public"),
                    new DatabaseObjectReference("archive", "orders", DatabaseObjectType.Table, "public")
                ]);
            var configuration = new ConnectionMappingConfiguration("1",
                new Dictionary<DatabaseObjectType, ObjectTypeMapping>
                {
                    [DatabaseObjectType.Table] = new(DatabaseObjectType.Table, output, "{NamePascal}.cs")
                });
            await Assert.ThrowsAsync<InvalidOperationException>(() => new ClassGenerator().GenerateAsync(
                request, configuration, _ => "// changed", TestContext.Current.CancellationToken));
            Assert.Equal("// keep", File.ReadAllText(existing));
            Assert.Single(Directory.GetFiles(output));
        }
        finally
        {
            Directory.Delete(output, recursive: true);
        }
    }

    /// <summary>
    /// EN: Rejects a filename that escapes its configured directory.
    /// PT-br: Rejeita um nome de arquivo que escapa do diretorio configurado.
    /// </summary>
    [Theory]
    [InlineData("../Other.cs")]
    [InlineData("..\\Other.cs")]
    [InlineData("bad:name.cs")]
    public void Resolve_UnsafeFileName_RejectsBeforeWriting(string fileName)
        => Assert.Throws<InvalidOperationException>(() => GeneratedFilePath.Resolve(Path.GetTempPath(), fileName));
}
