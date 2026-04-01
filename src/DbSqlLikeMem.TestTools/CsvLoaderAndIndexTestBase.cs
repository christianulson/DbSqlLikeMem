namespace DbSqlLikeMem.TestTools;

/// <summary>
/// EN: Shared CsvLoader and index behavior tests executed by provider-specific derived classes.
/// PT: Testes compartilhados de CsvLoader e índices executados por classes derivadas de cada provedor.
/// </summary>
public abstract class CsvLoaderAndIndexTestBase<TDbMock, TSqlMockException>(
    ITestOutputHelper helper
    ) : XUnitTestBase(helper)
    where TDbMock : DbMock
    where TSqlMockException : SqlMockException
{
    /// <summary>
    /// EN: Creates a provider-specific database mock used by each shared CsvLoader/index test.
    /// PT: Cria um simulado de banco específico do provedor usado por cada teste compartilhado de CsvLoader/índice.
    /// </summary>
    protected abstract TDbMock CreateDb();

    /// <summary>
    /// EN: Verifies CSV loading maps file columns to table columns by name.
    /// PT: Verifica se o carregamento CSV mapeia as colunas do arquivo para as colunas da tabela por nome.
    /// </summary>
    [Fact]
    [Trait("Category", "CsvLoaderAndIndex")]
    public void CsvLoader_ShouldLoadRows_ByColumnName()
    {
        var db = CreateDb();
        var tb = db.AddTable("users");
        tb.AddColumn("id", DbType.Int32, false);
        tb.AddColumn("name", DbType.String, false);

        var tmp = Path.GetTempFileName();
        File.WriteAllText(tmp,
            "id,name\n" +
            "1,John\n" +
            "2,Jane\n");

        db.LoadCsv(tmp, "users");

        db.GetTable("users").Count.Should().Be(2);
        tb[0][1].Should().Be("John");
    }

    /// <summary>
    /// EN: Verifies missing columns raise the provider-specific unknown-column error.
    /// PT: Verifica se colunas ausentes geram o erro especifico do provedor para coluna desconhecida.
    /// </summary>
    [Fact]
    [Trait("Category", "CsvLoaderAndIndex")]
    public void GetColumn_ShouldThrow_UnknownColumn()
    {
        var db = CreateDb();
        var tb = db.AddTable("users");
        tb.AddColumn("id", DbType.Int32, false);

        var ex = FluentActions.Invoking(() => tb.GetColumn("nope")).Should().Throw<TSqlMockException>().Which;
        ex.ErrorCode.Should().Be(1054);
    }

    /// <summary>
    /// EN: Verifies index lookups return the expected row positions.
    /// PT: Verifica se as consultas ao indice retornam as posicoes esperadas das linhas.
    /// </summary>
    [Fact]
    [Trait("Category", "CsvLoaderAndIndex")]
    public void Index_Lookup_ShouldReturnRowPositions()
    {
        var db = CreateDb();
        var tb = db.AddTable("users");
        tb.AddColumn("id", DbType.Int32, false);
        tb.AddColumn("name", DbType.String, false);

        tb.Add(new Dictionary<int, object?> { [0] = 1, [1] = "John" });
        tb.Add(new Dictionary<int, object?> { [0] = 2, [1] = "John" });
        tb.Add(new Dictionary<int, object?> { [0] = 3, [1] = "Jane" });

        var idxDef = tb.CreateIndex("ix_name", ["name"]);

        var ix = tb.Lookup(idxDef, new IndexKey("John"));
        ix!.Select(_ => _.Key).OrderBy(_ => _).Should().Equal(0, 1);
    }

    /// <summary>
    /// EN: Verifies backup and restore roll back table mutations.
    /// PT: Verifica se backup e restore desfazem as mutacoes da tabela.
    /// </summary>
    [Fact]
    [Trait("Category", "CsvLoaderAndIndex")]
    public void BackupRestore_ShouldRollbackData()
    {
        var db = CreateDb();
        var tb = db.AddTable("users");
        tb.AddColumn("id", DbType.Int32, false);
        tb.AddColumn("name", DbType.String, false);

        tb.Add(new Dictionary<int, object?> { [0] = 1, [1] = "John" });

        tb.Backup();
        tb.UpdateRowColumn(0, 1, "Hacked");
        tb.Restore();

        tb[0][1].Should().Be("John");
    }
}
