namespace DbSqlLikeMem.TestTools;

/// <summary>
/// EN: Provides a shared readability-focused contract for provider-specific DbMockConnectionFactory tests.
/// PT: Fornece um contrato compartilhado, focado em legibilidade, para testes por provedor da DbMockConnectionFactory.
/// </summary>
public abstract class DbMockConnectionFactoryContractTestsBase
{
    /// <summary>
    /// EN: Gets the canonical provider hint used to resolve the provider in factory calls.
    /// PT: Obtem a dica canonica de provedor usada para resolver o provedor nas chamadas da factory.
    /// </summary>
    protected abstract string ProviderHint { get; }
    /// <summary>
    /// EN: Gets the expected DbMock runtime type for this provider contract.
    /// PT: Obtem o tipo de runtime DbMock esperado para este contrato de provedor.
    /// </summary>
    protected abstract Type ExpectedDbType { get; }
    /// <summary>
    /// EN: Gets the expected connection runtime type for this provider contract.
    /// PT: Obtem o tipo de runtime de conexao esperado para este contrato de provedor.
    /// </summary>
    protected abstract Type ExpectedConnectionType { get; }
    /// <summary>
    /// EN: Gets the provider aliases that must resolve to the same provider implementation.
    /// PT: Obtem os aliases de provedor que devem resolver para a mesma implementacao de provedor.
    /// </summary>
    protected abstract IReadOnlyList<string> ProviderAliases { get; }

    /// <summary>
    /// EN: Creates a DbMock and connection pair through the provider shortcut for contract validation.
    /// PT: Cria um par DbMock e conexao pelo atalho de provedor para validacao do contrato.
    /// </summary>
    protected abstract (DbMock Db, IDbConnection Connection) CreateViaProviderShortcut(params Action<DbMock>[] tableMappers);

    /// <summary>
    /// EN: Verifies that the provider shortcut creates the expected DbMock and connection types.
    /// PT: Verifica se o atalho de provedor cria os tipos esperados de DbMock e conexao.
    /// </summary>
    [Fact]
    public void CreateViaProviderShortcut_ShouldCreateExpectedDbAndConnection()
    {
        var (db, connection) = CreateViaProviderShortcut();

        db.Should().BeOfType(ExpectedDbType);
        connection.Should().BeOfType(ExpectedConnectionType);
    }

    /// <summary>
    /// EN: Verifies that CreateWithTables applies table mapper actions to the created DbMock.
    /// PT: Verifica se CreateWithTables aplica as acoes de mapeamento de tabela ao DbMock criado.
    /// </summary>
    [Fact]
    public void CreateWithTables_ShouldApplyTableMappers()
    {
        var (db, connection) = DbMockConnectionFactory.CreateWithTables(
            ProviderHint,
            it =>
            {
                var tb = it.AddTable("Users");
                tb.AddColumn("Id", DbType.Int32, false);
                tb.AddColumn("Name", DbType.String, false);
                tb.Add(new Dictionary<int, object?> { [0] = 1, [1] = "Ana" });
            });

        db.Should().BeOfType(ExpectedDbType);
        connection.Should().BeOfType(ExpectedConnectionType);
        db.GetTable("Users").Should().HaveCount(1);
    }

    /// <summary>
    /// EN: Verifies that successive CreateWithTables calls create isolated DbMock instances.
    /// PT: Verifica se chamadas sucessivas de CreateWithTables criam instancias isoladas de DbMock.
    /// </summary>
    [Fact]
    public void CreateWithTables_ShouldCreateIsolatedInstancesBetweenCalls()
    {
        var (firstDb, _) = DbMockConnectionFactory.CreateWithTables(
            ProviderHint,
            it =>
            {
                var tb = it.AddTable("Users");
                tb.AddColumn("Id", DbType.Int32, false);
                tb.Add(new Dictionary<int, object?> { [0] = 1 });
            });

        var (secondDb, _) = DbMockConnectionFactory.CreateWithTables(ProviderHint);

        firstDb.ContainsTable("Users").Should().BeTrue();
        secondDb.ContainsTable("Users").Should().BeFalse();
    }

    /// <summary>
    /// EN: Verifies that provider aliases resolve to the expected DbMock and connection types.
    /// PT: Verifica se os aliases de provedor resolvem para os tipos esperados de DbMock e conexao.
    /// </summary>
    [Fact]
    public void CreateWithTables_ForAliases_ShouldResolveExpectedTypes()
    {
        foreach (var providerHint in ProviderAliases)
        {
            var (db, connection) = DbMockConnectionFactory.CreateWithTables(providerHint);
            db.Should().BeOfType(ExpectedDbType, because: $"alias '{providerHint}' must resolve to expected DbMock type");
            connection.Should().BeOfType(ExpectedConnectionType, because: $"alias '{providerHint}' must resolve to expected connection type");
        }
    }
}
